// Copyright © 2022 Meroxa, Inc. & Yalantis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iterator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/conduitio-labs/conduit-connector-redshift/columntypes"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

const (
	// keySearchPath is a key of get parameter of a datatable's schema name.
	keySearchPath = "search_path"
	// pingTimeout is a database ping timeout.
	pingTimeout = 10 * time.Second
)

type Worker struct {
	db       *sqlx.DB
	rows     *sqlx.Rows
	position TablePosition

	// table is a table name
	table string
	// keyColumns are table column which the worker will use to create a record's key
	keyColumns []string
	// orderingColumn is the name of the column that worker will use for sorting data
	orderingColumn string
	// batchSize is the size of a batch retrieved from Redshift
	batchSize int
	// columnTypes is a mapping from column names to their respective types
	columnTypes map[string]string

	// iterator is an instance of the iterator
	iterator *Iterator
}

type WorkerConfig struct {
	db       *sqlx.DB
	position TablePosition

	table          string
	orderingColumn string
	keyColumns     []string
	batchSize      int
	// snapshot is the configuration that determines whether the connector
	// will take a snapshot of the entire table before starting cdc mode.
	snapshot bool
	dsn      string
}

// NewWorker creates a new instance of the worker.
func NewWorker(ctx context.Context, config WorkerConfig, iterator *Iterator) (*Worker, error) {
	worker := &Worker{
		db:             config.db,
		position:       config.position,
		table:          config.table,
		keyColumns:     config.keyColumns,
		orderingColumn: config.orderingColumn,
		batchSize:      config.batchSize,
		iterator:       iterator,
	}

	if worker.position.LastProcessedValue == nil {
		latestSnapshotValue, latestValErr := worker.latestSnapshotValue(ctx)
		if latestValErr != nil {
			return nil, fmt.Errorf("get latest snapshot value: %w", latestValErr)
		}

		if config.snapshot {
			// set the LatestSnapshotValue to specify which record the snapshot worker will work to
			worker.position.LatestSnapshotValue = latestSnapshotValue
		} else {
			// set the LastProcessedValue to skip a snapshot of the entire table
			worker.position.LastProcessedValue = latestSnapshotValue
		}
	}

	uri, err := url.Parse(config.dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	err = worker.populateKeyColumns(ctx, uri.Query().Get(keySearchPath))
	if err != nil {
		return nil, fmt.Errorf("populate key columns: %w", err)
	}

	worker.columnTypes, err = columntypes.GetColumnTypes(
		ctx,
		worker.db,
		worker.table,
		uri.Query().Get(keySearchPath),
	)
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}

	err = worker.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return worker, nil
}

// HasNext returns a bool indicating whether the source has the next record to return or not.
func (w *Worker) HasNext(ctx context.Context) (bool, error) {
	if w.rows != nil && w.rows.Next() {
		return true, nil
	}

	if err := w.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	if w.rows.Next() {
		return true, nil
	}

	// At this point, there are no more rows to load
	// so if we are in snapshot mode, we can switch to CDC
	if w.position.LatestSnapshotValue != nil {
		// switch to CDC mode
		w.position.LastProcessedValue = w.position.LatestSnapshotValue
		w.position.LatestSnapshotValue = nil

		w.iterator.updateTablePosition(w.table, w.position)

		// and load new rows
		if err := w.loadRows(ctx); err != nil {
			return false, fmt.Errorf("load rows: %w", err)
		}

		return w.rows.Next(), nil
	}

	return false, nil
}

// Next returns the next record.
func (w *Worker) Next(_ context.Context) (opencdc.Record, error) {
	row := make(map[string]any)
	if err := w.rows.MapScan(row); err != nil {
		return opencdc.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := columntypes.TransformRow(row, w.columnTypes)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	if _, ok := transformedRow[w.orderingColumn]; !ok {
		return opencdc.Record{}, fmt.Errorf("ordering column %q not found", w.orderingColumn)
	}

	key := make(opencdc.StructuredData)
	for i := range w.keyColumns {
		val, ok := transformedRow[w.keyColumns[i]]
		if !ok {
			return opencdc.Record{}, fmt.Errorf("key column %q not found", w.keyColumns[i])
		}

		key[w.keyColumns[i]] = val
	}

	rowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	position := w.iterator.getPosition()

	tablePos := w.getTablePosition(position, transformedRow)

	// set the value from iter.orderingColumn column you chose
	position.TablePositions[w.table] = tablePos

	sdkPosition, err := position.marshal()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed converting to SDK position :%w", err)
	}

	w.position = position.TablePositions[w.table]
	w.iterator.updateTablePosition(w.table, tablePos)

	metadata := opencdc.Metadata{
		opencdc.MetadataCollection: w.table,
	}
	metadata.SetCreatedAt(time.Now().UTC())

	if w.position.LatestSnapshotValue != nil {
		return sdk.Util.Source.NewRecordSnapshot(sdkPosition, metadata, key, opencdc.RawData(rowBytes)), nil
	}

	return sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, opencdc.RawData(rowBytes)), nil
}

// Stop stops worker.
func (w *Worker) Stop() error {
	var err error

	if w.rows != nil {
		err = w.rows.Close()
	}

	if err != nil {
		return fmt.Errorf("close db rows: %w", err)
	}

	return nil
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and current position.
func (w *Worker) loadRows(ctx context.Context) error {
	var err error

	sb := sqlbuilder.PostgreSQL.NewSelectBuilder().
		Select("*").
		From(w.table).
		OrderBy(w.orderingColumn).
		Limit(w.batchSize)

	if w.position.LastProcessedValue != nil {
		sb.Where(sb.GreaterThan(w.orderingColumn, w.position.LastProcessedValue))
	}

	if w.position.LatestSnapshotValue != nil {
		sb.Where(sb.LessEqualThan(w.orderingColumn, w.position.LatestSnapshotValue))
	}

	query, args := sb.Build()

	w.rows, err = w.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select data query %q, %v: %w", query, args, err)
	}

	return nil
}

// populateKeyColumns populates keyColumn from the database metadata
// or from the orderingColumn configuration field in the described order if it's empty.
func (w *Worker) populateKeyColumns(ctx context.Context, schema string) error {
	if len(w.keyColumns) != 0 {
		return nil
	}

	sb := sqlbuilder.PostgreSQL.NewSelectBuilder().
		Select("kcu.column_name").
		From("information_schema.table_constraints tco").
		Join(
			"information_schema.key_column_usage kcu",
			"kcu.constraint_name = tco.constraint_name "+
				"AND kcu.constraint_schema = tco.constraint_schema AND kcu.constraint_name = tco.constraint_name",
		).
		Where("tco.constraint_type = 'PRIMARY KEY'")

	sb.Where(sb.Equal("kcu.table_name", w.table))

	if schema != "" {
		sb.Where(sb.Equal("kcu.table_schema", schema))
	}

	query, args := sb.Build()

	rows, err := w.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute query select primary keys %q, %v: %w", query, args, err)
	}
	defer rows.Close()

	columnName := ""
	for rows.Next() {
		if err = rows.Scan(&columnName); err != nil {
			return fmt.Errorf("scan primary key column name: %w", err)
		}

		w.keyColumns = append(w.keyColumns, columnName)
	}

	if len(w.keyColumns) == 0 {
		w.keyColumns = []string{w.orderingColumn}
	}

	return nil
}

// latestSnapshotValue returns most recent value of orderingColumn column.
func (w *Worker) latestSnapshotValue(ctx context.Context) (any, error) {
	var latestSnapshotValue any

	query := sqlbuilder.PostgreSQL.NewSelectBuilder().
		Select(w.orderingColumn).
		From(w.table).
		OrderBy(w.orderingColumn).Desc().
		Limit(1).
		String()

	rows, err := w.db.QueryxContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("execute select latest snapshot value query %q: %w", query, err)
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&latestSnapshotValue); err != nil {
			return nil, fmt.Errorf("scan latest snapshot value: %w", err)
		}
	}

	return latestSnapshotValue, nil
}

func (w *Worker) getTablePosition(position *Position, transformedRow map[string]any) TablePosition {
	tablePos, ok := position.TablePositions[w.table]
	if !ok {
		tablePos = TablePosition{
			LastProcessedValue:  transformedRow[w.orderingColumn],
			LatestSnapshotValue: w.position.LatestSnapshotValue,
		}
	} else {
		tablePos.LastProcessedValue = transformedRow[w.orderingColumn]
	}

	return tablePos
}
