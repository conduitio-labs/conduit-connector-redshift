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

type TableIterator struct {
	db       *sqlx.DB
	rows     *sqlx.Rows
	position TablePosition

	// table is a table name
	table string
	// keyColumns are table column which the table iterator will use to create a record's key
	keyColumns []string
	// orderingColumn is the name of the column that table iterator will use for sorting data
	orderingColumn string
	// batchSize is the size of a batch retrieved from Redshift
	batchSize int
	// columnTypes is a mapping from column names to their respective types
	columnTypes map[string]string

	// iterator is an instance of the iterator
	iterator *Iterator
}

type TableIteratorConfig struct {
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

// NewTableIterator creates a new instance of the table iterator.
func NewTableIterator(ctx context.Context, config TableIteratorConfig, iterator *Iterator) (*TableIterator, error) {
	ti := &TableIterator{
		db:             config.db,
		position:       config.position,
		table:          config.table,
		keyColumns:     config.keyColumns,
		orderingColumn: config.orderingColumn,
		batchSize:      config.batchSize,
		iterator:       iterator,
	}

	if ti.position.LastProcessedValue == nil {
		latestSnapshotValue, latestValErr := ti.latestSnapshotValue(ctx)
		if latestValErr != nil {
			return nil, fmt.Errorf("get latest snapshot value: %w", latestValErr)
		}

		if config.snapshot {
			// set the LatestSnapshotValue to specify which record the snapshot table iterator will work to
			ti.position.LatestSnapshotValue = latestSnapshotValue
		} else {
			// set the LastProcessedValue to skip a snapshot of the entire table
			ti.position.LastProcessedValue = latestSnapshotValue
		}
	}

	uri, err := url.Parse(config.dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	err = ti.populateKeyColumns(ctx, uri.Query().Get(keySearchPath))
	if err != nil {
		return nil, fmt.Errorf("populate key columns: %w", err)
	}

	ti.columnTypes, err = columntypes.GetColumnTypes(
		ctx,
		ti.db,
		ti.table,
		uri.Query().Get(keySearchPath),
	)
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}

	err = ti.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return ti, nil
}

// HasNext returns a bool indicating whether the source has the next record to return or not.
func (ti *TableIterator) HasNext(ctx context.Context) (bool, error) {
	if ti.rows != nil && ti.rows.Next() {
		return true, nil
	}

	if err := ti.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	if ti.rows.Next() {
		return true, nil
	}

	// At this point, there are no more rows to load
	// so if we are in snapshot mode, we can switch to CDC
	if ti.position.LatestSnapshotValue != nil {
		// switch to CDC mode
		ti.position.LastProcessedValue = ti.position.LatestSnapshotValue
		ti.position.LatestSnapshotValue = nil

		ti.iterator.updateTablePosition(ti.table, ti.position)

		// and load new rows
		if err := ti.loadRows(ctx); err != nil {
			return false, fmt.Errorf("load rows: %w", err)
		}

		return ti.rows.Next(), nil
	}

	return false, nil
}

// Next returns the next record.
func (ti *TableIterator) Next(_ context.Context) (opencdc.Record, error) {
	row := make(map[string]any)
	if err := ti.rows.MapScan(row); err != nil {
		return opencdc.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := columntypes.TransformRow(row, ti.columnTypes)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	if _, ok := transformedRow[ti.orderingColumn]; !ok {
		return opencdc.Record{}, fmt.Errorf("ordering column %q not found", ti.orderingColumn)
	}

	key := make(opencdc.StructuredData)
	for i := range ti.keyColumns {
		val, ok := transformedRow[ti.keyColumns[i]]
		if !ok {
			return opencdc.Record{}, fmt.Errorf("key column %q not found", ti.keyColumns[i])
		}

		key[ti.keyColumns[i]] = val
	}

	rowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	position := ti.iterator.getPosition()

	tablePos := ti.getTablePosition(position, transformedRow)

	// set the value from iter.orderingColumn column you chose
	position.TablePositions[ti.table] = tablePos

	sdkPosition, err := position.marshal()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed converting to SDK position :%w", err)
	}

	ti.position = position.TablePositions[ti.table]
	ti.iterator.updateTablePosition(ti.table, tablePos)

	metadata := opencdc.Metadata{
		opencdc.MetadataCollection: ti.table,
	}
	metadata.SetCreatedAt(time.Now().UTC())

	if ti.position.LatestSnapshotValue != nil {
		return sdk.Util.Source.NewRecordSnapshot(sdkPosition, metadata, key, opencdc.RawData(rowBytes)), nil
	}

	return sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, opencdc.RawData(rowBytes)), nil
}

// Stop stops table iterator.
func (ti *TableIterator) Stop() error {
	if ti.rows != nil {
		err := ti.rows.Close()
		if err != nil {
			return fmt.Errorf("close db rows: %w", err)
		}
	}

	return nil
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and current position.
func (ti *TableIterator) loadRows(ctx context.Context) error {
	var err error

	sb := sqlbuilder.PostgreSQL.NewSelectBuilder().
		Select("*").
		From(ti.table).
		OrderBy(ti.orderingColumn).
		Limit(ti.batchSize)

	if ti.position.LastProcessedValue != nil {
		sb.Where(sb.GreaterThan(ti.orderingColumn, ti.position.LastProcessedValue))
	}

	if ti.position.LatestSnapshotValue != nil {
		sb.Where(sb.LessEqualThan(ti.orderingColumn, ti.position.LatestSnapshotValue))
	}

	query, args := sb.Build()

	ti.rows, err = ti.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select data query %q, %v: %w", query, args, err)
	}

	return nil
}

// populateKeyColumns populates keyColumn from the database metadata
// or from the orderingColumn configuration field in the described order if it's empty.
func (ti *TableIterator) populateKeyColumns(ctx context.Context, schema string) error {
	if len(ti.keyColumns) != 0 {
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

	sb.Where(sb.Equal("kcu.table_name", ti.table))

	if schema != "" {
		sb.Where(sb.Equal("kcu.table_schema", schema))
	}

	query, args := sb.Build()

	rows, err := ti.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute query select primary keys %q, %v: %w", query, args, err)
	}
	defer rows.Close()

	columnName := ""
	for rows.Next() {
		if err = rows.Scan(&columnName); err != nil {
			return fmt.Errorf("scan primary key column name: %w", err)
		}

		ti.keyColumns = append(ti.keyColumns, columnName)
	}

	if len(ti.keyColumns) == 0 {
		ti.keyColumns = []string{ti.orderingColumn}
	}

	return nil
}

// latestSnapshotValue returns most recent value of orderingColumn column.
func (ti *TableIterator) latestSnapshotValue(ctx context.Context) (any, error) {
	var latestSnapshotValue any

	query := sqlbuilder.PostgreSQL.NewSelectBuilder().
		Select(ti.orderingColumn).
		From(ti.table).
		OrderBy(ti.orderingColumn).Desc().
		Limit(1).
		String()

	rows, err := ti.db.QueryxContext(ctx, query)
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

func (ti *TableIterator) getTablePosition(position *Position, transformedRow map[string]any) TablePosition {
	tablePos, ok := position.TablePositions[ti.table]
	if !ok {
		tablePos = TablePosition{
			LastProcessedValue:  transformedRow[ti.orderingColumn],
			LatestSnapshotValue: ti.position.LatestSnapshotValue,
		}
	} else {
		tablePos.LastProcessedValue = transformedRow[ti.orderingColumn]
	}

	return tablePos
}
