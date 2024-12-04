// Copyright © 2024 Meroxa, Inc. & Yalantis
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

package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
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

type Iterator struct {
	db             *sqlx.DB
	rows           *sqlx.Rows
	sourcePosition *Position
	position       TablePosition

	// table is a table name
	table string
	// keyColumns are table column which the iterator will use to create a record's key
	keyColumns []string
	// orderingColumn is the name of the column that iterator will use for sorting data
	orderingColumn string
	// batchSize is the size of a batch retrieved from Redshift
	batchSize int
	// columnTypes is a mapping from column names to their respective types
	columnTypes map[string]string
	// pollingPeriod is used by iterator to poll for new data at regular intervals.
	pollingPeriod time.Duration

	wg *sync.WaitGroup
	ch chan opencdc.Record
}

type IteratorConfig struct {
	db             *sqlx.DB
	position       TablePosition
	sourcePosition *Position
	table          string
	orderingColumn string
	keyColumns     []string
	batchSize      int
	pollingPeriod  time.Duration
	snapshot       bool
	dsn            string
	wg             *sync.WaitGroup
	ch             chan opencdc.Record
}

// NewTableIterator creates a new iterator goroutine and polls redshift for new records.
func NewIterator(ctx context.Context, cfg IteratorConfig) error {
	iter := &Iterator{
		db:             cfg.db,
		position:       cfg.position,
		sourcePosition: cfg.sourcePosition,
		table:          cfg.table,
		keyColumns:     cfg.keyColumns,
		orderingColumn: cfg.orderingColumn,
		batchSize:      cfg.batchSize,
		pollingPeriod:  cfg.pollingPeriod,
		wg:             cfg.wg,
		ch:             cfg.ch,
	}

	if iter.position.LastProcessedValue == nil {
		latestSnapshotValue, latestValErr := iter.latestSnapshotValue(ctx)
		if latestValErr != nil {
			return fmt.Errorf("get latest snapshot value: %w", latestValErr)
		}

		if cfg.snapshot {
			// set the LatestSnapshotValue to specify which record the snapshot table iterator will work to
			iter.position.LatestSnapshotValue = latestSnapshotValue
		} else {
			// set the LastProcessedValue to skip a snapshot of the entire table
			iter.position.LastProcessedValue = latestSnapshotValue
		}
	}

	uri, err := url.Parse(cfg.dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}

	err = iter.populateKeyColumns(ctx, uri.Query().Get(keySearchPath))
	if err != nil {
		return fmt.Errorf("populate key columns: %w", err)
	}

	iter.columnTypes, err = columntypes.GetColumnTypes(
		ctx,
		iter.db,
		iter.table,
		uri.Query().Get(keySearchPath),
	)
	if err != nil {
		return fmt.Errorf("get column types: %w", err)
	}

	err = iter.loadRows(ctx)
	if err != nil {
		return fmt.Errorf("load rows: %w", err)
	}

	go iter.start(ctx)

	return nil
}

// start polls redshift for new records and writes it into the source channel.
func (iter *Iterator) start(ctx context.Context) {
	defer iter.wg.Done()

	for {
		hasNext, err := iter.hasNext(ctx)
		if err != nil {
			sdk.Logger(ctx).Err(err).Msg("iterator shutting down...")
			return //nolint:nlreturn // compact code style
		}

		if !hasNext {
			select {
			case <-ctx.Done():
				sdk.Logger(ctx).Debug().Msg("context cancelled, iterator shutting down...")
				err = iter.stop()
				if err != nil {
					sdk.Logger(ctx).Err(err).Msg("failed to properly stop iterator after context cancellation")
				}
				return //nolint:nlreturn // compact code style

			case <-time.After(iter.pollingPeriod):
				continue
			}
		}

		record, err := iter.next(ctx)
		if err != nil {
			sdk.Logger(ctx).Err(err).Msg("iterator shutting down...")
			return //nolint:nlreturn // compact code style
		}

		select {
		case iter.ch <- record:
			position, _ := iter.sourcePosition.get(iter.table)
			iter.position = position

		case <-ctx.Done():
			sdk.Logger(ctx).Debug().Msg("context cancelled, iterator shutting down...")
			err = iter.stop()
			if err != nil {
				sdk.Logger(ctx).Err(err).Msg("failed to properly stop iterator after context cancellation")
			}
			return //nolint:nlreturn // compact code style
		}
	}
}

// hasNext returns a bool indicating whether the source has the next record to return or not.
func (iter *Iterator) hasNext(ctx context.Context) (bool, error) {
	if iter.rows != nil && iter.rows.Next() {
		return true, nil
	}

	if err := iter.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	if iter.rows.Next() {
		return true, nil
	}

	// At this point, there are no more rows to load
	// so if we are in snapshot mode, we can switch to CDC
	if iter.position.LatestSnapshotValue != nil {
		// switch to CDC mode
		iter.position.LastProcessedValue = iter.position.LatestSnapshotValue
		iter.position.LatestSnapshotValue = nil

		iter.sourcePosition.set(iter.table, iter.position)

		// and load new rows
		if err := iter.loadRows(ctx); err != nil {
			return false, fmt.Errorf("load rows: %w", err)
		}

		return iter.rows.Next(), nil
	}

	return false, nil
}

// next returns the next record.
func (iter *Iterator) next(_ context.Context) (opencdc.Record, error) {
	row := make(map[string]any)
	if err := iter.rows.MapScan(row); err != nil {
		return opencdc.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := columntypes.TransformRow(row, iter.columnTypes)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	if _, ok := transformedRow[iter.orderingColumn]; !ok {
		return opencdc.Record{}, fmt.Errorf("ordering column %q not found", iter.orderingColumn)
	}

	key := make(opencdc.StructuredData)
	for i := range iter.keyColumns {
		val, ok := transformedRow[iter.keyColumns[i]]
		if !ok {
			return opencdc.Record{}, fmt.Errorf("key column %q not found", iter.keyColumns[i])
		}

		key[iter.keyColumns[i]] = val
	}

	rowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	position, exists := iter.sourcePosition.get(iter.table)
	if !exists {
		position = TablePosition{
			LastProcessedValue:  transformedRow[iter.orderingColumn],
			LatestSnapshotValue: iter.position.LatestSnapshotValue,
		}
	} else {
		position.LastProcessedValue = transformedRow[iter.orderingColumn]
	}

	iter.sourcePosition.set(iter.table, position)

	sdkPosition, err := iter.sourcePosition.marshal()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed converting to SDK position :%w", err)
	}

	metadata := opencdc.Metadata{
		opencdc.MetadataCollection: iter.table,
	}
	metadata.SetCreatedAt(time.Now().UTC())

	if position.LatestSnapshotValue != nil {
		return sdk.Util.Source.NewRecordSnapshot(sdkPosition, metadata, key, opencdc.RawData(rowBytes)), nil
	}

	return sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, opencdc.RawData(rowBytes)), nil
}

// stop stops table iterator.
func (iter *Iterator) stop() error {
	if iter.rows != nil {
		err := iter.rows.Close()
		if err != nil {
			return fmt.Errorf("close db rows: %w", err)
		}
	}

	return nil
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and current position.
func (iter *Iterator) loadRows(ctx context.Context) error {
	var err error

	sb := sqlbuilder.PostgreSQL.NewSelectBuilder().
		Select("*").
		From(iter.table).
		OrderBy(iter.orderingColumn).
		Limit(iter.batchSize)

	if iter.position.LastProcessedValue != nil {
		sb.Where(sb.GreaterThan(iter.orderingColumn, iter.position.LastProcessedValue))
	}

	if iter.position.LatestSnapshotValue != nil {
		sb.Where(sb.LessEqualThan(iter.orderingColumn, iter.position.LatestSnapshotValue))
	}

	query, args := sb.Build()

	iter.rows, err = iter.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select data query %q, %v: %w", query, args, err)
	}

	return nil
}

// populateKeyColumns populates keyColumn from the database metadata
// or from the orderingColumn configuration field in the described order if it's empty.
func (iter *Iterator) populateKeyColumns(ctx context.Context, schema string) error {
	if len(iter.keyColumns) != 0 {
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

	sb.Where(sb.Equal("kcu.table_name", iter.table))

	if schema != "" {
		sb.Where(sb.Equal("kcu.table_schema", schema))
	}

	query, args := sb.Build()

	rows, err := iter.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute query select primary keys %q, %v: %w", query, args, err)
	}
	defer rows.Close()

	columnName := ""
	for rows.Next() {
		if err = rows.Scan(&columnName); err != nil {
			return fmt.Errorf("scan primary key column name: %w", err)
		}

		iter.keyColumns = append(iter.keyColumns, columnName)
	}

	if len(iter.keyColumns) == 0 {
		iter.keyColumns = []string{iter.orderingColumn}
	}

	return nil
}

// latestSnapshotValue returns most recent value of orderingColumn column.
func (iter *Iterator) latestSnapshotValue(ctx context.Context) (any, error) {
	var latestSnapshotValue any

	query := sqlbuilder.PostgreSQL.NewSelectBuilder().
		Select(iter.orderingColumn).
		From(iter.table).
		OrderBy(iter.orderingColumn).Desc().
		Limit(1).
		String()

	rows, err := iter.db.QueryxContext(ctx, query)
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
