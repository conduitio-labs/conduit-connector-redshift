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
	"github.com/conduitio-labs/conduit-connector-redshift/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
	"go.uber.org/multierr"
)

const (
	// metadataFieldTable is a name of a record metadata field that stores a Redshift table name.
	metadataFieldTable = "redshift.table"
	// keySearchPath is a key of get parameter of a datatable's schema name.
	keySearchPath = "search_path"
	// pingTimeoutSec is a ping db timeout in seconds.
	pingTimeoutSec = 10
)

// Iterator is an implementation of an iterator for Amazon Redshift.
type Iterator struct {
	db       *sqlx.DB
	rows     *sqlx.Rows
	position *Position

	// table is a table name.
	table string
	// keyColumns is a name of the column that iterator will use for setting key in record.
	keyColumns []string
	// orderingColumn is a name of the column that iterator will use for sorting data.
	orderingColumn string
	// batchSize is a size of rows batch.
	batchSize int
	// columnTypes is a map with all column types.
	columnTypes map[string]string
}

// New creates a new instance of the iterator.
func New(ctx context.Context, driverName string, pos *Position, config config.Source) (*Iterator, error) {
	var err error

	iterator := &Iterator{
		position:       pos,
		table:          config.Table,
		keyColumns:     config.KeyColumns,
		orderingColumn: config.OrderingColumn,
		batchSize:      config.BatchSize,
	}

	iterator.db, err = sqlx.Open(driverName, config.DSN)
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, pingTimeoutSec*time.Second)
	defer cancel()

	err = iterator.db.PingContext(ctxTimeout)
	if err != nil {
		return nil, fmt.Errorf("ping db with timeout: %w", err)
	}

	if iterator.position.LastProcessedValue == nil {
		latestSnapshotValue, latestValErr := iterator.latestSnapshotValue(ctx)
		if latestValErr != nil {
			return nil, fmt.Errorf("get latest snapshot value: %w", latestValErr)
		}

		if config.Snapshot {
			// set the LatestSnapshotValue to specify which record the snapshot iterator will work to
			iterator.position.LatestSnapshotValue = latestSnapshotValue
		} else {
			// set the LastProcessedValue to skip a snapshot of the entire table
			iterator.position.LastProcessedValue = latestSnapshotValue
		}
	}

	uri, err := url.Parse(config.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	err = iterator.populateKeyColumns(ctx, uri.Query().Get(keySearchPath))
	if err != nil {
		return nil, fmt.Errorf("populate key columns: %w", err)
	}

	iterator.columnTypes, err = columntypes.GetColumnTypes(ctx,
		iterator.db, iterator.table, uri.Query().Get(keySearchPath))
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}

	err = iterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the source has the next record to return or not.
func (iter *Iterator) HasNext(ctx context.Context) (bool, error) {
	if iter.rows != nil && iter.rows.Next() {
		return true, nil
	}

	if err := iter.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	if iter.rows.Next() {
		return true, nil
	}

	if iter.position.LatestSnapshotValue != nil {
		// switch to CDC mode
		iter.position.LastProcessedValue = iter.position.LatestSnapshotValue
		iter.position.LatestSnapshotValue = nil

		// and load new rows
		if err := iter.loadRows(ctx); err != nil {
			return false, fmt.Errorf("load rows: %w", err)
		}

		return iter.rows.Next(), nil
	}

	return false, nil
}

// Next returns the next record.
func (iter *Iterator) Next(_ context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := iter.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	transformedRow, err := columntypes.TransformRow(row, iter.columnTypes)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("transform row column types: %w", err)
	}

	if _, ok := transformedRow[iter.orderingColumn]; !ok {
		return sdk.Record{}, fmt.Errorf("ordering column %q not found", iter.orderingColumn)
	}

	key := make(sdk.StructuredData)
	for i := range iter.keyColumns {
		val, ok := transformedRow[iter.keyColumns[i]]
		if !ok {
			return sdk.Record{}, fmt.Errorf("key column %q not found", iter.keyColumns[i])
		}

		key[iter.keyColumns[i]] = val
	}

	rowBytes, err := json.Marshal(transformedRow)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	// set a new position into the variable,
	// to avoid saving position into the struct until we marshal the position
	position := *iter.position
	// set the value from iter.orderingColumn column you chose
	position.LastProcessedValue = transformedRow[iter.orderingColumn]

	convertedPosition, err := position.marshal()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position :%w", err)
	}

	iter.position = &position

	metadata := sdk.Metadata{
		metadataFieldTable: iter.table,
	}
	metadata.SetCreatedAt(time.Now().UTC())

	if position.LatestSnapshotValue != nil {
		return sdk.Util.Source.NewRecordSnapshot(convertedPosition, metadata, key, sdk.RawData(rowBytes)), nil
	}

	return sdk.Util.Source.NewRecordCreate(convertedPosition, metadata, key, sdk.RawData(rowBytes)), nil
}

// Stop stops iterators and closes database connection.
func (iter *Iterator) Stop() error {
	var err error

	if iter.rows != nil {
		err = iter.rows.Close()
	}

	if iter.db != nil {
		err = multierr.Append(err, iter.db.Close())
	}

	if err != nil {
		return fmt.Errorf("close db rows and db: %w", err)
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
		Join("information_schema.key_column_usage kcu", "kcu.constraint_name = tco.constraint_name "+
			"AND kcu.constraint_schema = tco.constraint_schema AND kcu.constraint_name = tco.constraint_name").
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

	if len(iter.keyColumns) != 0 {
		return nil
	}

	iter.keyColumns = []string{iter.orderingColumn}

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

	for rows.Next() {
		if err = rows.Scan(&latestSnapshotValue); err != nil {
			return nil, fmt.Errorf("scan latest snapshot value: %w", err)
		}
	}

	return latestSnapshotValue, nil
}
