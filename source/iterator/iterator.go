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
	"time"

	"github.com/conduitio-labs/conduit-connector-redshift/columntypes"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

// metadata related.
const (
	metadataTable = "redshift.table"

	querySelectRowsFmt = "SELECT * FROM %s%s ORDER BY %s ASC LIMIT %d;"
	whereClauseFmt     = " WHERE %s > $1"

	querySelectPKs = `
	SELECT
		kcu.column_name
	FROM
    	information_schema.table_constraints tco
        	JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tco.constraint_name
        	AND kcu.constraint_schema = tco.constraint_schema
        	AND kcu.constraint_name = tco.constraint_name
	WHERE
    	tco.constraint_type = 'PRIMARY KEY'
		AND kcu.table_name = $1%s`
	whereClauseSchema = " AND kcu.table_schema = $2"
)

// Iterator is an implementation of an iterator for Amazon Redshift.
type Iterator struct {
	db   *sqlx.DB
	rows *sqlx.Rows

	// lastProcessedVal is a last processed orderingColumn column value.
	lastProcessedVal any
	// table is a table name.
	table string
	// keyColumns is a name of the column that iterator will use for setting key in record.
	keyColumns []string
	// orderingColumn is a name of the column that iterator will use for sorting data.
	orderingColumn string
	// batchSize is a size of rows batch.
	batchSize int
	// schema is a schema name.
	schema string
	// columnTypes is a map with all column types.
	columnTypes map[string]string
}

// Params is an incoming iterator params for the New function.
type Params struct {
	DB               *sqlx.DB
	LastProcessedVal any
	Table            string
	KeyColumns       []string
	OrderingColumn   string
	BatchSize        int
	Schema           string
}

// New creates a new instance of the iterator.
func New(ctx context.Context, params Params) (*Iterator, error) {
	iterator := &Iterator{
		db:               params.DB,
		lastProcessedVal: params.LastProcessedVal,
		table:            params.Table,
		keyColumns:       params.KeyColumns,
		orderingColumn:   params.OrderingColumn,
		batchSize:        params.BatchSize,
		schema:           params.Schema,
	}

	err := iterator.populateKeyColumns(ctx)
	if err != nil {
		return nil, fmt.Errorf("populate key columns: %w", err)
	}

	iterator.columnTypes, err = columntypes.GetColumnTypes(ctx, params.DB, params.Table, params.Schema)
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}

	err = iterator.loadRows(ctx)
	if err != nil {
		return nil, fmt.Errorf("load rows: %w", err)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the iterator has the next record to return or not.
func (iter *Iterator) HasNext(ctx context.Context) (bool, error) {
	return iter.hasNext(ctx)
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
	positionBytes, err := json.Marshal(transformedRow[iter.orderingColumn])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal position: %w", err)
	}

	iter.lastProcessedVal = transformedRow[iter.orderingColumn]

	metadata := sdk.Metadata{
		metadataTable: iter.table,
	}
	metadata.SetCreatedAt(time.Now().UTC())

	return sdk.Util.Source.NewRecordCreate(
		positionBytes,
		metadata,
		key,
		sdk.RawData(rowBytes),
	), nil
}

// Stop stops iterators and closes database connection.
func (iter *Iterator) Stop() error {
	if iter.rows != nil {
		if err := iter.rows.Close(); err != nil {
			return fmt.Errorf("stop iterator: %w", err)
		}
	}

	return nil
}

// hasNext returns a bool indicating whether the source has the next record to return or not.
func (iter *Iterator) hasNext(ctx context.Context) (bool, error) {
	if iter.rows != nil && iter.rows.Next() {
		return true, nil
	}

	if err := iter.loadRows(ctx); err != nil {
		return false, fmt.Errorf("load rows: %w", err)
	}

	return iter.rows.Next(), nil
}

// loadRows selects a batch of rows from a database, based on the
// table, columns, orderingColumn, batchSize and the current position.
func (iter *Iterator) loadRows(ctx context.Context) error {
	whereClause := ""
	args := make([]any, 0, 1)
	if iter.lastProcessedVal != nil {
		whereClause = fmt.Sprintf(whereClauseFmt, iter.orderingColumn)
		args = append(args, iter.lastProcessedVal)
	}

	query := fmt.Sprintf(querySelectRowsFmt, iter.table, whereClause, iter.orderingColumn, iter.batchSize)

	rows, err := iter.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select query %q, %v: %w", query, args, err)
	}

	iter.rows = rows

	return nil
}

// populateKeyColumns populates keyColumn from the database's metadata
// or from the orderingColumn configuration field.
func (iter *Iterator) populateKeyColumns(ctx context.Context) error {
	if len(iter.keyColumns) != 0 {
		return nil
	}

	var (
		query = fmt.Sprintf(querySelectPKs, "")
		args  = []any{iter.table}
	)

	if iter.schema != "" {
		query = fmt.Sprintf(querySelectPKs, whereClauseSchema)
		args = append(args, iter.schema)
	}

	rows, err := iter.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("execute select primary keys %q, %v: %w", query, args, err)
	}
	defer rows.Close()

	keyColumn := ""
	for rows.Next() {
		if err = rows.Scan(&keyColumn); err != nil {
			return fmt.Errorf("scan key column value: %w", err)
		}

		iter.keyColumns = append(iter.keyColumns, keyColumn)
	}

	if len(iter.keyColumns) != 0 {
		return nil
	}

	iter.keyColumns = []string{iter.orderingColumn}

	return nil
}
