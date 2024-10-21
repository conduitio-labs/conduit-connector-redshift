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

package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-redshift/columntypes"
	"github.com/conduitio-labs/conduit-connector-redshift/destination/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

type ColumnTypes map[string]string

const (
	// keySearchPath is a key of get parameter of a datatable's schema name.
	keySearchPath = "search_path"
	// pingTimeout is a database ping timeout.
	pingTimeout = 10 * time.Second
)

// Writer implements a writer logic for Redshift destination.
type Writer struct {
	db *sqlx.DB
	// Schema name to write data into.
	schema string
	// Table name provided through config.
	tableName string
	// Key columns provided through config.
	keyColumns []string
	// Function to dynamically get table name for each record.
	tableNameFunc config.TableFn
	// Maps table names to their column types.
	columnTypes map[string]ColumnTypes
}

// NewWriter creates new instance of the Writer.
func NewWriter(ctx context.Context, driverName string, config config.Config) (*Writer, error) {
	tableFn, err := config.TableFunction()
	if err != nil {
		return nil, fmt.Errorf("invalid table name or table function: %w", err)
	}

	writer := &Writer{
		tableName:     config.Table,
		keyColumns:    config.KeyColumns,
		tableNameFunc: tableFn,
		columnTypes:   make(map[string]ColumnTypes),
	}

	writer.db, err = sqlx.Open(driverName, config.DSN)
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	err = writer.db.PingContext(ctxTimeout)
	if err != nil {
		return nil, fmt.Errorf("ping db with timeout: %w", err)
	}

	u, err := url.Parse(config.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	writer.schema = u.Query().Get(keySearchPath)

	return writer, nil
}

// Insert inserts a record.
func (w *Writer) Insert(ctx context.Context, record opencdc.Record) error {
	table, err := w.tableNameFunc(record)
	if err != nil {
		return err
	}

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty return empty payload error
	if payload == nil {
		return ErrNoPayload
	}

	columnTypes, err := w.getColumnTypes(ctx, table)
	if err != nil {
		return fmt.Errorf("get column types: %w", err)
	}

	payload, err = columntypes.ConvertStructuredData(columnTypes, payload)
	if err != nil {
		return fmt.Errorf("convert structure data: %w", err)
	}

	columns, values := w.extractColumnsAndValues(payload)

	query, args := sqlbuilder.PostgreSQL.NewInsertBuilder().
		InsertInto(table).
		Cols(columns...).
		Values(values...).
		Build()

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec insert %q, %v: %w", query, args, err)
	}

	return nil
}

// Update updates a record.
func (w *Writer) Update(ctx context.Context, record opencdc.Record) error {
	table, err := w.tableNameFunc(record)
	if err != nil {
		return err
	}

	payload, key, err := w.preparePayloadAndKey(record)
	if err != nil {
		return err
	}

	columnTypes, err := w.getColumnTypes(ctx, table)
	if err != nil {
		return err
	}

	payload, err = columntypes.ConvertStructuredData(columnTypes, payload)
	if err != nil {
		return fmt.Errorf("convert structure data: %w", err)
	}

	key, err = w.populateKey(key, payload)
	if err != nil {
		return fmt.Errorf("populate key with keyColumns: %w", err)
	}

	keyColumns, err := w.getKeyColumns(key)
	if err != nil {
		return fmt.Errorf("get key columns: %w", err)
	}

	// remove keys from the payload
	for i := range keyColumns {
		delete(payload, keyColumns[i])
	}

	columns, values := w.extractColumnsAndValues(payload)

	ub := sqlbuilder.PostgreSQL.NewUpdateBuilder().
		Update(table)

	assignments := make([]string, len(columns))
	for i := range columns {
		assignments[i] = ub.Assign(columns[i], values[i])
	}
	ub.Set(assignments...)

	for i := range keyColumns {
		ub.Where(ub.Equal(keyColumns[i], key[keyColumns[i]]))
	}

	query, args := ub.Build()

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec update %q, %v: %w", query, args, err)
	}

	return nil
}

// Delete deletes a record.
func (w *Writer) Delete(ctx context.Context, record opencdc.Record) error {
	table, err := w.tableNameFunc(record)
	if err != nil {
		return err
	}

	key, err := w.structurizeData(record.Key)
	if err != nil {
		return fmt.Errorf("structurize key: %w", err)
	}

	keyColumns, err := w.getKeyColumns(key)
	if err != nil {
		return fmt.Errorf("get key columns: %w", err)
	}

	db := sqlbuilder.PostgreSQL.NewDeleteBuilder().
		DeleteFrom(table)

	for i := range keyColumns {
		db.Where(db.Equal(keyColumns[i], key[keyColumns[i]]))
	}

	query, args := db.Build()

	_, err = w.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("exec delete %q, %v: %w", query, args, err)
	}

	return nil
}

// Stop closes database connection.
func (w *Writer) Stop() error {
	if w.db != nil {
		if err := w.db.Close(); err != nil {
			return fmt.Errorf("close db connection: %w", err)
		}
	}

	return nil
}

func (w *Writer) preparePayloadAndKey(record opencdc.Record) (map[string]interface{}, map[string]interface{}, error) {
	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return nil, nil, fmt.Errorf("structurize payload: %w", err)
	}

	if payload == nil {
		return nil, nil, ErrNoPayload
	}

	key, err := w.structurizeData(record.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("structurize key: %w", err)
	}

	return payload, key, nil
}

// getKeyColumns returns either all the keys of the opencdc.Record's Key field.
func (w *Writer) getKeyColumns(key opencdc.StructuredData) ([]string, error) {
	if len(key) == 0 {
		return nil, ErrNoKey
	}

	keyColumns := make([]string, 0, len(key))
	for k := range key {
		keyColumns = append(keyColumns, k)
	}

	return keyColumns, nil
}

// structurizeData converts opencdc.Data to opencdc.StructuredData.
func (w *Writer) structurizeData(data opencdc.Data) (opencdc.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return opencdc.StructuredData{}, nil
	}

	structuredData := make(opencdc.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	return structuredData, nil
}

// extractColumnsAndValues turns the payload into slices
// of columns and values for inserting into Redshift.
func (w *Writer) extractColumnsAndValues(payload opencdc.StructuredData) ([]string, []any) {
	var (
		columns []string
		values  []any
	)

	for key, value := range payload {
		columns = append(columns, key)
		values = append(values, value)
	}

	return columns, values
}

// populateKey populates the key from the payload by provided keyColumns keys if it's empty and a static table is provided.
func (w *Writer) populateKey(key opencdc.StructuredData, payload opencdc.StructuredData) (opencdc.StructuredData, error) {
	if key != nil {
		return key, nil
	}

	// keyColumns not available for non-static tables
	if strings.Contains(w.tableName, "{{") && strings.Contains(w.tableName, "}}") {
		return nil, ErrNoKey
	}

	key = make(opencdc.StructuredData, len(w.keyColumns))
	for i := range w.keyColumns {
		val, ok := payload[w.keyColumns[i]]
		if !ok {
			return nil, fmt.Errorf("key column %q not found", w.keyColumns[i])
		}

		key[w.keyColumns[i]] = val
	}

	return key, nil
}

// getColumnTypes returns column types from w.tableColumnTypes, or queries the database if not exists.
func (w *Writer) getColumnTypes(ctx context.Context, table string) (map[string]string, error) {
	if columnTypes, ok := w.columnTypes[table]; ok {
		return columnTypes, nil
	}

	columnTypes, err := columntypes.GetColumnTypes(ctx, w.db, table, w.schema)
	if err != nil {
		return nil, fmt.Errorf("get column types: %w", err)
	}
	w.columnTypes[table] = columnTypes

	return columnTypes, nil
}
