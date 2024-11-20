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
	"fmt"
	"sync"

	"github.com/conduitio-labs/conduit-connector-redshift/source/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/jmoiron/sqlx"
)

type Iterator struct {
	db       *sqlx.DB
	position *Position

	// tables to pull data from and their config
	tables map[string]config.TableConfig
	// batchSize is the size of a batch retrieved from Redshift
	batchSize int

	// index of the current table being iterated over
	currentTable int
	// store table iterators for each table
	tableIterators []*TableIterator

	mu sync.Mutex
}

// New creates a new instance of the iterator.
func New(ctx context.Context, driverName string, pos *Position, config config.Config) (*Iterator, error) {
	iterator := &Iterator{
		tables:       config.Tables,
		batchSize:    config.BatchSize,
		currentTable: 0,
		position:     pos,
	}

	if len(pos.TablePositions) == 0 {
		iterator.position = &Position{TablePositions: make(map[string]TablePosition)}
	}

	err := iterator.openDB(ctx, driverName, config.DSN)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	for tableName, tableConfig := range iterator.tables {
		position := iterator.position.TablePositions[tableName]

		ti, err := NewTableIterator(ctx, TableIteratorConfig{
			db:             iterator.db,
			position:       position,
			table:          tableName,
			orderingColumn: tableConfig.OrderingColumn,
			keyColumns:     tableConfig.KeyColumns,
			batchSize:      iterator.batchSize,
			snapshot:       config.Snapshot,
			dsn:            config.DSN,
		}, iterator)
		if err != nil {
			return nil, fmt.Errorf("create iterator for table %s: %w", tableName, err)
		}
		iterator.tableIterators = append(iterator.tableIterators, ti)
	}

	return iterator, nil
}

// HasNext returns a bool indicating whether the source has the next record to return or not.
func (iter *Iterator) HasNext(ctx context.Context) (bool, error) {
	if iter.currentTable == len(iter.tables) {
		iter.currentTable = 0
	}

	for iter.currentTable < len(iter.tables) {
		ti := iter.tableIterators[iter.currentTable]

		hasNext, err := ti.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("error checking next for table %s: %w", ti.table, err)
		}

		if hasNext {
			return true, nil
		}

		iter.currentTable++
	}

	return false, nil
}

// Next returns the next record.
func (iter *Iterator) Next(ctx context.Context) (opencdc.Record, error) {
	if iter.currentTable < len(iter.tables) {
		ti := iter.tableIterators[iter.currentTable]

		record, err := ti.Next(ctx)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("error getting next record from table %s: %w", ti.table, err)
		}

		return record, nil
	}

	return opencdc.Record{}, fmt.Errorf("no more records")
}

// Stop stops iterator and closes database connection.
func (iter *Iterator) Stop() error {
	for _, ti := range iter.tableIterators {
		err := ti.Stop()
		if err != nil {
			return fmt.Errorf("stop table iterator: %w", err)
		}
	}

	if iter.db != nil {
		err := iter.db.Close()
		if err != nil {
			return fmt.Errorf("close db: %w", err)
		}
	}

	return nil
}

func (iter *Iterator) openDB(ctx context.Context, driverName string, dsn string) error {
	var err error
	iter.db, err = sqlx.Open(driverName, dsn)
	if err != nil {
		return fmt.Errorf("open db connection: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	err = iter.db.PingContext(pingCtx)
	if err != nil {
		return fmt.Errorf("ping db with timeout: %w", err)
	}

	return nil
}

func (iter *Iterator) updateTablePosition(table string, newPosition TablePosition) {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	iter.position.TablePositions[table] = newPosition
}

func (iter *Iterator) getPosition() *Position {
	iter.mu.Lock()
	defer iter.mu.Unlock()

	position := iter.position

	return position
}
