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

package source

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/conduitio-labs/conduit-connector-redshift/source/config"
	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/jackc/pgx/v5/stdlib" // sql driver
	"github.com/jmoiron/sqlx"
)

// driverName is a database driver name.
const driverName = "pgx"

//go:generate mockgen -package mock -source source.go -destination ./mock/source.go

// Source is an Amazon Redshift source plugin.
type Source struct {
	sdk.UnimplementedSource

	db       *sqlx.DB
	config   config.Config
	position *Position
	ch       chan opencdc.Record
	wg       *sync.WaitGroup
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() commonsConfig.Parameters {
	return s.config.Parameters()
}

// Configure parses and stores configurations,
// returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw commonsConfig.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Amazon Redshift Source...")

	err := sdk.Util.ParseConfig(ctx, cfgRaw, &s.config, NewSource().Parameters())
	if err != nil {
		return err //nolint: wrapcheck // not needed here
	}

	s.config = s.config.Init()

	err = s.config.Validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
	}

	return nil
}

// Open parses the position and initializes the iterator.
func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening an Amazon Redshift Source...")

	var err error
	s.position, err = ParseSDKPosition(position)
	if err != nil {
		return err
	}

	// Open database connection
	s.db, err = sqlx.Open(driverName, s.config.DSN)
	if err != nil {
		return fmt.Errorf("failed opening db connection: %w", err)
	}

	// Check the connection
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	err = s.db.PingContext(pingCtx)
	if err != nil {
		return fmt.Errorf("ping db with timeout: %w", err)
	}

	s.ch = make(chan opencdc.Record, s.config.BatchSize)
	s.wg = &sync.WaitGroup{}

	for table, tableConfig := range s.config.Tables {
		s.wg.Add(1)

		tablePosition, _ := s.position.get(table)
		// A new iterator for each table
		err := NewIterator(ctx, IteratorConfig{
			db:             s.db,
			position:       tablePosition,
			sourcePosition: s.position,
			table:          table,
			orderingColumn: tableConfig.OrderingColumn,
			keyColumns:     tableConfig.KeyColumns,
			batchSize:      s.config.BatchSize,
			pollingPeriod:  s.config.PollingPeriod,
			snapshot:       s.config.Snapshot,
			dsn:            s.config.DSN,
			wg:             s.wg,
			ch:             s.ch,
		})
		if err != nil {
			return fmt.Errorf("new iterator: %w", err)
		}
	}

	return nil
}

// Read returns the next record.
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	sdk.Logger(ctx).Debug().Msg("Reading a record from Amazon Redshift Source...")

	if s == nil || s.ch == nil {
		return opencdc.Record{}, errors.New("source not opened for reading")
	}

	select {
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	case record, ok := <-s.ch:
		if !ok {
			return opencdc.Record{}, fmt.Errorf("error reading data, records channel closed unexpectedly")
		}
		return record, nil //nolint:nlreturn // compact code style
	default:
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}
}

// Ack logs the debug event with the position.
func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Trace().
		Str("position", string(position)).
		Msg("got ack")

	return nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down the Amazon Redshift Source")

	if s.wg != nil {
		// wait for goroutines to finish
		s.wg.Wait()
	}

	if s.ch != nil {
		// close the read channel for write
		close(s.ch)
		// reset read channel to nil, to avoid reading buffered records
		s.ch = nil
	}

	if s.db != nil {
		err := s.db.Close()
		if err != nil {
			return fmt.Errorf("close db: %w", err)
		}
	}

	return nil
}
