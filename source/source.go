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
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/conduitio-labs/conduit-connector-redshift/config"
	"github.com/conduitio-labs/conduit-connector-redshift/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"go.uber.org/multierr"

	_ "github.com/jackc/pgx/v5/stdlib" // sql driver
)

const (
	// driverName is a database driver name.
	driverName = "pgx"
	// keySearchPath is a key of get parameter of a datatable's schema name.
	keySearchPath = "search_path"

	// querySelectLastProcessedValFmt is a query pattern to select the last value of the orderingColumn column.
	querySelectLastProcessedValFmt = "SELECT %s FROM %s ORDER BY %s DESC LIMIT 1;"
)

// Iterator interface.
type Iterator interface {
	HasNext(context.Context) (bool, error)
	Next(context.Context) (sdk.Record, error)
	Stop() error
}

// Source is an Amazon Redshift source plugin.
type Source struct {
	sdk.UnimplementedSource

	config           config.Source
	db               *sqlx.DB
	lastProcessedVal any
	iterator         Iterator
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.DSN: {
			Default:     "",
			Required:    true,
			Description: "data source name to connect to the Amazon Redshift.",
		},
		config.Table: {
			Default:     "",
			Required:    true,
			Description: "Name of the table that the connector should read.",
		},
		config.OrderingColumn: {
			Default:  "",
			Required: true,
			Description: "Column name that the connector will use for ordering rows. Column must contain unique " +
				"values and suitable for sorting, otherwise the snapshot won't work correctly.",
		},
		config.CopyExistingData: {
			Default:  "true",
			Required: false,
			Description: "The field determines whether the connector will move already existing data " +
				"or only those that will be available after it starts.",
		},
		config.KeyColumns: {
			Default:     "",
			Required:    false,
			Description: "Comma-separated list of column names to build the sdk.Record.Key.",
		},
		config.BatchSize: {
			Default:     "1000",
			Required:    false,
			Description: "Size of rows batch. Min is 1 and max is 100000. The default is 1000.",
		},
	}
}

// Configure parses and stores configurations,
// returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Amazon Redshift Source...")

	cfg, err := config.ParseSource(cfgRaw)
	if err != nil {
		return fmt.Errorf("parse source config: %w", err)
	}

	s.config = cfg

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening an Amazon Redshift Source...")

	var err error

	s.db, err = sqlx.Open(driverName, s.config.DSN)
	if err != nil {
		return fmt.Errorf("open db connection: %w", err)
	}

	err = s.db.Ping()
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	// if position is nil will use it, if it's not and copyExistingData is false -
	// populate it with the value of the last row orderingColumn column
	if position != nil {
		if err = json.Unmarshal(position, &s.lastProcessedVal); err != nil {
			return fmt.Errorf("unmarshal sdk.Position into Position: %w", err)
		}
	} else if !s.config.CopyExistingData {
		if err = s.populateLastProcessedVal(ctx); err != nil {
			return fmt.Errorf("populate last processed value: %w", err)
		}
	}

	u, err := url.Parse(s.config.DSN)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}

	s.iterator, err = iterator.New(ctx, iterator.Params{
		DB:               s.db,
		LastProcessedVal: s.lastProcessedVal,
		Table:            s.config.Table,
		KeyColumns:       s.config.KeyColumns,
		OrderingColumn:   s.config.OrderingColumn,
		BatchSize:        s.config.BatchSize,
		Schema:           u.Query().Get(keySearchPath),
	})
	if err != nil {
		return fmt.Errorf("new iterator: %w", err)
	}

	return nil
}

// Read returns the next record.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	sdk.Logger(ctx).Debug().Msg("Reading a record from Amazon Redshift Source...")

	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("next: %w", err)
	}

	return record, nil
}

// Ack appends the last processed value to the slice to clear the tracking table in the future.
func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")

	return nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) (err error) {
	sdk.Logger(ctx).Info().Msg("Tearing down the Amazon Redshift Source")

	if s.iterator != nil {
		err = s.iterator.Stop()
	}

	if s.db != nil {
		err = multierr.Append(err, s.db.Close())
	}

	return
}

// populateLastProcessedVal selects the last value of orderingColumn column
// and sets it to the lastProcessedVal.
func (s *Source) populateLastProcessedVal(ctx context.Context) error {
	query := fmt.Sprintf(querySelectLastProcessedValFmt,
		s.config.OrderingColumn, s.config.Table, s.config.OrderingColumn)

	rows, err := s.db.QueryxContext(ctx, query)
	if err != nil {
		return fmt.Errorf("execute select last processed value query %q: %w", query, err)
	}

	for rows.Next() {
		if err = rows.Scan(&s.lastProcessedVal); err != nil {
			return fmt.Errorf("scan last processed value: %w", err)
		}
	}

	return nil
}
