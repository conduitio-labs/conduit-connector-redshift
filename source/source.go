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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-redshift/config"
	"github.com/conduitio-labs/conduit-connector-redshift/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/jackc/pgx/v5/stdlib" // sql driver
)

// driverName is a database driver name.
const driverName = "pgx"

// Iterator interface.
type Iterator interface {
	HasNext(context.Context) (bool, error)
	Next(context.Context) (sdk.Record, error)
	Stop() error
}

// Source is an Amazon Redshift source plugin.
type Source struct {
	sdk.UnimplementedSource

	config   config.Source
	iterator Iterator
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.DSN: {
			Default: "",
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
			Description: "Data source name to connect to the Amazon Redshift.",
		},
		config.Table: {
			Default: "",
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
			Description: "Name of a table, the connector must read from.",
		},
		config.OrderingColumn: {
			Default: "",
			Validations: []sdk.Validation{
				sdk.ValidationRequired{},
			},
			Description: "Column name that the connector will use for ordering rows. Column must contain unique " +
				"values and suitable for sorting, otherwise the snapshot won't work correctly.",
		},
		config.Snapshot: {
			Default:     "true",
			Description: "Whether the connector will take a snapshot of the entire table before starting cdc mode.",
		},
		config.KeyColumns: {
			Default:     "",
			Description: "Comma-separated list of column names to build the sdk.Record.Key.",
		},
		config.BatchSize: {
			Default:     "1000",
			Description: "Size of rows batch. Min is 1 and max is 100000. The default is 1000.",
		},
	}
}

// Configure parses and stores configurations,
// returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Amazon Redshift Source...")

	var err error

	s.config, err = config.ParseSource(cfgRaw)
	if err != nil {
		return fmt.Errorf("parse source config: %w", err)
	}

	return nil
}

// Open parses the position and initializes the iterator.
func (s *Source) Open(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening an Amazon Redshift Source...")

	pos, err := iterator.ParseSDKPosition(position)
	if err != nil {
		return fmt.Errorf("parse position: %w", err)
	}

	s.iterator, err = iterator.New(ctx, driverName, pos, s.config)
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

// Ack logs the debug event with the position.
func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	sdk.Logger(ctx).Trace().
		Str("position", string(position)).
		Msg("got ack")

	return nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down the Amazon Redshift Source")

	if s.iterator != nil {
		if err := s.iterator.Stop(); err != nil {
			return fmt.Errorf("stop iterator: %w", err)
		}
	}

	return nil
}
