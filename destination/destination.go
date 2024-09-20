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

package destination

import (
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-redshift/config"
	destConfig "github.com/conduitio-labs/conduit-connector-redshift/destination/config"
	"github.com/conduitio-labs/conduit-connector-redshift/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/jackc/pgx/v5/stdlib" // sql driver
)

// driverName is a database driver name.
const driverName = "pgx"

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	Insert(context.Context, sdk.Record) error
	Update(context.Context, sdk.Record) error
	Delete(context.Context, sdk.Record) error
	Stop() error
}

// Destination is a Redshift destination plugin.
type Destination struct {
	sdk.UnimplementedDestination

	config destConfig.Config
	writer Writer
}

// NewDestination initialises a new Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.DSN: {
			Default:     "",
			Required:    true,
			Description: "Data source name to connect to the Amazon Redshift.",
		},
		config.Table: {
			Default:     "",
			Required:    true,
			Description: "Name of the table that the connector should read.",
		},
		config.KeyColumns: {
			Default:  "",
			Required: false,
			Description: "Comma-separated list of column names to build the where clause " +
				"in case if sdk.Record.Key is empty.",
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Redshift Destination...")

	// var err error

	// // d.config, err = config.ParseDestination(cfg)
	// // if err != nil {
	// // 	return fmt.Errorf("parse destination config: %w", err)
	// // }

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Opening a Redshift Destination...")

	var err error

	d.writer, err = writer.NewWriter(ctx, driverName, d.config)
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes records into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i := range records {
		sdk.Logger(ctx).Debug().Bytes("record", records[i].Bytes()).
			Msg("Writing a record into Redshift Destination...")

		err := sdk.Util.Destination.Route(ctx, records[i],
			d.writer.Insert,
			d.writer.Update,
			d.writer.Delete,
			d.writer.Insert,
		)
		if err != nil {
			if records[i].Key != nil {
				return i, fmt.Errorf("key %s: %w", string(records[i].Key.Bytes()), err)
			}

			return i, fmt.Errorf("record with no key: %w", err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down the Redshift Destination")

	if d.writer != nil {
		if err := d.writer.Stop(); err != nil {
			return fmt.Errorf("stop writer: %w", err)
		}
	}

	return nil
}
