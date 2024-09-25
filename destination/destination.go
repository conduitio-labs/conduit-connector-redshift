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

	"github.com/conduitio-labs/conduit-connector-redshift/destination/config"
	"github.com/conduitio-labs/conduit-connector-redshift/destination/writer"
	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/jackc/pgx/v5/stdlib" // sql driver
)

// driverName is a database driver name.
const driverName = "pgx"

//go:generate mockgen -package mock -source destination.go -destination ./mock/destination.go

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	Insert(context.Context, opencdc.Record) error
	Update(context.Context, opencdc.Record) error
	Delete(context.Context, opencdc.Record) error
	Stop() error
}

// Destination is a Redshift destination plugin.
type Destination struct {
	sdk.UnimplementedDestination

	config config.Config
	writer Writer
}

// NewDestination initialises a new Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() commonsConfig.Parameters {
	return d.config.Parameters()
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (d *Destination) Configure(ctx context.Context, cfg commonsConfig.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Redshift Destination...")

	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, NewDestination().Parameters())
	if err != nil {
		return err //nolint: wrapcheck // not needed here
	}

	err = d.config.Validate()
	if err != nil {
		return fmt.Errorf("error validating configuration: %w", err)
	}

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
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
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
