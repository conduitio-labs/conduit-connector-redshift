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

	"github.com/conduitio-labs/conduit-connector-redshift/source/config"
	"github.com/conduitio-labs/conduit-connector-redshift/source/iterator"
	commonsConfig "github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/jackc/pgx/v5/stdlib" // sql driver
)

// driverName is a database driver name.
const driverName = "pgx"

//go:generate mockgen -package mock -source source.go -destination ./mock/source.go

// Iterator interface.
type Iterator interface {
	HasNext(context.Context) (bool, error)
	Next(context.Context) (opencdc.Record, error)
	Stop() error
}

// Source is an Amazon Redshift source plugin.
type Source struct {
	sdk.UnimplementedSource

	config   config.Config
	iterator Iterator
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
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	sdk.Logger(ctx).Debug().Msg("Reading a record from Amazon Redshift Source...")

	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("next: %w", err)
	}

	return record, nil
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

	if s.iterator != nil {
		if err := s.iterator.Stop(); err != nil {
			return fmt.Errorf("stop iterator: %w", err)
		}
	}

	return nil
}
