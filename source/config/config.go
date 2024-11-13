// Copyright © 2024 Meroxa, Inc. & Yalantis
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

//go:generate paramgen -output=paramgen.go Config

package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-redshift/common"
)

// Config contains source-specific configurable values.
type Config struct {
	common.Configuration

	// Deprecated: use `tables` instead
	Table string `json:"table"`
	// Deprecated: use `tables.*.keyColumns` instead
	KeyColumns []string `json:"keyColumns"`
	// Deprecated: use `tables.*.orderingColumn` instead
	OrderingColumn string `json:"orderingColumn"`

	// Tables is the table names and their configs to pull data from
	Tables map[string]TableConfig `json:"tables"`
	// Snapshot is the configuration that determines whether the connector
	// will take a snapshot of the entire table before starting cdc mode.
	Snapshot bool `json:"snapshot" default:"true"`
	// BatchSize is a size of rows batch.
	BatchSize int `json:"batchSize" default:"1000" validate:"gt=0,lt=100001"`
	// This period is used by iterator to poll for new data at regular intervals.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"5s"`
}

type TableConfig struct {
	// KeyColumns is the configuration list of column names to build the opencdc.Record.Key.
	KeyColumns []string `json:"keyColumns"`
	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"orderingColumn"`
}

// Init sets the desired value on Tables while Table, OrderingColumn & KeyColumns are being deprecated
// and converts all the table names into lowercase.
func (c Config) Init() Config {
	if c.Table != "" && c.OrderingColumn != "" && len(c.Tables) == 0 {
		tables := make(map[string]TableConfig, 1)

		tables[c.Table] = TableConfig{
			KeyColumns:     c.KeyColumns,
			OrderingColumn: c.OrderingColumn,
		}

		c.Tables = tables
		c.Table = ""
	}

	tables := make(map[string]TableConfig)
	for tableName, tableConfig := range c.Tables {
		tables[strings.ToLower(tableName)] = tableConfig
	}
	c.Tables = tables

	return c
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c Config) Validate() error {
	// ensure that both `table` and `tables` are not provided
	if len(c.Tables) > 0 && c.Table != "" {
		return fmt.Errorf(`error validating "tables": cannot provide both "table" and "tables", use "tables" only`)
	}

	// ensure at least one table is configured.
	if len(c.Tables) == 0 {
		return fmt.Errorf(`error validating "tables": required parameter is not provided`)
	}

	// ensure `orderingColumn` is provided if `table` is being used.
	if c.OrderingColumn == "" && c.Table != "" {
		return fmt.Errorf(`error validating "orderingColumn": required if "table" is being used`)
	}

	// validate each table and table config.
	for tableName, tableConfig := range c.Tables {
		// handling "excludesall= " and "lte=127" validations for table name
		if strings.Contains(tableName, " ") {
			return common.NewExcludesSpacesError(ConfigTable)
		}
		if len(tableName) > common.MaxConfigStringLength {
			return common.NewLessThanError(ConfigTable, common.MaxConfigStringLength)
		}

		// validate table config
		if err := tableConfig.validate(); err != nil {
			return err
		}
	}

	return nil
}

// Validate checks the individual table configuration.
func (tc TableConfig) validate() error {
	// tc.OrderingColumn required validation is handled in stuct tag
	// handling "required", "lowercase", "excludesall= " and "lte=127" validations.
	if tc.OrderingColumn == "" {
		return fmt.Errorf(`error validating %s: required parameter is not provided`, ConfigOrderingColumn)
	}
	if tc.OrderingColumn != strings.ToLower(tc.OrderingColumn) {
		return common.NewLowercaseError(ConfigOrderingColumn)
	}
	if strings.Contains(tc.OrderingColumn, " ") {
		return common.NewExcludesSpacesError(ConfigOrderingColumn)
	}
	if len(tc.OrderingColumn) > common.MaxConfigStringLength {
		return common.NewLessThanError(ConfigOrderingColumn, common.MaxConfigStringLength)
	}

	if len(tc.KeyColumns) == 0 {
		return nil
	}
	// handling "lowercase", "excludesall= " and "lte=127" validations on each key column from tc.KeyColumns
	for _, keyColumn := range tc.KeyColumns {
		if keyColumn != strings.ToLower(keyColumn) {
			return common.NewLowercaseError(ConfigKeyColumns)
		}
		if strings.Contains(keyColumn, " ") {
			return common.NewExcludesSpacesError(ConfigKeyColumns)
		}
		if len(keyColumn) > common.MaxConfigStringLength {
			return common.NewLessThanError(ConfigKeyColumns, common.MaxConfigStringLength)
		}
	}

	return nil
}
