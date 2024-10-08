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

//go:generate paramgen -output=paramgen.go Config

package config

import (
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-redshift/common"
)

// Config contains source-specific configurable values.
type Config struct {
	common.Configuration
	// Tables is a list of table names to pull data from.
	Tables []string `json:"tables" validate:"required"`
	// OrderingColumns is a list of corresponding ordering columns for the table
	// that the connector will use for ordering rows.
	OrderingColumns []string `json:"orderingColumns" validate:"required"`
	// Snapshot is the configuration that determines whether the connector
	// will take a snapshot of the entire table before starting cdc mode.
	Snapshot bool `json:"snapshot" default:"true"`
	// BatchSize is a size of rows batch.
	BatchSize int `json:"batchSize" default:"1000"`
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c *Config) Validate() error {
	// c.DSN has required validation handled in struct tag.

	// Ensure there is at least one table and one corresponding ordering column.
	if len(c.Tables) == 0 || len(c.OrderingColumns) == 0 {
		return common.NewNoTablesOrColumnsError()
	}

	// Ensure that the number of tables and ordering columns match.
	if len(c.Tables) != len(c.OrderingColumns) {
		return common.NewMismatchedTablesAndColumnsError(len(c.Tables), len(c.OrderingColumns))
	}

	// c.Tables required validation is handled in stuct tag
	// handling "lowercase", "excludesall= " and "lte=127" validations.
	for i, table := range c.Tables {
		if table != strings.ToLower(table) {
			return common.NewLowercaseError(fmt.Sprintf("table[%d]", i))
		}
		if strings.Contains(table, " ") {
			return common.NewExcludesSpacesError(fmt.Sprintf("table[%d]", i))
		}
		if len(table) > common.MaxConfigStringLength {
			return common.NewLessThanError(fmt.Sprintf("table[%d]", i), common.MaxConfigStringLength)
		}
	}

	// c.OrderingColumn handling "lowercase", "excludesall= " and "lte=127" validations.
	for i, col := range c.OrderingColumns {
		if col != strings.ToLower(col) {
			return common.NewLowercaseError(fmt.Sprintf("orderingColumn[%d]", i))
		}
		if strings.Contains(col, " ") {
			return common.NewExcludesSpacesError(fmt.Sprintf("orderingColumn[%d]", i))
		}
		if len(col) > common.MaxConfigStringLength {
			return common.NewLessThanError(fmt.Sprintf("orderingColumn[%d]", i), common.MaxConfigStringLength)
		}
	}

	// c.BatchSize handling "gte=1" and "lte=100000" validations.
	if c.BatchSize < common.MinConfigBatchSize {
		return common.NewGreaterThanError(ConfigBatchSize, common.MinConfigBatchSize)
	}
	if c.BatchSize > common.MaxConfigBatchSize {
		return common.NewLessThanError(ConfigBatchSize, common.MaxConfigBatchSize)
	}

	return nil
}

func (c *Config) GetTableOrderingMap() map[string]string {
	tableOrderingMap := make(map[string]string)

	for i := range c.Tables {
		tableOrderingMap[c.Tables[i]] = c.OrderingColumns[i]
	}

	return tableOrderingMap
}
