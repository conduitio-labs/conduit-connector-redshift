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
	"strings"

	"github.com/conduitio-labs/conduit-connector-redshift/common"
)

// Config contains source-specific configurable values.
type Config struct {
	common.Configuration
	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"orderingColumn" validate:"required"`
	// Snapshot is the configuration that determines whether the connector
	// will take a snapshot of the entire table before starting cdc mode.
	Snapshot bool `json:"snapshot" default:"true"`
	// BatchSize is a size of rows batch.
	BatchSize int `json:"batchSize" default:"1000"`
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c *Config) Validate() error {
	// c.DSN has required validation handled in struct tag.

	// c.Table required validation is handled in stuct tag
	// handling "lowercase", "excludesall= " and "lte=127" validations.
	if c.Table != strings.ToLower(c.Table) {
		return common.NewLowercaseError(ConfigTable)
	}
	if strings.Contains(c.Table, " ") {
		return common.NewExcludesSpacesError(ConfigTable)
	}
	if len(c.Table) > common.MaxConfigStringLength {
		return common.NewLessThanError(ConfigTable, common.MaxConfigStringLength)
	}

	// c.KeyColumns handling "lowercase", "excludesall= " and "lte=127" validations.
	for _, v := range c.KeyColumns {
		if v != strings.ToLower(v) {
			return common.NewLowercaseError(ConfigKeyColumns)
		}
		if strings.Contains(v, " ") {
			return common.NewExcludesSpacesError(ConfigKeyColumns)
		}
		if len(v) > common.MaxConfigStringLength {
			return common.NewLessThanError(ConfigKeyColumns, common.MaxConfigStringLength)
		}
	}

	// c.OrderingColumn handling "lowercase", "excludesall= " and "lte=127" validations.
	if c.OrderingColumn != strings.ToLower(c.OrderingColumn) {
		return common.NewLowercaseError(ConfigOrderingColumn)
	}
	if strings.Contains(c.OrderingColumn, " ") {
		return common.NewExcludesSpacesError(ConfigOrderingColumn)
	}
	if len(c.OrderingColumn) > common.MaxConfigStringLength {
		return common.NewLessThanError(ConfigOrderingColumn, common.MaxConfigStringLength)
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
