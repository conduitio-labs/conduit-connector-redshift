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

package common

import (
	"strings"
)

const (
	MaxConfigStringLength = 127
	ConfigTable           = "table"
	ConfigKeyColumns      = "keyColumns"
)

// Configuration contains common for source and destination configurable values.
type Configuration struct {
	// DSN is the configuration of the data source name to connect to the Amazon Redshift.
	DSN string `json:"dsn" validate:"required"`
	// Table is the configuration of the table name.
	Table string `json:"table" validate:"required"`
	// KeyColumns is the configuration list of column names to build the opencdc.Record.Key (for Source).
	KeyColumns []string `json:"keyColumns"`
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c *Configuration) Validate() error {
	// c.DSN has required validation handled in struct tag

	// c.Table required validation is handled in stuct tag
	// handling "lowercase", "excludesall= " and "lte=127" validations
	if c.Table != strings.ToLower(c.Table) {
		return NewLowercaseError(ConfigTable)
	}
	if strings.Contains(c.Table, " ") {
		return NewExcludesSpacesError(ConfigTable)
	}
	if len(c.Table) > MaxConfigStringLength {
		return NewLessThanError(ConfigTable, MaxConfigStringLength)
	}

	// c.KeyColumns handling "lowercase", "excludesall= " and "lte=127" validations
	for _, v := range c.KeyColumns {
		if v != strings.ToLower(v) {
			return NewLowercaseError(ConfigKeyColumns)
		}
		if strings.Contains(v, " ") {
			return NewExcludesSpacesError(ConfigKeyColumns)
		}
		if len(v) > MaxConfigStringLength {
			return NewLessThanError(ConfigKeyColumns, MaxConfigStringLength)
		}
	}

	return nil
}
