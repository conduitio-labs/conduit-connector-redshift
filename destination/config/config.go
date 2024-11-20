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
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/conduitio-labs/conduit-connector-redshift/common"
	"github.com/conduitio/conduit-commons/opencdc"
)

type TableFn func(opencdc.Record) (string, error)

// Config is a destination configuration needed to connect to Redshift database.
type Config struct {
	common.Configuration
	// Table is the configuration of the table name.
	Table string `json:"table" default:"{{ index .Metadata \"opencdc.collection\" }}"`
	// KeyColumns is the configuration of comma-separated column names to build the record key.
	KeyColumns []string `json:"keyColumns"`
}

// Init sets lowercase "table" name if not a template.
func (c Config) Init() Config {
	if !c.isTableTemplate() {
		c.Table = strings.ToLower(c.Table)
	}

	return c
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c Config) Validate() error {
	// c.DSN has required validation handled in struct tag

	// handling "excludesall= " and "lte=127" validations for c.Table
	if !c.isTableTemplate() && strings.Contains(c.Table, " ") {
		return common.NewExcludesSpacesError(ConfigTable)
	}
	if len(c.Table) > common.MaxConfigStringLength {
		return common.NewLessThanError(ConfigTable, common.MaxConfigStringLength)
	}

	// c.KeyColumns handling "lowercase", "excludesall= " and "lte=127" validations
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

	return nil
}

// TableFunction returns a function that determines the table for each record individually.
// The function might be returning a static table name.
// If the table is neither static nor a template, an error is returned.
func (c Config) TableFunction() (f TableFn, err error) {
	// Not a template, i.e. it's a static table name
	if !c.isTableTemplate() {
		return func(_ opencdc.Record) (string, error) {
			return c.Table, nil
		}, nil
	}

	// Try to parse the table
	t, err := template.New("table").Funcs(sprig.FuncMap()).Parse(c.Table)
	if err != nil {
		// The table is not a valid Go template.
		return nil, fmt.Errorf("table is neither a valid static table nor a valid Go template: %w", err)
	}

	// The table is a valid template, return TableFn.
	var buf bytes.Buffer

	return func(r opencdc.Record) (string, error) {
		buf.Reset()
		if err := t.Execute(&buf, r); err != nil {
			return "", fmt.Errorf("failed to execute table template: %w", err)
		}

		return buf.String(), nil
	}, nil
}

// isTableTemplate returns true if "table" contains a template placeholder.
func (c Config) isTableTemplate() bool {
	return strings.Contains(c.Table, "{{") && strings.Contains(c.Table, "}}")
}
