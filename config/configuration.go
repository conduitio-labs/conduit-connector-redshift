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

package config

import (
	"fmt"
	"strings"
)

const (
	// DSN is the configuration name of the data source name to connect to the Amazon Redshift.
	DSN = "dsn"
	// Table is the configuration name of the table.
	Table = "table"
)

// Configuration is the general configurations needed to connect to Amazon Redshift database.
type Configuration struct {
	// DSN is the configuration of the data source name to connect to the Amazon Redshift.
	DSN string `key:"dsn" validate:"required"`
	// Table is the configuration of the table name.
	Table string `key:"table" validate:"required"`
}

// parseConfiguration parses a general configuration.
func parseConfiguration(cfg map[string]string) (Configuration, error) {
	config := Configuration{
		DSN:   strings.TrimSpace(cfg[DSN]),
		Table: strings.TrimSpace(cfg[Table]),
	}

	err := validateStruct(config)
	if err != nil {
		return Configuration{}, fmt.Errorf("validate general configuration: %w", err)
	}

	return config, nil
}
