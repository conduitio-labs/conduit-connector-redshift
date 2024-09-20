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

package common

// const (
// 	// DSN is the configuration name of the data source name to connect to the Amazon Redshift.
// 	DSN = "dsn"
// 	// Table is the configuration name of the table name.
// 	Table = "table"
// 	// KeyColumns is the configuration name of comma-separated column names to build the opencdc.Record.Key.
// 	KeyColumns = "keyColumns"
// )

// Configuration contains common for source and destination configurable values.
type Configuration struct {
	// DSN is the configuration of the data source name to connect to the Amazon Redshift.
	DSN string `json:"dsn" validate:"required"`
	// Table is the configuration of the table name.
	Table string `json:"table" validate:"required"` //,lowercase,excludesall= ,lte=127"`
	// KeyColumns is the configuration of comma-separated column names to build the opencdc.Record.Key (for Source).
	KeyColumns []string `json:"keyColumns"` // validate:"omitempty,dive,lowercase,excludesall= ,lte=127"`
}

// // parseCommon parses a common configuration.
// func parseCommon(cfg map[string]string) (Configuration, error) {
// 	commonConfig := Configuration{
// 		DSN:   cfg[DSN],
// 		Table: cfg[Table],
// 	}

// 	if cfg[KeyColumns] != "" {
// 		keyColumns := strings.Split(cfg[KeyColumns], ",")
// 		for i := range keyColumns {
// 			if keyColumns[i] == "" {
// 				return Configuration{}, fmt.Errorf("invalid %q", KeyColumns)
// 			}

// 			commonConfig.KeyColumns = append(commonConfig.KeyColumns, strings.TrimSpace(keyColumns[i]))
// 		}
// 	}

// 	if err := validateStruct(commonConfig); err != nil {
// 		return Configuration{}, fmt.Errorf("validate common configuration: %w", err)
// 	}

// 	return commonConfig, nil
// }
