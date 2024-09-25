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

const (
	MaxConfigStringLength = 127
	MinConfigBatchSize    = 1
	MaxConfigBatchSize    = 100000
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
