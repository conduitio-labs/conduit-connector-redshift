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

package config

import (
	"fmt"
	"testing"

	"github.com/conduitio-labs/conduit-connector-redshift/common"
	"github.com/matryer/is"
)

const (
	testValueDSN   = "postgres://username:password@endpoint:5439/database"
	testValueTable = "test_table"
	testLongString = `this_is_a_very_long_string_which_exceeds_max_config_string_limit_
						abcdefghijklmnopqrstuvwxyz_zyxwvutsrqponmlkjihgfedcba_xxxxxxxx`
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      Config
		wantErr error
	}{
		{
			name: "success_single_table_config",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"test_table": {
						KeyColumns:     []string{"id"},
						OrderingColumn: "id",
					},
				},
				BatchSize: 1000,
				Snapshot:  true,
			},
		},
		{
			name: "success_multiple_tables_config",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"users": {
						KeyColumns:     []string{"id", "email"},
						OrderingColumn: "updated_at",
					},
					"orders": {
						KeyColumns:     []string{"order_id"},
						OrderingColumn: "created_at",
					},
				},
				BatchSize: 1000,
				Snapshot:  true,
			},
		},
		{
			name: "failure_no_tables_configured",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:    map[string]TableConfig{},
				Snapshot:  true,
				BatchSize: 1000,
			},
			wantErr: fmt.Errorf(`error validating "tables": required parameter is not provided`),
		},
		{
			name: "failure_both_table_and_tables_provided",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Table: "legacy_table",
				Tables: map[string]TableConfig{
					"new_table": {
						KeyColumns:     []string{"id"},
						OrderingColumn: "updated_at",
					},
				},
				OrderingColumn: "id",
				BatchSize:      1000,
			},
			wantErr: fmt.Errorf(`error validating "tables": cannot provide both "table" and "tables", use "tables" only`),
		},
		{
			name: "failure_table_name_has_space",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"test table": {
						KeyColumns:     []string{"id"},
						OrderingColumn: "updated_at",
					},
				},
				BatchSize: 1000,
			},
			wantErr: common.NewExcludesSpacesError(ConfigTable),
		},
		{
			name: "failure_table_name_too_long",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					testLongString: {
						KeyColumns:     []string{"id"},
						OrderingColumn: "updated_at",
					},
				},
				BatchSize: 1000,
			},
			wantErr: common.NewLessThanError(ConfigTable, common.MaxConfigStringLength),
		},
		{
			name: "failure_ordering_column_missing",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"test_table": {
						KeyColumns: []string{"id"},
					},
				},
				BatchSize: 1000,
			},
			wantErr: fmt.Errorf(`error validating %s: required parameter is not provided`, ConfigOrderingColumn),
		},
		{
			name: "failure_ordering_column_has_uppercase",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"test_table": {
						KeyColumns:     []string{"id"},
						OrderingColumn: "Updated_At",
					},
				},
				BatchSize: 1000,
			},
			wantErr: common.NewLowercaseError(ConfigOrderingColumn),
		},
		{
			name: "failure_key_column_has_space",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"test_table": {
						KeyColumns:     []string{"user id"},
						OrderingColumn: "updated_at",
					},
				},
				BatchSize: 1000,
			},
			wantErr: common.NewExcludesSpacesError(ConfigKeyColumns),
		},
		{
			name: "failure_key_column_has_uppercase",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"test_table": {
						KeyColumns:     []string{"ID"},
						OrderingColumn: "updated_at",
					},
				},
				BatchSize: 1000,
			},
			wantErr: common.NewLowercaseError(ConfigKeyColumns),
		},
		{
			name: "failure_batch_size_too_small",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"test_table": {
						KeyColumns:     []string{"id"},
						OrderingColumn: "updated_at",
					},
				},
				BatchSize: 0,
			},
			wantErr: common.NewGreaterThanError(ConfigBatchSize, common.MinConfigBatchSize),
		},
		{
			name: "failure_batch_size_too_large",
			in: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"test_table": {
						KeyColumns:     []string{"id"},
						OrderingColumn: "updated_at",
					},
				},
				BatchSize: 100001,
			},
			wantErr: common.NewLessThanError(ConfigBatchSize, common.MaxConfigBatchSize),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			err := tt.in.Validate()
			if tt.wantErr == nil {
				is.NoErr(err)
			} else {
				is.True(err != nil)
				is.Equal(err.Error(), tt.wantErr.Error())
			}
		})
	}
}

func TestConfigInit(t *testing.T) {
	t.Parallel()
	is := is.New(t)

	tests := []struct {
		name     string
		input    Config
		expected Config
	}{
		{
			name: "convert_legacy_config",
			input: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Table:          "Legacy_Table",
				KeyColumns:     []string{"id", "email"},
				OrderingColumn: "updated_at",
				BatchSize:      1000,
			},
			expected: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"legacy_table": {
						KeyColumns:     []string{"id", "email"},
						OrderingColumn: "updated_at",
					},
				},
				BatchSize: 1000,
				Table:     "",
			},
		},
		{
			name: "convert_table_names_to_lowercase",
			input: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"Users": {
						KeyColumns:     []string{"id"},
						OrderingColumn: "updated_at",
					},
					"Orders": {
						KeyColumns:     []string{"order_id"},
						OrderingColumn: "created_at",
					},
				},
				BatchSize: 1000,
			},
			expected: Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables: map[string]TableConfig{
					"users": {
						KeyColumns:     []string{"id"},
						OrderingColumn: "updated_at",
					},
					"orders": {
						KeyColumns:     []string{"order_id"},
						OrderingColumn: "created_at",
					},
				},
				BatchSize: 1000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.input.Init()

			is.Equal(len(result.Tables), len(tt.expected.Tables))
			for tableName, tableConfig := range result.Tables {
				expectedConfig, exists := tt.expected.Tables[tableName]
				is.True(exists)
				is.Equal(tableConfig, expectedConfig)
			}

			is.Equal(result.Table, tt.expected.Table)
			is.Equal(result.BatchSize, tt.expected.BatchSize)
			is.Equal(result.DSN, tt.expected.DSN)
		})
	}
}
