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
		in      *Config
		wantErr error
	}{
		{
			name: "success_keyColumns_has_one_key",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id"},
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: nil,
		},
		{
			name: "success_keyColumns_has_two_keys",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id", "name"},
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: nil,
		},
		{
			name: "success_keyColumns_ends_with_space",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id", "name "},
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: common.NewExcludesSpacesError(ConfigKeyColumns),
		},
		{
			name: "success_keyColumns_starts_with_space",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id", "name "},
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: common.NewExcludesSpacesError(ConfigKeyColumns),
		},
		{
			name: "success_keyColumns_has_two_spaces",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id", "  name"},
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: common.NewExcludesSpacesError(ConfigKeyColumns),
		},
		{
			name: "failure_table_has_space",
			in: &Config{
				Configuration: common.Configuration{
					DSN:   testValueDSN,
					Table: "test table",
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: common.NewExcludesSpacesError(ConfigTable),
		},
		{
			name: "failure_table_has_uppercase_letter",
			in: &Config{
				Configuration: common.Configuration{
					DSN:   testValueDSN,
					Table: "Test_table",
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: common.NewLowercaseError(ConfigTable),
		},
		{
			name: "failure_keyColumns_has_uppercase_letter",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"ID"},
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: common.NewLowercaseError(ConfigKeyColumns),
		},
		{
			name: "failure_keyColumns_exceeds_max_length",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{testLongString},
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: common.NewLessThanError(ConfigKeyColumns, common.MaxConfigStringLength),
		},
		{
			name: "failure_table_exceeds_max_length",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testLongString,
					KeyColumns: []string{"id"},
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
			wantErr: common.NewLessThanError(ConfigTable, common.MaxConfigStringLength),
		},
		{
			name: "failure_ordering_column_has_uppercase_letter",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id"},
				},
				OrderingColumn: "ID",
				BatchSize:      1,
			},
			wantErr: common.NewLowercaseError(ConfigOrderingColumn),
		},
		{
			name: "failure_ordering_column_has_space",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id"},
				},
				OrderingColumn: " id",
				BatchSize:      1,
			},
			wantErr: common.NewExcludesSpacesError(ConfigOrderingColumn),
		},
		{
			name: "failure_ordering_column_exceeds_max_length",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id"},
				},
				OrderingColumn: testLongString,
				BatchSize:      1,
			},
			wantErr: common.NewLessThanError(ConfigOrderingColumn, common.MaxConfigStringLength),
		},
		{
			name: "failure_batch_size_less_than_min_value",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id"},
				},
				OrderingColumn: "id",
				BatchSize:      0,
			},
			wantErr: common.NewGreaterThanError(ConfigBatchSize, common.MinConfigBatchSize),
		},
		{
			name: "failure_batch_size_greater_than_max_value",
			in: &Config{
				Configuration: common.Configuration{
					DSN:        testValueDSN,
					Table:      testValueTable,
					KeyColumns: []string{"id"},
				},
				OrderingColumn: "id",
				BatchSize:      100001,
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
