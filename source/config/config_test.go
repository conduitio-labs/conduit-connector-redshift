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
	"testing"

	"github.com/conduitio-labs/conduit-connector-redshift/common"
	"github.com/matryer/is"
)

const (
	testValueDSN   = "postgres://username:password@endpoint:5439/database"
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
			name: "success",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:          []string{"test_table1", "test_table2"},
				OrderingColumns: []string{"id1", "id2"},
				Snapshot:        true,
				BatchSize:       1000,
			},
			wantErr: nil,
		},
		{
			name: "failure_no_tables",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				OrderingColumns: []string{"id"},
				BatchSize:       1000,
			},
			wantErr: common.NewNoTablesOrColumnsError(),
		},
		{
			name: "failure_no_ordering_columns",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:    []string{"test_table"},
				BatchSize: 1000,
			},
			wantErr: common.NewNoTablesOrColumnsError(),
		},
		{
			name: "failure_mismatched_tables_and_columns",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:          []string{"test_table1", "test_table2"},
				OrderingColumns: []string{"id"},
				BatchSize:       1000,
			},
			wantErr: common.NewMismatchedTablesAndColumnsError(2, 1),
		},
		{
			name: "failure_table_has_uppercase_letter",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:          []string{"Test_table"},
				OrderingColumns: []string{"id"},
				BatchSize:       1000,
			},
			wantErr: common.NewLowercaseError("table[0]"),
		},
		{
			name: "failure_table_has_space",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:          []string{"test table"},
				OrderingColumns: []string{"id"},
				BatchSize:       1000,
			},
			wantErr: common.NewExcludesSpacesError("table[0]"),
		},
		{
			name: "failure_table_exceeds_max_length",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:          []string{testLongString},
				OrderingColumns: []string{"id"},
				BatchSize:       1000,
			},
			wantErr: common.NewLessThanError("table[0]", common.MaxConfigStringLength),
		},
		{
			name: "failure_ordering_column_has_uppercase_letter",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:          []string{"test_table"},
				OrderingColumns: []string{"ID"},
				BatchSize:       1000,
			},
			wantErr: common.NewLowercaseError("orderingColumn[0]"),
		},
		{
			name: "failure_ordering_column_has_space",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:          []string{"test_table"},
				OrderingColumns: []string{" id"},
				BatchSize:       1000,
			},
			wantErr: common.NewExcludesSpacesError("orderingColumn[0]"),
		},
		{
			name: "failure_ordering_column_exceeds_max_length",
			in: &Config{
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
				Tables:          []string{"test_table"},
				OrderingColumns: []string{testLongString},
				BatchSize:       1000,
			},
			wantErr: common.NewLessThanError("orderingColumn[0]", common.MaxConfigStringLength),
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

func TestGetTableOrderingMap(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	config := &Config{
		Tables:          []string{"table1", "table2"},
		OrderingColumns: []string{"id1", "id2"},
	}

	expectedMap := map[string]string{
		"table1": "id1",
		"table2": "id2",
	}

	result := config.GetTableOrderingMap()

	is.Equal(result, expectedMap)
}
