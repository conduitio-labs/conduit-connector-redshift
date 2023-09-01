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
	"github.com/matryer/is"
	"testing"

	"go.uber.org/multierr"
)

const (
	testValueDSN   = "postgres://username:password@endpoint:5439/database"
	testValueTable = "test_table"
)

func TestParseCommon(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        map[string]string
		wantValue Configuration
		wantErr   error
	}{
		{
			name: "success_required_values_only",
			in: map[string]string{
				DSN:   testValueDSN,
				Table: testValueTable,
			},
			wantValue: Configuration{
				DSN:   testValueDSN,
				Table: testValueTable,
			},
		},
		{
			name: "success_keyColumns_has_one_key",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "id",
			},
			wantValue: Configuration{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: []string{"id"},
			},
		},
		{
			name: "success_keyColumns_has_two_keys",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "id,name",
			},
			wantValue: Configuration{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_has_space_between_keys",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "id, name",
			},
			wantValue: Configuration{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_ends_with_space",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "id,name ",
			},
			wantValue: Configuration{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_starts_with_space",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: " id,name",
			},
			wantValue: Configuration{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_has_space_between_keys_before_comma",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "id ,name",
			},
			wantValue: Configuration{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "success_keyColumns_has_two_spaces",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "id,  name",
			},
			wantValue: Configuration{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: []string{"id", "name"},
			},
		},
		{
			name: "failure_keyColumns_ends_with_comma",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "id,name,",
			},
			wantErr: fmt.Errorf("invalid %q", KeyColumns),
		},
		{
			name: "failure_keyColumns_starts_with_comma",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: ",id,name",
			},
			wantErr: fmt.Errorf("invalid %q", KeyColumns),
		},
		{
			name: "failure_keyColumns_invalid",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: ",",
			},
			wantErr: fmt.Errorf("invalid %q", KeyColumns),
		},
		{
			name: "failure_table_has_space",
			in: map[string]string{
				DSN:   testValueDSN,
				Table: "test table",
			},
			wantErr: fmt.Errorf("validate common configuration: %w", excludesallErr(Table, excludeSpace)),
		},
		{
			name: "failure_table_has_uppercase_letter",
			in: map[string]string{
				DSN:   testValueDSN,
				Table: "Test_table",
			},
			wantErr: fmt.Errorf("validate common configuration: %w", lowercaseErr(Table)),
		},
		{
			name: "failure_keyColumns_has_uppercase_letter",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "ID",
			},
			wantErr: fmt.Errorf("validate common configuration: %w", lowercaseErr(KeyColumns)),
		},
		{
			name: "failure_keyColumn_has_space",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "na me",
			},
			wantErr: fmt.Errorf("validate common configuration: %w", excludesallErr(KeyColumns, excludeSpace)),
		},
		{
			name: "failure_dsn_is_required",
			in: map[string]string{
				Table: testValueTable,
			},
			wantErr: fmt.Errorf("validate common configuration: %w", requiredErr(DSN)),
		},
		{
			name: "failure_table_is_required",
			in: map[string]string{
				DSN: testValueDSN,
			},
			wantErr: fmt.Errorf("validate common configuration: %w", requiredErr(Table)),
		},
		{
			name: "failure_dsn_and_table_are_required",
			in:   map[string]string{},
			wantErr: fmt.Errorf("validate common configuration: %w",
				multierr.Combine(requiredErr(DSN), requiredErr(Table))),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			got, err := parseCommon(tt.in)
			if tt.wantErr == nil {
				is.NoErr(err)
				is.Equal(got, tt.wantValue)
			} else {
				is.True(err != nil)
				is.Equal(err.Error(), tt.wantErr.Error())
			}
		})
	}
}
