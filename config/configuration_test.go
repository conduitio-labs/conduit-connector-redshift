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
	"reflect"
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
		name string
		in   map[string]string
		want Configuration
		err  error
	}{
		{
			name: "success_required_values_only",
			in: map[string]string{
				DSN:   testValueDSN,
				Table: testValueTable,
			},
			want: Configuration{
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
			want: Configuration{
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
			want: Configuration{
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
			want: Configuration{
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
			want: Configuration{
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
			want: Configuration{
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
			want: Configuration{
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
			want: Configuration{
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
			err: fmt.Errorf("invalid %q", KeyColumns),
		},
		{
			name: "failure_keyColumns_starts_with_comma",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: ",id,name",
			},
			err: fmt.Errorf("invalid %q", KeyColumns),
		},
		{
			name: "failure_keyColumns_invalid",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: ",",
			},
			err: fmt.Errorf("invalid %q", KeyColumns),
		},
		{
			name: "failure_table_has_space",
			in: map[string]string{
				DSN:   testValueDSN,
				Table: "test table",
			},
			err: fmt.Errorf("validate common configuration: %w", excludesallErr(Table, excludeSpace)),
		},
		{
			name: "failure_table_has_uppercase_letter",
			in: map[string]string{
				DSN:   testValueDSN,
				Table: "Test_table",
			},
			err: fmt.Errorf("validate common configuration: %w", lowercaseErr(Table)),
		},
		{
			name: "failure_keyColumns_has_uppercase_letter",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "ID",
			},
			err: fmt.Errorf("validate common configuration: %w", lowercaseErr(KeyColumns)),
		},
		{
			name: "failure_keyColumn_has_space",
			in: map[string]string{
				DSN:        testValueDSN,
				Table:      testValueTable,
				KeyColumns: "na me",
			},
			err: fmt.Errorf("validate common configuration: %w", excludesallErr(KeyColumns, excludeSpace)),
		},
		{
			name: "failure_dsn_is_required",
			in: map[string]string{
				Table: testValueTable,
			},
			err: fmt.Errorf("validate common configuration: %w", requiredErr(DSN)),
		},
		{
			name: "failure_table_is_required",
			in: map[string]string{
				DSN: testValueDSN,
			},
			err: fmt.Errorf("validate common configuration: %w", requiredErr(Table)),
		},
		{
			name: "failure_dsn_and_table_are_required",
			in:   map[string]string{},
			err: fmt.Errorf("validate common configuration: %w",
				multierr.Combine(requiredErr(DSN), requiredErr(Table))),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseCommon(tt.in)
			if err != nil {
				if tt.err == nil {
					t.Errorf("unexpected error: %s", err.Error())

					return
				}

				if err.Error() != tt.err.Error() {
					t.Errorf("unexpected error, got: %s, want: %s", err.Error(), tt.err.Error())

					return
				}

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got: %v, want: %v", got, tt.want)
			}
		})
	}
}
