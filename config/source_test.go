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
)

func TestParseSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want Source
		err  error
	}{
		{
			name: "success_required_values_only",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "id",
			},
			want: Source{
				Configuration: Configuration{
					DSN:   testValueDSN,
					Table: testValueTable,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "success_batchSize",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "id",
				BatchSize:      "100",
			},
			want: Source{
				Configuration: Configuration{
					DSN:   testValueDSN,
					Table: testValueTable,
				},
				OrderingColumn: "id",
				BatchSize:      100,
			},
		},
		{
			name: "success_batchSize_max",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "id",
				BatchSize:      "100000",
			},
			want: Source{
				Configuration: Configuration{
					DSN:   testValueDSN,
					Table: testValueTable,
				},
				OrderingColumn: "id",
				BatchSize:      100000,
			},
		},
		{
			name: "success_batchSize_min",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "id",
				BatchSize:      "1",
			},
			want: Source{
				Configuration: Configuration{
					DSN:   testValueDSN,
					Table: testValueTable,
				},
				OrderingColumn: "id",
				BatchSize:      1,
			},
		},
		{
			name: "failure_orderingColumn_is_required",
			in: map[string]string{
				DSN:   testValueDSN,
				Table: testValueTable,
			},
			err: requiredErr(OrderingColumn),
		},
		{
			name: "failure_orderingColumn_has_uppercase_letter",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "ID",
			},
			err: lowercaseErr(OrderingColumn),
		},
		{
			name: "failure_orderingColumn_has_space",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "na me",
			},
			err: excludesallErr(OrderingColumn, excludeSpace),
		},
		{
			name: "failure_batchSize_is_invalid",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "id",
				BatchSize:      "a",
			},
			err: fmt.Errorf(`parse %q: strconv.Atoi: parsing "a": invalid syntax`, BatchSize),
		},
		{
			name: "failure_batchSize_is_too_big",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "id",
				BatchSize:      "100001",
			},
			err: lteErr(BatchSize, "100000"),
		},
		{
			name: "failure_batchSize_is_zero",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "id",
				BatchSize:      "0",
			},
			err: gteErr(BatchSize, "1"),
		},
		{
			name: "failure_batchSize_is_negative",
			in: map[string]string{
				DSN:            testValueDSN,
				Table:          testValueTable,
				OrderingColumn: "id",
				BatchSize:      "-1",
			},
			err: gteErr(BatchSize, "1"),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseSource(tt.in)
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