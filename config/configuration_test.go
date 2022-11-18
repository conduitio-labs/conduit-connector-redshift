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
	testDSN   = "postgres://username:password@endpoint:5439/database"
	testTable = "test_table"
)

func TestParseGeneral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want Configuration
		err  error
	}{
		{
			name: "success_required_values",
			in: map[string]string{
				DSN:   testDSN,
				Table: testTable,
			},
			want: Configuration{
				DSN:   testDSN,
				Table: testTable,
			},
		},
		{
			name: "failure_required_url",
			in: map[string]string{
				Table: testTable,
			},
			err: fmt.Errorf("validate general configuration: %w", requiredErr("dsn")),
		},
		{
			name: "failure_required_table",
			in: map[string]string{
				DSN: testDSN,
			},
			err: fmt.Errorf("validate general configuration: %w", requiredErr("table")),
		},
		{
			name: "failure_required_url_and_table",
			in:   map[string]string{},
			err: fmt.Errorf("validate general configuration: %w",
				multierr.Combine(requiredErr("dsn"), requiredErr("table"))),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseConfiguration(tt.in)
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
