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

// import (
// 	"fmt"
// 	"testing"

// 	"github.com/matryer/is"
// )

// func TestParseSource(t *testing.T) {
// 	t.Parallel()

// 	tests := []struct {
// 		name      string
// 		in        map[string]string
// 		wantValue Source
// 		wantErr   error
// 	}{
// 		{
// 			name: "success_required_values_only",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 			},
// 			wantValue: Source{
// 				Configuration: Configuration{
// 					DSN:   testValueDSN,
// 					Table: testValueTable,
// 				},
// 				OrderingColumn: "id",
// 				Snapshot:       true,
// 				BatchSize:      defaultBatchSize,
// 			},
// 		},
// 		{
// 			name: "success_batchSize",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 				BatchSize:      "100",
// 			},
// 			wantValue: Source{
// 				Configuration: Configuration{
// 					DSN:   testValueDSN,
// 					Table: testValueTable,
// 				},
// 				OrderingColumn: "id",
// 				Snapshot:       true,
// 				BatchSize:      100,
// 			},
// 		},
// 		{
// 			name: "success_snapshot_is_false",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 				Snapshot:       "false",
// 				BatchSize:      "100",
// 			},
// 			wantValue: Source{
// 				Configuration: Configuration{
// 					DSN:   testValueDSN,
// 					Table: testValueTable,
// 				},
// 				OrderingColumn: "id",
// 				Snapshot:       false,
// 				BatchSize:      100,
// 			},
// 		},
// 		{
// 			name: "success_batchSize_max",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 				BatchSize:      "100000",
// 			},
// 			wantValue: Source{
// 				Configuration: Configuration{
// 					DSN:   testValueDSN,
// 					Table: testValueTable,
// 				},
// 				OrderingColumn: "id",
// 				Snapshot:       true,
// 				BatchSize:      100000,
// 			},
// 		},
// 		{
// 			name: "success_batchSize_min",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 				BatchSize:      "1",
// 			},
// 			wantValue: Source{
// 				Configuration: Configuration{
// 					DSN:   testValueDSN,
// 					Table: testValueTable,
// 				},
// 				OrderingColumn: "id",
// 				Snapshot:       true,
// 				BatchSize:      1,
// 			},
// 		},
// 		{
// 			name: "failure_orderingColumn_is_required",
// 			in: map[string]string{
// 				DSN:   testValueDSN,
// 				Table: testValueTable,
// 			},
// 			wantErr: requiredErr(OrderingColumn),
// 		},
// 		{
// 			name: "failure_orderingColumn_has_uppercase_letter",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "ID",
// 			},
// 			wantErr: lowercaseErr(OrderingColumn),
// 		},
// 		{
// 			name: "failure_orderingColumn_has_space",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "na me",
// 			},
// 			wantErr: excludesallErr(OrderingColumn, excludeSpace),
// 		},
// 		{
// 			name: "failure_batchSize_is_invalid",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 				BatchSize:      "a",
// 			},
// 			wantErr: fmt.Errorf(`parse %q: strconv.Atoi: parsing "a": invalid syntax`, BatchSize),
// 		},
// 		{
// 			name: "failure_batchSize_is_too_big",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 				BatchSize:      "100001",
// 			},
// 			wantErr: lteErr(BatchSize, "100000"),
// 		},
// 		{
// 			name: "failure_batchSize_is_zero",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 				BatchSize:      "0",
// 			},
// 			wantErr: gteErr(BatchSize, "1"),
// 		},
// 		{
// 			name: "failure_batchSize_is_negative",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 				BatchSize:      "-1",
// 			},
// 			wantErr: gteErr(BatchSize, "1"),
// 		},
// 		{
// 			name: "failure_snapshot_is_invalid",
// 			in: map[string]string{
// 				DSN:            testValueDSN,
// 				Table:          testValueTable,
// 				OrderingColumn: "id",
// 				Snapshot:       "a",
// 			},
// 			wantErr: fmt.Errorf(`parse %q: strconv.ParseBool: parsing "a": invalid syntax`, Snapshot),
// 		},
// 	}

// 	for _, tt := range tests {
// 		tt := tt

// 		t.Run(tt.name, func(t *testing.T) {
// 			t.Parallel()
// 			is := is.New(t)

// 			got, err := ParseSource(tt.in)
// 			if tt.wantErr == nil {
// 				is.NoErr(err)
// 				is.Equal(got, tt.wantValue)
// 			} else {
// 				is.True(err != nil)
// 				is.Equal(err.Error(), tt.wantErr.Error())
// 			}
// 		})
// 	}
// }
