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

package iterator

import (
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestParseSDKPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      opencdc.Position
		wantPos Position
		wantErr error
	}{
		{
			name:    "success_position_is_nil",
			in:      nil,
			wantPos: Position{},
		},
		{
			name: "success_single_table",
			in: opencdc.Position(`{
				"tablePositions": {
					"table1": {
						"lastProcessedValue": 10,
						"latestSnapshotValue": 30
					}
				}
			}`),
			wantPos: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue:  float64(10),
						LatestSnapshotValue: float64(30),
					},
				},
			},
		},
		{
			name: "success_multiple_tables",
			in: opencdc.Position(`{
				"tablePositions": {
					"table1": {
						"lastProcessedValue": "abc",
						"latestSnapshotValue": "def"
					},
					"table2": {
						"lastProcessedValue": 20,
						"latestSnapshotValue": 40
					}
				}
			}`),
			wantPos: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue:  "abc",
						LatestSnapshotValue: "def",
					},
					"table2": {
						LastProcessedValue:  float64(20),
						LatestSnapshotValue: float64(40),
					},
				},
			},
		},
		{
			name: "success_empty_table_positions",
			in:   opencdc.Position(`{"tablePositions": {}}`),
			wantPos: Position{
				TablePositions: map[string]TablePosition{},
			},
		},
		{
			name: "success_single_table_float64_fields",
			in: opencdc.Position(`{
				"tablePositions": {
					"table1": {
						"lastProcessedValue": 10,
						"latestSnapshotValue": 30
					}
				}
			}`),
			wantPos: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue:  float64(10),
						LatestSnapshotValue: float64(30),
					},
				},
			},
		},
		{
			name: "success_single_table_string_fields",
			in: opencdc.Position(`{
				"tablePositions": {
					"table1": {
						"lastProcessedValue": "abc",
						"latestSnapshotValue": "def"
					}
				}
			}`),
			wantPos: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue:  "abc",
						LatestSnapshotValue: "def",
					},
				},
			},
		},
		{
			name: "success_single_table_lastProcessedValue_only",
			in: opencdc.Position(`{
				"tablePositions": {
					"table1": {
						"lastProcessedValue": 10
					}
				}
			}`),
			wantPos: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue: float64(10),
					},
				},
			},
		},
		{
			name: "success_single_table_latestSnapshotValue_only",
			in: opencdc.Position(`{
				"tablePositions": {
					"table1": {
						"latestSnapshotValue": 30
					}
				}
			}`),
			wantPos: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LatestSnapshotValue: float64(30),
					},
				},
			},
		},
		{
			name:    "failure_invalid_json",
			in:      opencdc.Position("invalid"),
			wantErr: errors.New("unmarshal opencdc.Position into Position: invalid character 'i' looking for beginning of value"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			got, err := ParseSDKPosition(tt.in)
			if tt.wantErr == nil {
				is.NoErr(err)
				is.Equal(*got, tt.wantPos)
			} else {
				is.True(err != nil)
				is.Equal(err.Error(), tt.wantErr.Error())
			}
		})
	}
}

func TestMarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   Position
		want opencdc.Position
	}{
		{
			name: "success_empty_table_positions",
			in:   Position{TablePositions: map[string]TablePosition{}},
			want: opencdc.Position(`{"tablePositions":{}}`),
		},
		{
			name: "success_single_table",
			in: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue:  10,
						LatestSnapshotValue: 30,
					},
				},
			},
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":10,"latestSnapshotValue":30}}}`),
		},
		{
			name: "success_multiple_tables",
			in: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue:  "abc",
						LatestSnapshotValue: "def",
					},
					"table2": {
						LastProcessedValue:  20,
						LatestSnapshotValue: 40,
					},
				},
			},
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":"abc","latestSnapshotValue":"def"},"table2":{"lastProcessedValue":20,"latestSnapshotValue":40}}}`),
		},
		{
			name: "success_single_table_integer_fields",
			in: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue:  10,
						LatestSnapshotValue: 30,
					},
				},
			},
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":10,"latestSnapshotValue":30}}}`),
		},
		{
			name: "success_single_table_string_fields",
			in: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue:  "abc",
						LatestSnapshotValue: "def",
					},
				},
			},
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":"abc","latestSnapshotValue":"def"}}}`),
		},
		{
			name: "success_single_table_lastProcessedValue_only",
			in: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LastProcessedValue: float64(10),
					},
				},
			},
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":10,"latestSnapshotValue":null}}}`),
		},
		{
			name: "success_single_table_latestSnapshotValue_only",
			in: Position{
				TablePositions: map[string]TablePosition{
					"table1": {
						LatestSnapshotValue: float64(30),
					},
				},
			},
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":null,"latestSnapshotValue":30}}}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			got, err := tt.in.marshal()
			is.NoErr(err)
			is.Equal(got, tt.want)
		})
	}
}
