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

package source

import (
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestNewPosition(t *testing.T) {
	t.Parallel()
	is := is.New(t)
	pos := NewPosition()
	is.True(pos != nil)
	is.Equal(pos.TablePositions.Len(), 0)
}

func TestParseSDKPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      opencdc.Position
		wantPos *Position
		wantErr error
	}{
		{
			name:    "success_position_is_nil",
			in:      nil,
			wantPos: NewPosition(),
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
			wantPos: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  float64(10),
					LatestSnapshotValue: float64(30),
				})
				return pos //nolint:nlreturn // compact code style
			}(),
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
			wantPos: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  "abc",
					LatestSnapshotValue: "def",
				})
				pos.set("table2", TablePosition{
					LastProcessedValue:  float64(20),
					LatestSnapshotValue: float64(40),
				})
				return pos //nolint:nlreturn // compact code style
			}(),
		},
		{
			name:    "success_empty_table_positions",
			in:      opencdc.Position(`{"tablePositions": {}}`),
			wantPos: NewPosition(),
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
			wantPos: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  float64(10),
					LatestSnapshotValue: float64(30),
				})
				return pos //nolint:nlreturn // compact code style
			}(),
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
			wantPos: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  "abc",
					LatestSnapshotValue: "def",
				})
				return pos //nolint:nlreturn // compact code style
			}(),
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
			wantPos: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue: float64(10),
				})
				return pos //nolint:nlreturn // compact code style
			}(),
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
			wantPos: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LatestSnapshotValue: float64(30),
				})
				return pos //nolint:nlreturn // compact code style
			}(),
		},
		{
			name:    "failure_invalid_json",
			in:      opencdc.Position("invalid"),
			wantErr: errors.New("unmarshal opencdc.Position into Position: unmarshal position JSON: invalid character 'i' looking for beginning of value"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			got, err := ParseSDKPosition(tt.in)
			if tt.wantErr == nil {
				is.NoErr(err)
				is.Equal(got, tt.wantPos)
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
		in   *Position
		want opencdc.Position
	}{
		{
			name: "success_empty_table_positions",
			in:   NewPosition(),
			want: opencdc.Position(`{"tablePositions":{}}`),
		},
		{
			name: "success_single_table",
			in: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  10,
					LatestSnapshotValue: 30,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":10,"latestSnapshotValue":30}}}`),
		},
		{
			name: "success_multiple_tables",
			in: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  "abc",
					LatestSnapshotValue: "def",
				})
				pos.set("table2", TablePosition{
					LastProcessedValue:  20,
					LatestSnapshotValue: 40,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":"abc","latestSnapshotValue":"def"},"table2":{"lastProcessedValue":20,"latestSnapshotValue":40}}}`),
		},
		{
			name: "success_single_table_integer_fields",
			in: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  10,
					LatestSnapshotValue: 30,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":10,"latestSnapshotValue":30}}}`),
		},
		{
			name: "success_single_table_string_fields",
			in: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  "abc",
					LatestSnapshotValue: "def",
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":"abc","latestSnapshotValue":"def"}}}`),
		},
		{
			name: "success_single_table_lastProcessedValue_only",
			in: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue: float64(10),
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			want: opencdc.Position(`{"tablePositions":{"table1":{"lastProcessedValue":10,"latestSnapshotValue":null}}}`),
		},
		{
			name: "success_single_table_latestSnapshotValue_only",
			in: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LatestSnapshotValue: float64(30),
				})
				return pos //nolint:nlreturn // compact code style
			}(),
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

func TestPosition_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		initial   *Position
		table     string
		updatePos TablePosition
		want      *Position
	}{
		{
			name: "update_existing_table",
			initial: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  1,
					LatestSnapshotValue: 2,
				})
				pos.set("table2", TablePosition{
					LastProcessedValue:  3,
					LatestSnapshotValue: 4,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			table:     "table1",
			updatePos: TablePosition{LastProcessedValue: 10, LatestSnapshotValue: 20},
			want: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  10,
					LatestSnapshotValue: 20,
				})
				pos.set("table2", TablePosition{
					LastProcessedValue:  3,
					LatestSnapshotValue: 4,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
		},
		{
			name: "update_new_table",
			initial: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  1,
					LatestSnapshotValue: 2,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			table:     "table2",
			updatePos: TablePosition{LastProcessedValue: 3, LatestSnapshotValue: 4},
			want: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  1,
					LatestSnapshotValue: 2,
				})
				pos.set("table2", TablePosition{
					LastProcessedValue:  3,
					LatestSnapshotValue: 4,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
		},
		{
			name: "update_with_nil_values",
			initial: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  1,
					LatestSnapshotValue: 2,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			table:     "table1",
			updatePos: TablePosition{LastProcessedValue: nil, LatestSnapshotValue: nil},
			want: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{})
				return pos //nolint:nlreturn // compact code style
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			tt.initial.set(tt.table, tt.updatePos)
			is.Equal(tt.initial, tt.want)
		})
	}
}

func TestPosition_Get(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initial    *Position
		table      string
		wantPos    TablePosition
		wantExists bool
	}{
		{
			name: "get_existing_table",
			initial: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  1,
					LatestSnapshotValue: 2,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			table:      "table1",
			wantPos:    TablePosition{LastProcessedValue: 1, LatestSnapshotValue: 2},
			wantExists: true,
		},
		{
			name: "get_non_existing_table",
			initial: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{
					LastProcessedValue:  1,
					LatestSnapshotValue: 2,
				})
				return pos //nolint:nlreturn // compact code style
			}(),
			table:      "table2",
			wantPos:    TablePosition{},
			wantExists: false,
		},
		{
			name:       "get_from_empty_positions",
			initial:    NewPosition(),
			table:      "table1",
			wantPos:    TablePosition{},
			wantExists: false,
		},
		{
			name: "get_table_with_nil_values",
			initial: func() *Position {
				pos := NewPosition()
				pos.set("table1", TablePosition{})
				return pos //nolint:nlreturn // compact code style
			}(),
			table:      "table1",
			wantPos:    TablePosition{LastProcessedValue: nil, LatestSnapshotValue: nil},
			wantExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			gotPos, exists := tt.initial.get(tt.table)
			is.Equal(exists, tt.wantExists)
			is.Equal(gotPos, tt.wantPos)
		})
	}
}
