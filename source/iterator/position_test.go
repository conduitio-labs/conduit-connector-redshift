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

	"github.com/matryer/is"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestParseSDKPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      sdk.Position
		wantPos Position
		wantErr error
	}{
		{
			name:    "success_position_is_nil",
			in:      nil,
			wantPos: Position{},
		},
		{
			name: "success_float64_fields",
			in: sdk.Position(`{
			   "lastProcessedValue":10,
			   "latestSnapshotValue":30
			}`),
			wantPos: Position{
				LastProcessedValue:  float64(10),
				LatestSnapshotValue: float64(30),
			},
		},
		{
			name: "success_string_fields",
			in: sdk.Position(`{
			   "lastProcessedValue":"abc",
			   "latestSnapshotValue":"def"
			}`),
			wantPos: Position{
				LastProcessedValue:  "abc",
				LatestSnapshotValue: "def",
			},
		},
		{
			name: "success_lastProcessedValue_only",
			in: sdk.Position(`{
			   "lastProcessedValue":10
			}`),
			wantPos: Position{
				LastProcessedValue: float64(10),
			},
		},
		{
			name: "success_latestSnapshotValue_only",
			in: sdk.Position(`{
			   "latestSnapshotValue":30
			}`),
			wantPos: Position{
				LatestSnapshotValue: float64(30),
			},
		},
		{
			name: "failure_required_dsn_and_table",
			in:   sdk.Position("invalid"),
			wantErr: errors.New("unmarshal sdk.Position into Position: " +
				"invalid character 'i' looking for beginning of value"),
		},
	}

	for _, tt := range tests {
		tt := tt

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
		want sdk.Position
	}{
		{
			name: "success_position_is_nil",
			in:   Position{},
			want: sdk.Position(`{"lastProcessedValue":null,"latestSnapshotValue":null}`),
		},
		{
			name: "success_integer_fields",
			in: Position{
				LastProcessedValue:  10,
				LatestSnapshotValue: 30,
			},
			want: sdk.Position(`{"lastProcessedValue":10,"latestSnapshotValue":30}`),
		},
		{
			name: "success_string_fields",
			in: Position{
				LastProcessedValue:  "abc",
				LatestSnapshotValue: "def",
			},
			want: sdk.Position(`{"lastProcessedValue":"abc","latestSnapshotValue":"def"}`),
		},
		{
			name: "success_lastProcessedValue_only",
			in: Position{
				LastProcessedValue: float64(10),
			},
			want: sdk.Position(`{"lastProcessedValue":10,"latestSnapshotValue":null}`),
		},
		{
			name: "success_latestSnapshotValue_only",
			in: Position{
				LatestSnapshotValue: float64(30),
			},
			want: sdk.Position(`{"lastProcessedValue":null,"latestSnapshotValue":30}`),
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			got, err := tt.in.marshal()
			is.NoErr(err)
			is.Equal(got, tt.want)
		})
	}
}
