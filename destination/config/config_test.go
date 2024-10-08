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
			name: "failure_table_has_space",
			in: &Config{
				Table: "test_table ",
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
			},
			wantErr: common.NewExcludesSpacesError(ConfigTable),
		},
		{
			name: "failure_table_has_uppercase_letter",
			in: &Config{
				Table: "Test_table",
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
			},
			wantErr: common.NewLowercaseError(ConfigTable),
		},
		{
			name: "failure_table_exceeds_max_length",
			in: &Config{
				Table: testLongString,
				Configuration: common.Configuration{
					DSN: testValueDSN,
				},
			},
			wantErr: common.NewLessThanError(ConfigTable, common.MaxConfigStringLength),
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
