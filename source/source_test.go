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

package source

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio-labs/conduit-connector-redshift/common"
	config "github.com/conduitio-labs/conduit-connector-redshift/source/config"
	"github.com/conduitio-labs/conduit-connector-redshift/source/mock"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

const (
	testDSN   = "postgres://username:password@host1:5439/database1?search_path=schema1"
	testTable = "test_table"
)

func TestSource_Configure_requiredFieldsSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	s := Source{}

	err := s.Configure(context.Background(), map[string]string{
		config.ConfigDsn:            testDSN,
		config.ConfigTable:          testTable,
		config.ConfigOrderingColumn: "created_at",
	})
	is.NoErr(err)
	is.Equal(s.config, config.Config{
		Configuration: common.Configuration{
			DSN: testDSN,
		},
		Tables: func() map[string]config.TableConfig {
			tables := make(map[string]config.TableConfig)
			tables[testTable] = config.TableConfig{OrderingColumn: "created_at"}

			return tables
		}(),
		OrderingColumn: "created_at",
		Snapshot:       true,
		BatchSize:      1000,
	})
}

func TestSource_Configure_allFieldsSuccess(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	s := Source{}

	err := s.Configure(context.Background(), map[string]string{
		config.ConfigDsn:            testDSN,
		config.ConfigTable:          testTable,
		config.ConfigOrderingColumn: "created_at",
		config.ConfigSnapshot:       "false",
		config.ConfigKeyColumns:     "id,name",
		config.ConfigBatchSize:      "10000",
	})
	is.NoErr(err)
	is.Equal(s.config, config.Config{
		Configuration: common.Configuration{
			DSN: testDSN,
		},
		Tables: func() map[string]config.TableConfig {
			tables := make(map[string]config.TableConfig)
			tables[testTable] = config.TableConfig{OrderingColumn: "created_at", KeyColumns: []string{"id", "name"}}

			return tables
		}(),
		KeyColumns:     []string{"id", "name"},
		OrderingColumn: "created_at",
		Snapshot:       false,
		BatchSize:      10000,
	})
}

func TestSource_Configure_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	s := Source{}

	err := s.Configure(context.Background(), map[string]string{
		config.ConfigDsn:   testDSN,
		config.ConfigTable: testTable,
	})
	is.True(err != nil)
	is.Equal(err.Error(), "error validating configuration: error validating \"tables\": required parameter is not provided")
}

func TestSource_Read_success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	st := make(opencdc.StructuredData)
	st["key"] = "value"

	record := opencdc.Record{
		Position: opencdc.Position(`{"last_processed_element_value": 1}`),
		Metadata: nil,
		Key:      st,
		Payload:  opencdc.Change{After: st},
	}

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(record, nil)

	s := Source{
		iterator: it,
	}

	r, err := s.Read(ctx)
	is.NoErr(err)
	is.Equal(r, record)
}

func TestSource_Read_hasNextFailure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, errors.New("get data: fail"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.True(err != nil)
	is.Equal(err.Error(), "has next: get data: fail")
}

func TestSource_Read_nextFailure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)
	ctx := context.Background()

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().HasNext(ctx).Return(true, nil)
	it.EXPECT().Next(ctx).Return(opencdc.Record{}, errors.New("key is not exist"))

	s := Source{
		iterator: it,
	}

	_, err := s.Read(ctx)
	is.True(err != nil)
	is.Equal(err.Error(), "next: key is not exist")
}

func TestSource_Teardown(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop().Return(nil)

	s := Source{
		iterator: it,
	}

	err := s.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Teardown_failure(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	ctrl := gomock.NewController(t)

	it := mock.NewMockIterator(ctrl)
	it.EXPECT().Stop().Return(errors.New("some error"))

	s := Source{
		iterator: it,
	}

	err := s.Teardown(context.Background())
	is.True(err != nil)
	is.Equal(err.Error(), "stop iterator: some error")
}
