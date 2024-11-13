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
	"sync"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-redshift/common"
	config "github.com/conduitio-labs/conduit-connector-redshift/source/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
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
		config.ConfigPollingPeriod:  "5s",
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
		PollingPeriod:  5 * time.Second,
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
		config.ConfigPollingPeriod:  "5s",
	})
	is.NoErr(err)
	is.Equal(s.config, config.Config{
		Configuration: common.Configuration{
			DSN: testDSN,
		},
		Tables: func() map[string]config.TableConfig {
			return map[string]config.TableConfig{
				testTable: {OrderingColumn: "created_at", KeyColumns: []string{"id", "name"}},
			}
		}(),
		KeyColumns:     []string{"id", "name"},
		OrderingColumn: "created_at",
		Snapshot:       false,
		BatchSize:      10000,
		PollingPeriod:  5 * time.Second,
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

func TestSource_Read_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	st := opencdc.StructuredData{
		"key": "value",
	}
	expectedRecord := opencdc.Record{
		Position: opencdc.Position(`{"last_processed_element_value": 1}`),
		Metadata: nil,
		Key:      st,
		Payload:  opencdc.Change{After: st},
	}

	s := &Source{
		ch: make(chan opencdc.Record, 1),
	}
	s.ch <- expectedRecord

	record, err := s.Read(ctx)
	is.NoErr(err)
	is.Equal(record, expectedRecord)
}

func TestSource_Read_SourceNotInitialized(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	var s *Source
	_, err := s.Read(ctx)
	is.True(err != nil)
	is.Equal(err.Error(), "error source not opened for reading")

	s = &Source{}
	_, err = s.Read(ctx)
	is.True(err != nil)
	is.Equal(err.Error(), "error source not opened for reading")
}

func TestSource_Read_ClosedChannel(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	s := &Source{
		ch: make(chan opencdc.Record),
	}
	close(s.ch)

	_, err := s.Read(ctx)
	is.True(err != nil)
	is.Equal(err.Error(), "error reading data")
}

func TestSource_Teardown_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	s := &Source{
		ch: make(chan opencdc.Record),
		wg: &sync.WaitGroup{},
	}

	err := s.Teardown(ctx)
	is.NoErr(err)
	is.True(s.ch == nil)
}

func TestSource_Teardown_WithPendingGoroutines(t *testing.T) {
	t.Parallel()

	is := is.New(t)
	ctx := context.Background()

	s := &Source{
		ch: make(chan opencdc.Record),
		wg: &sync.WaitGroup{},
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		time.Sleep(100 * time.Millisecond)
	}()

	err := s.Teardown(ctx)
	is.NoErr(err)
	is.True(s.ch == nil)
}
