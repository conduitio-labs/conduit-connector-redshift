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

package redshift

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-redshift/destination"
	destConfig "github.com/conduitio-labs/conduit-connector-redshift/destination/config"
	"github.com/conduitio-labs/conduit-connector-redshift/source"
	srcConfig "github.com/conduitio-labs/conduit-connector-redshift/source/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
)

const (
	// driverName is a database driver name.
	driverName = "pgx"
	// envNameDSN is a Redshift dsn environment name.
	envNameDSN = "REDSHIFT_DSN"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	id int64
}

// GenerateRecord generates a random opencdc.Record.
func (d *driver) GenerateRecord(_ *testing.T, operation opencdc.Operation) opencdc.Record {
	atomic.AddInt64(&d.id, 1)

	return opencdc.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			opencdc.MetadataCollection: d.Config.SourceConfig[srcConfig.ConfigTable],
		},
		Key: opencdc.StructuredData{
			"col1": d.id,
		},
		Payload: opencdc.Change{After: opencdc.RawData(
			fmt.Sprintf(`{"col1":%d,"col2":"%s"}`, d.id, uuid.NewString()),
		)},
	}
}

func (d *driver) WriteTimeout() time.Duration {
	return time.Minute
}

func TestAcceptance(t *testing.T) {
	dsn := os.Getenv(envNameDSN)
	if dsn == "" {
		t.Skipf("%s env var must be set", envNameDSN)
	}

	table := fmt.Sprintf("conduit_test_%d", time.Now().UnixNano())

	srcCfg := map[string]string{
		srcConfig.ConfigDsn:            dsn,
		srcConfig.ConfigTable:          table,
		srcConfig.ConfigOrderingColumn: "col1",
	}

	destCfg := map[string]string{
		destConfig.ConfigDsn:        dsn,
		destConfig.ConfigTable:      table,
		destConfig.ConfigKeyColumns: "col1",
	}

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: Specification,
					NewSource: func() sdk.Source {
						return sdk.Source(&source.Source{})
					},
					NewDestination: destination.NewDestination,
				},
				SourceConfig:      srcCfg,
				DestinationConfig: destCfg,
				BeforeTest:        beforeTest(destCfg),
				AfterTest:         afterTest(destCfg),
				// some parameters contains "*" which is a valid character in source parameter
				Skip: []string{"TestSource_Parameters_Success"},
			},
		},
	})
}

// beforeTest creates the test table.
func beforeTest(cfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		is := is.New(t)

		db, err := sqlx.Open(driverName, cfg[destConfig.ConfigDsn])
		is.NoErr(err)
		defer db.Close()

		_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (col1 INTEGER, col2 VARCHAR(36));", cfg[destConfig.ConfigTable]))
		is.NoErr(err)
	}
}

// afterTest drops the test table.
func afterTest(cfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		is := is.New(t)

		db, err := sqlx.Open(driverName, cfg[destConfig.ConfigDsn])
		is.NoErr(err)
		defer db.Close()

		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", cfg[destConfig.ConfigTable]))
		is.NoErr(err)
	}
}
