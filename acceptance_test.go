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

	"github.com/conduitio-labs/conduit-connector-redshift/config"
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
	// metadataFieldTable is a name of a record metadata field that stores a Redshift table name.
	metadataFieldTable = "redshift.table"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	id int64
}

// GenerateRecord generates a random sdk.Record.
func (d *driver) GenerateRecord(_ *testing.T, operation sdk.Operation) sdk.Record {
	atomic.AddInt64(&d.id, 1)

	return sdk.Record{
		Position:  nil,
		Operation: operation,
		Metadata: map[string]string{
			metadataFieldTable: d.Config.SourceConfig[config.Table],
		},
		Key: sdk.StructuredData{
			"col1": d.id,
		},
		Payload: sdk.Change{After: sdk.RawData(
			fmt.Sprintf(`{"col1":%d,"col2":"%s"}`, d.id, uuid.NewString()),
		)},
	}
}

func TestAcceptance(t *testing.T) {
	dsn := os.Getenv(envNameDSN)
	if dsn == "" {
		t.Skipf("%s env var must be set", envNameDSN)
	}

	cfg := map[string]string{
		config.DSN:            dsn,
		config.Table:          fmt.Sprintf("conduit_test_%d", time.Now().UnixNano()),
		config.OrderingColumn: "col1",
	}

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest:        beforeTest(cfg),
				AfterTest:         afterTest(cfg),
			},
		},
	})
}

// beforeTest creates the test table.
func beforeTest(cfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		is := is.New(t)

		db, err := sqlx.Open(driverName, cfg[config.DSN])
		is.NoErr(err)
		defer db.Close()

		_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (col1 INTEGER, col2 VARCHAR(36));", cfg[config.Table]))
		is.NoErr(err)
	}
}

// afterTest drops the test table.
func afterTest(cfg map[string]string) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		is := is.New(t)

		db, err := sqlx.Open(driverName, cfg[config.DSN])
		is.NoErr(err)
		defer db.Close()

		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s", cfg[config.Table]))
		is.NoErr(err)
	}
}
