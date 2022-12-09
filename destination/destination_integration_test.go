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

package destination

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-redshift/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
)

// envNameURL is a Redshift url environment name.
const envNameDSN = "REDSHIFT_DSN"

func TestDestination_Write_successKeyColumns(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (col1 INTEGER, col2 INTEGER);", cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s;", cfg[config.Table]))
		is.NoErr(err)
	}()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 2);", cfg[config.Table]))
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set a KeyColumns field to the config
	cfg[config.KeyColumns] = "col1"

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	// update the record with a Key
	n, err := dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationUpdate,
			Key: sdk.StructuredData{
				"col1": 1,
			},
			Payload: sdk.Change{After: sdk.StructuredData{
				"col1": 1,
				"col2": 10,
			}},
		},
	})
	is.NoErr(err)
	is.Equal(n, 1)

	col2 := ""

	err = db.QueryRow(fmt.Sprintf("SELECT col2 FROM %s WHERE col1 = 1;", cfg[config.Table])).Scan(&col2)
	is.NoErr(err)

	col2Int, err := strconv.Atoi(col2)
	is.NoErr(err)

	is.Equal(col2Int, 10)

	// update the record with no Key
	n, err = dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationUpdate,
			Payload: sdk.Change{After: sdk.StructuredData{
				"col1": 1,
				"col2": 20,
			}},
		},
	})
	is.NoErr(err)
	is.Equal(n, 1)

	err = db.QueryRow(fmt.Sprintf("SELECT col2 FROM %s WHERE col1 = 1;", cfg[config.Table])).Scan(&col2)
	is.NoErr(err)

	col2Int, err = strconv.Atoi(col2)
	is.NoErr(err)

	is.Equal(col2Int, 20)

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_failedWrongKeyColumnsField(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (col1 INTEGER, col2 INTEGER);", cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s;", cfg[config.Table]))
		is.NoErr(err)
	}()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 2);", cfg[config.Table]))
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// set a wrong KeyColumns field to the config
	cfg[config.KeyColumns] = "wrong_column"

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	// update the record with no Key
	_, err = dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationUpdate,
			Payload: sdk.Change{After: sdk.StructuredData{
				"col1": 1,
				"col2": 10,
			}},
		},
	})
	is.True(err != nil)
	is.Equal(err.Error(), "record with no key: populate key with keyColumns: key column \"wrong_column\" not found")

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write_failedWrongPayloadKey(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t)
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (col1 INTEGER, col2 INTEGER);", cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s;", cfg[config.Table]))
		is.NoErr(err)
	}()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 2);", cfg[config.Table]))
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	_, err = dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationSnapshot,
			Payload: sdk.Change{After: sdk.StructuredData{
				"col1":      1,
				"wrong_key": 10,
			}},
		},
	})
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "record with no key: exec insert"))

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

// prepareConfig retrieves the value of the environment variable named by envNameDSN,
// generates a name of database's table and returns a configuration map.
func prepareConfig(t *testing.T, keyColumns ...string) map[string]string {
	t.Helper()

	dsn := os.Getenv(envNameDSN)
	if dsn == "" {
		t.Skipf("%s env var must be set", envNameDSN)

		return nil
	}

	return map[string]string{
		config.DSN:        dsn,
		config.Table:      fmt.Sprintf("conduit_dst_test_%d", time.Now().UnixNano()),
		config.KeyColumns: strings.Join(keyColumns, ","),
	}
}
