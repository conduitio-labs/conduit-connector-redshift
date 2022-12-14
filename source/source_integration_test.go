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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-redshift/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
)

const (
	// envNameURL is a Redshift url environment name.
	envNameDSN = "REDSHIFT_DSN"
	// pingTimeoutSec is a ping db timeout in seconds.
	pingTimeoutSec = 10
)

func TestSource_Read_tableDoesNotExist(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t, "col")
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxTimeout, cancel := context.WithTimeout(ctx, pingTimeoutSec*time.Second)
	defer cancel()

	err = db.PingContext(ctxTimeout)
	is.NoErr(err)

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.True(strings.Contains(err.Error(),
		"new iterator: get latest snapshot value: execute select latest snapshot value query"))

	cancel()
}

func TestSource_Read_tableHasNoData(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t, "col")
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxTimeout, cancel := context.WithTimeout(ctx, pingTimeoutSec*time.Second)
	defer cancel()

	err = db.PingContext(ctxTimeout)
	is.NoErr(err)

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (col INTEGER, PRIMARY KEY (col));", cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s;", cfg[config.Table]))
		is.NoErr(err)
	}()

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_keyColumnsFromConfig(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t, "col1", "col1", "col2")
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxTimeout, cancel := context.WithTimeout(ctx, pingTimeoutSec*time.Second)
	defer cancel()

	err = db.PingContext(ctxTimeout)
	is.NoErr(err)

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (col1 INTEGER, col2 INTEGER);", cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s;", cfg[config.Table]))
		is.NoErr(err)
	}()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 2);", cfg[config.Table]))
	is.NoErr(err)

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Key, sdk.StructuredData(map[string]any{
		"col1": int64(1),
		"col2": int64(2),
	}))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_keyColumnsFromTableMetadata(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t, "col1")
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxTimeout, cancel := context.WithTimeout(ctx, pingTimeoutSec*time.Second)
	defer cancel()

	err = db.PingContext(ctxTimeout)
	is.NoErr(err)

	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE %s (col1 INTEGER, col2 INTEGER, col3 INTEGER, PRIMARY KEY (col1, col2, col3));",
		cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s;", cfg[config.Table]))
		is.NoErr(err)
	}()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 2, 3);", cfg[config.Table]))
	is.NoErr(err)

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Key, sdk.StructuredData(map[string]any{
		"col1": int64(1),
		"col2": int64(2),
		"col3": int64(3),
	}))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_keyColumnsFromOrderingColumn(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t, "col1")
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxTimeout, cancel := context.WithTimeout(ctx, pingTimeoutSec*time.Second)
	defer cancel()

	err = db.PingContext(ctxTimeout)
	is.NoErr(err)

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (col1 INTEGER, col2 INTEGER);", cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s;", cfg[config.Table]))
		is.NoErr(err)
	}()

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 2);", cfg[config.Table]))
	is.NoErr(err)

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Key, sdk.StructuredData(map[string]any{
		"col1": int64(1),
	}))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

//nolint:tagliatelle // Redshift does not support column names in uppercase
func TestSource_Read_checkTypes(t *testing.T) {
	const (
		timeTypeLayout   = "15:04:05"
		timeTzTypeLayout = "15:04:05Z07:00"
	)

	type dataRow struct {
		SmallIntType    int16     `json:"small_int_type"`
		IntegerType     int32     `json:"integer_type"`
		BigIntType      int64     `json:"big_int_type"`
		DecimalType     float64   `json:"decimal_type"`
		RealType        float32   `json:"real_type"`
		DoubleType      float64   `json:"double_type"`
		FloatType       float64   `json:"float_type"`
		BooleanType     bool      `json:"boolean_type"`
		CharType        string    `json:"char_type"`
		VarcharType     string    `json:"varchar_type"`
		DateType        time.Time `json:"date_type"`
		TimestampType   time.Time `json:"timestamp_type"`
		TimestampTzType time.Time `json:"timestamp_tz_type"`
		TimeType        time.Time `json:"time_type"`
		TimeTzType      time.Time `json:"time_tz_type"`
		VarbyteType     string    `json:"varbyte_type"`
	}

	var (
		is             = is.New(t)
		orderingColumn = "small_int_type"
		cfg            = prepareConfig(t, orderingColumn)
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxTimeout, cancel := context.WithTimeout(ctx, pingTimeoutSec*time.Second)
	defer cancel()

	err = db.PingContext(ctxTimeout)
	is.NoErr(err)

	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s
	(
		small_int_type    smallint,
		integer_type      integer,
		big_int_type      bigint,
		decimal_type      numeric(18,2),
		real_type         real,
		double_type       double precision,
		float_type        float,
		boolean_type      boolean,
		char_type         char,
		varchar_type      varchar,
		date_type         date,
		timestamp_type    timestamp,
		timestamp_tz_type timestamptz,
		time_type         time,
		time_tz_type      timetz,
		varbyte_type      varbyte
	);`, cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s;", cfg[config.Table]))
		is.NoErr(err)
	}()

	locationKyiv, err := time.LoadLocation("Europe/Warsaw")
	is.NoErr(err)

	varbyteTypeData := "test_varbyte"
	varbyteTypeHex := make([]byte, hex.EncodedLen(len(varbyteTypeData)))
	hex.Encode(varbyteTypeHex, []byte(varbyteTypeData))

	want := dataRow{
		SmallIntType:    1,
		IntegerType:     2,
		BigIntType:      3,
		DecimalType:     123.45,
		RealType:        float32(234.56),
		DoubleType:      345.67,
		FloatType:       456.78,
		BooleanType:     true,
		CharType:        "a",
		VarcharType:     "test_varchar",
		DateType:        time.Date(2022, 2, 24, 0, 0, 0, 0, time.UTC),
		TimestampType:   time.Date(2022, 2, 24, 23, 0, 0, 0, time.UTC),
		TimestampTzType: time.Date(2022, 2, 24, 23, 15, 0, 0, locationKyiv),
		TimeType:        time.Date(2022, 2, 24, 23, 30, 0, 0, time.UTC),
		TimeTzType:      time.Date(2022, 2, 24, 23, 45, 0, 0, locationKyiv),
		VarbyteType:     varbyteTypeData,
	}

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16);",
		cfg[config.Table]),
		want.SmallIntType,
		want.IntegerType,
		want.BigIntType,
		want.DecimalType,
		want.RealType,
		want.DoubleType,
		want.FloatType,
		want.BooleanType,
		want.CharType,
		want.VarcharType,
		want.DateType,
		want.TimestampType,
		want.TimestampTzType,
		want.TimeType.Format(timeTypeLayout),
		want.TimeTzType.Format(timeTzTypeLayout),
		want.VarbyteType)
	is.NoErr(err)

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Key, sdk.StructuredData(map[string]interface{}{orderingColumn: int64(want.SmallIntType)}))

	got := dataRow{}
	err = json.Unmarshal(record.Payload.After.Bytes(), &got)
	is.NoErr(err)

	is.Equal(got.SmallIntType, want.SmallIntType)
	is.Equal(got.IntegerType, want.IntegerType)
	is.Equal(got.BigIntType, want.BigIntType)
	is.Equal(got.DecimalType, want.DecimalType)
	is.Equal(got.RealType, want.RealType)
	is.Equal(got.DoubleType, want.DoubleType)
	is.Equal(got.FloatType, want.FloatType)
	is.Equal(got.BooleanType, want.BooleanType)
	is.Equal(got.CharType, want.CharType)
	is.Equal(got.VarcharType, want.VarcharType)
	is.Equal(got.DateType, want.DateType)
	is.Equal(got.TimestampType, want.TimestampType)
	is.Equal(got.TimestampTzType.UTC(), want.TimestampTzType.UTC())
	is.Equal(got.TimeType.Format(timeTypeLayout), want.TimeType.Format(timeTypeLayout))
	is.Equal(got.TimeTzType.UTC().Format(timeTzTypeLayout), want.TimeTzType.UTC().Format(timeTzTypeLayout))
	is.Equal(got.VarbyteType, string(varbyteTypeHex))

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_Read_snapshotIsFalse(t *testing.T) {
	var (
		is  = is.New(t)
		cfg = prepareConfig(t, "col1")
	)

	// set snapshot value to false
	cfg[config.Snapshot] = "false"

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxTimeout, cancel := context.WithTimeout(ctx, pingTimeoutSec*time.Second)
	defer cancel()

	err = db.PingContext(ctxTimeout)
	is.NoErr(err)

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (col1 INTEGER, col2 INTEGER);", cfg[config.Table]))
	is.NoErr(err)

	defer func() {
		_, err = db.Exec(fmt.Sprintf("DROP TABLE %s;", cfg[config.Table]))
		is.NoErr(err)
	}()

	// insert a row to be sure that this data will not be transferred to the destination
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (1, 2);", cfg[config.Table]))
	is.NoErr(err)

	src := NewSource()

	err = src.Configure(ctx, cfg)
	is.NoErr(err)

	err = src.Open(ctx, nil)
	is.NoErr(err)

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	// insert an additional row
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (3, 4);", cfg[config.Table]))
	is.NoErr(err)

	record, err := src.Read(ctx)
	is.NoErr(err)
	is.Equal(record.Key, sdk.StructuredData(map[string]any{
		"col1": int64(3),
	}))
	is.Equal(record.Operation, sdk.OperationCreate)

	_, err = src.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	cancel()

	err = src.Teardown(context.Background())
	is.NoErr(err)
}

// prepareConfig retrieves the value of the environment variable named by envNameDSN,
// generates a name of database's table and returns a configuration map.
func prepareConfig(t *testing.T, orderingColumn string, keyColumns ...string) map[string]string {
	t.Helper()

	dsn := os.Getenv(envNameDSN)
	if dsn == "" {
		t.Skipf("%s env var must be set", envNameDSN)

		return nil
	}

	return map[string]string{
		config.DSN:            dsn,
		config.Table:          fmt.Sprintf("conduit_src_test_%d", time.Now().UnixNano()),
		config.OrderingColumn: orderingColumn,
		config.KeyColumns:     strings.Join(keyColumns, ","),
	}
}
