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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-redshift/columntypes"
	"github.com/conduitio-labs/conduit-connector-redshift/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
)

// envNameURL is a Redshift url environment name.
const envNameDSN = "REDSHIFT_DSN"

func TestDestination_Write_checkTypes(t *testing.T) {
	//nolint:tagliatelle // Redshift does not support uppercase letters
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
		is  = is.New(t)
		cfg = prepareConfig(t)
	)

	db, err := sqlx.Open(driverName, cfg[config.DSN])
	is.NoErr(err)
	defer db.Close()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	locationKyiv, err := time.LoadLocation("Europe/Kyiv")
	is.NoErr(err)

	varbyteTypeData := "test_varbyte"
	varbyteTypeHex := make([]byte, hex.EncodedLen(len(varbyteTypeData)))
	hex.Encode(varbyteTypeHex, []byte(varbyteTypeData))

	record := dataRow{
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

	var payload sdk.StructuredData
	payloadBytes, err := json.Marshal(record)
	is.NoErr(err)

	err = json.Unmarshal(payloadBytes, &payload)
	is.NoErr(err)

	dest := NewDestination()

	err = dest.Configure(ctx, cfg)
	is.NoErr(err)

	err = dest.Open(ctx)
	is.NoErr(err)

	// update the record with a Key
	n, err := dest.Write(ctx, []sdk.Record{
		{
			Operation: sdk.OperationCreate,
			Payload:   sdk.Change{After: payload},
		},
	})
	is.NoErr(err)
	is.Equal(n, 1)

	rows, err := db.QueryxContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 1;", cfg[config.Table]))
	is.NoErr(err)
	defer rows.Close()

	is.True(rows.Next())

	var (
		got             dataRow
		timeTypeStr     string
		timeTzTypeStr   string
		timestampTzType time.Time
	)

	err = rows.Scan(&got.SmallIntType, &got.IntegerType, &got.BigIntType, &got.DecimalType, &got.RealType,
		&got.DoubleType, &got.FloatType, &got.BooleanType, &got.CharType, &got.VarcharType, &got.DateType,
		&got.TimestampType, &timestampTzType, &timeTypeStr, &timeTzTypeStr, &got.VarbyteType)
	is.NoErr(err)

	// update location
	got.TimestampTzType = timestampTzType.In(locationKyiv)

	// parse time via layout and set year, month, and day from the original record
	timeType, err := time.Parse(columntypes.TimeTypeLayout, timeTypeStr)
	is.NoErr(err)
	got.TimeType = time.Date(record.TimeType.Year(), record.TimeType.Month(), record.TimeType.Day(),
		timeType.Hour(), timeType.Minute(), timeType.Second(), 0, time.UTC)

	// parse time via layout with timezone,
	// set year, month, and day from the original record,
	// and represent in selected timezone
	timeTzType, err := time.Parse(columntypes.TimeTzTypeLayout, timeTzTypeStr)
	is.NoErr(err)
	got.TimeTzType = time.Date(record.TimeTzType.Year(), record.TimeTzType.Month(), record.TimeTzType.Day(),
		timeTzType.Hour(), timeTzType.Minute(), timeTzType.Second(), 0, time.UTC).In(locationKyiv)

	// decode VarbyteType
	varbyteTypeBytes, err := hex.DecodeString(got.VarbyteType)
	is.NoErr(err)
	got.VarbyteType = string(varbyteTypeBytes)

	gotBytes, err := json.Marshal(got)
	is.NoErr(err)

	is.Equal(payloadBytes, gotBytes)

	cancel()

	err = dest.Teardown(context.Background())
	is.NoErr(err)
}

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
func prepareConfig(t *testing.T) map[string]string {
	t.Helper()

	dsn := os.Getenv(envNameDSN)
	if dsn == "" {
		t.Skipf("%s env var must be set", envNameDSN)

		return nil
	}

	return map[string]string{
		config.DSN:   dsn,
		config.Table: fmt.Sprintf("conduit_dst_test_%d", time.Now().UnixNano()),
	}
}
