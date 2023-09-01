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

package columntypes

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

const (
	// TimeTypeLayout is a time layout of Redshift Time data type.
	TimeTypeLayout = "15:04:05"
	// TimeTzTypeLayout is a time layout of Redshift Time with timezone data type.
	TimeTzTypeLayout = "15:04:05Z07"

	// Redshift types.
	numericType = "numeric"
	timeType    = "time without time zone"
	timeTzType  = "time with time zone"

	// constants for sql-builder to get column types.
	selectColumns = `"column", type`
	colTable      = "tablename"
	colSchema     = "schemaname"
	tableDef      = "pg_table_def"
)

var timeLayouts = []string{
	time.RFC3339, time.RFC3339Nano, time.Layout, time.ANSIC, time.UnixDate, time.RubyDate, time.RFC822, time.RFC822Z,
	time.RFC850, time.RFC1123, time.RFC1123Z, time.RFC3339, time.RFC3339, time.RFC3339Nano, time.Kitchen, time.Stamp,
	time.StampMilli, time.StampMicro, time.StampNano,
}

// GetColumnTypes returns a map containing all table's columns and their database types.
func GetColumnTypes(ctx context.Context, db *sqlx.DB, table, schema string) (map[string]string, error) {
	sb := sqlbuilder.PostgreSQL.NewSelectBuilder().
		Select(selectColumns).
		From(tableDef)

	sb.Where(sb.Equal(colTable, table))

	if schema != "" {
		sb.Where(sb.Equal(colSchema, schema))
	}

	query, args := sb.Build()

	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select column types %q, %v: %w", query, args, err)
	}
	defer rows.Close()

	columnTypes := make(map[string]string)
	for rows.Next() {
		var columnName, dataType string
		err = rows.Scan(&columnName, &dataType)
		if err != nil {
			return nil, fmt.Errorf("scan column types rows: %w", err)
		}

		columnTypes[columnName] = dataType
	}

	return columnTypes, nil
}

// TransformRow converts row map values to appropriate Go types, based on the columnTypes.
func TransformRow(row map[string]any, columnTypes map[string]string) (map[string]any, error) {
	result := make(map[string]any, len(row))

	for key, value := range row {
		if value == nil {
			result[key] = nil

			continue
		}

		switch typeName := columnTypes[key]; {
		case strings.Contains(typeName, numericType):
			valueStr, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("convert %q value to string", value)
			}

			val, err := strconv.ParseFloat(valueStr, 64)
			if err != nil {
				return nil, fmt.Errorf("convert numeric type from string to time: %w", err)
			}

			result[key] = val

		case typeName == timeType:
			valueStr, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("convert %q value to string", value)
			}

			val, err := time.Parse(TimeTypeLayout, valueStr)
			if err != nil {
				return nil, fmt.Errorf("convert time type from string to time: %w", err)
			}

			result[key] = val

		case typeName == timeTzType:
			valueStr, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("convert %q value to string", value)
			}

			val, err := time.Parse(TimeTzTypeLayout, strings.TrimSpace(valueStr))
			if err != nil {
				return nil, fmt.Errorf("convert time with timezone type from string to time: %w", err)
			}

			result[key] = val

		default:
			result[key] = value
		}
	}

	return result, nil
}

// ConvertStructureData converts a [sdk.StructureData] values to a proper database types.
func ConvertStructureData(
	columnTypes map[string]string,
	data sdk.StructuredData,
) (sdk.StructuredData, error) {
	result := make(sdk.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = nil

			continue
		}

		// Redshift does not support map or slice types,
		// so it'll be stored as marshaled strings.
		//nolint:exhaustive // there is no need to check all types
		switch reflect.TypeOf(value).Kind() {
		case reflect.Map, reflect.Slice:
			bs, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("marshal map or slice type: %w", err)
			}

			result[key] = string(bs)

			continue
		}

		switch columnTypes[key] {
		case timeType:
			t, err := parseTime(value.(string))
			if err != nil {
				return nil, fmt.Errorf("parse time: %w", err)
			}

			result[key] = t.Format(TimeTypeLayout)
		case timeTzType:
			t, err := parseTime(value.(string))
			if err != nil {
				return nil, fmt.Errorf("parse time: %w", err)
			}

			result[key] = t.Format(TimeTzTypeLayout)
		default:
			result[key] = value
		}
	}

	return result, nil
}

func parseTime(val string) (time.Time, error) {
	for i := range timeLayouts {
		timeValue, err := time.Parse(timeLayouts[i], val)
		if err != nil {
			continue
		}

		return timeValue, nil
	}

	return time.Time{}, fmt.Errorf("cannot parse time: %s", val)
}
