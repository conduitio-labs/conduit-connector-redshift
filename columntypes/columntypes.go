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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
)

const (
	numericType = "numeric"
	timeType    = "time without time zone"
	timeTzType  = "time with time zone"

	timeTypeLayout   = "15:04:05"
	timeTzTypeLayout = "15:04:05Z07"

	selectColumns = `"column", type`
	colTable      = "tablename"
	colSchema     = "schemaname"
	tableDef      = "pg_table_def"
)

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

			val, err := time.Parse(timeTypeLayout, valueStr)
			if err != nil {
				return nil, fmt.Errorf("convert time type from string to time: %w", err)
			}

			result[key] = val

		case typeName == timeTzType:
			valueStr, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("convert %q value to string", value)
			}

			val, err := time.Parse(timeTzTypeLayout, strings.TrimSpace(valueStr))
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
