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

package common

const (
	// OrderingColumn is a config name for the orderingColumn field.
	OrderingColumn = "orderingColumn"
	// Snapshot is a config name for the snapshot field.
	Snapshot = "snapshot"
	// BatchSize is the config name for a batch size.
	BatchSize = "batchSize"

	// defaultBatchSize is a default value for the batchSize field.
	defaultBatchSize = 1000
	// defaultSnapshot is a default value for the snapshot field.
	defaultSnapshot = true
)

// // Source contains source-specific configurable values.
// type Source struct {
// 	Configuration

// 	// OrderingColumn is a name of a column that the connector will use for ordering rows.
// 	OrderingColumn string `key:"orderingColumn" validate:"required,lowercase,excludesall= ,lte=127"`
// 	// Snapshot is the configuration that determines whether the connector
// 	// will take a snapshot of the entire table before starting cdc mode.
// 	Snapshot bool `key:"snapshot"`
// 	// BatchSize is a size of rows batch.
// 	BatchSize int `key:"batchSize" validate:"gte=1,lte=100000"`
// }

// // ParseSource parses source configuration to the [Source] and validates it.
// func ParseSource(cfg map[string]string) (Source, error) {
// 	commonConfig, err := parseCommon(cfg)
// 	if err != nil {
// 		return Source{}, fmt.Errorf("parse source configuration: %w", err)
// 	}

// 	sourceConfig := Source{
// 		Configuration:  commonConfig,
// 		OrderingColumn: cfg[OrderingColumn],
// 		Snapshot:       defaultSnapshot,
// 		BatchSize:      defaultBatchSize,
// 	}

// 	if cfg[Snapshot] != "" {
// 		sourceConfig.Snapshot, err = strconv.ParseBool(cfg[Snapshot])
// 		if err != nil {
// 			return Source{}, fmt.Errorf("parse %q: %w", Snapshot, err)
// 		}
// 	}

// 	if cfg[BatchSize] != "" {
// 		sourceConfig.BatchSize, err = strconv.Atoi(cfg[BatchSize])
// 		if err != nil {
// 			return Source{}, fmt.Errorf("parse %q: %w", BatchSize, err)
// 		}
// 	}

// 	err = validateStruct(sourceConfig)
// 	if err != nil {
// 		return Source{}, err
// 	}

// 	return sourceConfig, nil
// }
