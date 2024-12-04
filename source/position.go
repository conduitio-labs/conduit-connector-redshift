// Copyright © 2024 Meroxa, Inc. & Yalantis
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
	"encoding/json"
	"fmt"

	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-commons/opencdc"
)

// Position represents Redshift's position.
type Position struct {
	TablePositions *csync.Map[string, TablePosition] `json:"tablePositions"` // Use csync.Map for thread safety
}

// NewPosition initializes a new position when sdk position is nil.
func NewPosition() *Position {
	return &Position{TablePositions: csync.NewMap[string, TablePosition]()}
}

type TablePosition struct {
	// LastProcessedValue represents the last processed value from ordering column.
	LastProcessedValue any `json:"lastProcessedValue"`
	// LatestSnapshotValue represents the most recent value of ordering column.
	LatestSnapshotValue any `json:"latestSnapshotValue"`
}

// ParseSDKPosition parses opencdc.Position and returns Position.
func ParseSDKPosition(position opencdc.Position) (*Position, error) {
	if position == nil {
		return NewPosition(), nil
	}

	var pos Position
	if err := pos.unmarshal(position); err != nil {
		return nil, fmt.Errorf("unmarshal opencdc.Position into Position: %w", err)
	}

	return &pos, nil
}

// marshal marshals Position and returns opencdc.Position or an error.
func (p *Position) marshal() (opencdc.Position, error) {
	// convert thread-safe map to a Go map for JSON serialization
	goMap := p.TablePositions.ToGoMap()

	positionBytes, err := json.Marshal(map[string]any{
		"tablePositions": goMap,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return positionBytes, nil
}

func (p *Position) unmarshal(data []byte) error {
	// temporary struct for unmarshaling JSON
	var temp struct {
		TablePositions map[string]TablePosition `json:"tablePositions"`
	}
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("unmarshal position JSON: %w", err)
	}

	// initialize csync.Map and populate it
	p.TablePositions = csync.NewMap[string, TablePosition]()
	for table, tablePos := range temp.TablePositions {
		p.TablePositions.Set(table, tablePos)
	}

	return nil
}

// set sets a table position in the source position.
func (p *Position) set(table string, newPosition TablePosition) {
	p.TablePositions.Set(table, newPosition)
}

// get fetches a table position from the source position.
func (p *Position) get(table string) (TablePosition, bool) {
	return p.TablePositions.Get(table)
}
