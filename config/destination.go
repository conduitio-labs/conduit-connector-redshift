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

package config

import (
	"fmt"
)

// Destination is a destination configuration needed to connect to Redshift database.
type Destination struct {
	Configuration
}

// ParseDestination parses a destination configuration.
func ParseDestination(cfg map[string]string) (Destination, error) {
	config, err := parseCommon(cfg)
	if err != nil {
		return Destination{}, fmt.Errorf("parse common config: %w", err)
	}

	destinationConfig := Destination{
		Configuration: config,
	}

	return destinationConfig, nil
}
