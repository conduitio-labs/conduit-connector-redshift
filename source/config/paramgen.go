// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package config

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigBatchSize      = "batchSize"
	ConfigDsn            = "dsn"
	ConfigKeyColumns     = "keyColumns"
	ConfigOrderingColumn = "orderingColumn"
	ConfigSnapshot       = "snapshot"
	ConfigTable          = "table"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigBatchSize: {
			Default:     "1000",
			Description: "BatchSize is a size of rows batch.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{},
		},
		ConfigDsn: {
			Default:     "",
			Description: "DSN is the configuration of the data source name to connect to the Amazon Redshift.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigKeyColumns: {
			Default:     "",
			Description: "KeyColumns is the configuration list of column names to build the opencdc.Record.Key (for Source).",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigOrderingColumn: {
			Default:     "",
			Description: "OrderingColumn is a name of a column that the connector will use for ordering rows.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigSnapshot: {
			Default:     "true",
			Description: "Snapshot is the configuration that determines whether the connector\nwill take a snapshot of the entire table before starting cdc mode.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		ConfigTable: {
			Default:     "",
			Description: "Table is the configuration of the table name.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
