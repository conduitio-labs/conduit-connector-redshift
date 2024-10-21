// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package config

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigDsn        = "dsn"
	ConfigKeyColumns = "keyColumns"
	ConfigTable      = "table"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
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
			Description: "KeyColumns is the configuration of comma-separated column names to build the sdk.Record.Key.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigTable: {
			Default:     "{{ index .Metadata \"opencdc.collection\" }}",
			Description: "Table is the configuration of the table name.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
	}
}
