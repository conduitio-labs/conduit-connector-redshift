# Conduit Connector Amazon Redshift

## General

Amazon Redshift connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a Source
and a Destination Amazon Redshift connectors.

## Prerequisites

- [Go](https://go.dev/) 1.19
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.50.1

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests. To run the integration test, set the Amazon Redshift database DSN
to the environment variables as an `REDSHIFT_DSN`.

## Source

The Redshift Source Connector allows you to move data from a Redshift table with the specified `dsn` and `table`
configuration parameters. Upon starting, the Source takes a snapshot of a given table in the database, then switches
into CDC mode. In CDC mode, the connector will only detect new rows.

### Snapshot Capture

At the first launch of the connector, the snapshot mode is enabled and the last value of the `orderingColumn` is stored
to the position, to know the boundary of this mode. The connector reads all rows of a table in batches, using a
keyset pagination, limiting the rows by `batchSize` and ordering by `orderingColumn`. The connector stores the
last processed element value of an `orderingColumn` in a position, so the snapshot process can be paused and resumed
without losing data. Once all rows in that initial snapshot are read the connector switches into CDC mode.

This behavior is enabled by default, but can be turned off by adding `"snapshot": false` to the Source configuration.

### Change Data Capture

In this mode, only rows added after the first launch of the connector are moved in batches. It’s using a keyset
pagination, limiting by `batchSize` and ordering by `orderingColumn`.

### Configuration

| name             | description                                                                                                                                                                                     | required | example                                               |
|------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------|
| `dsn`            | [DSN](https://en.wikipedia.org/wiki/Data_source_name) to connect to Redshift.                                                                                                                   | **true** | `postgres://username:password@endpoint:5439/database` |
| `table`          | Name of a table, the connector must read from.                                                                                                                                                  | **true** | `table_name`                                          |
| `orderingColumn` | Column name that the connector will use to order the rows. Keep in mind that the data will be sorted by this column, so the column must contain unique, consistent values suitable for sorting. | **true** | `id`                                                  |
| `snapshot`       | Whether the connector will take a snapshot of the entire table before starting cdc mode. By default is `"true"`.                                                                                | false    | `false`                                               |
| `keyColumns`     | Comma-separated list of column names to build the `sdk.Record.Key`. See more: [Key handling](#key-handling).                                                                                    | false    | `id,name`                                             |
| `batchSize`      | Size of rows batch. Min is 1 and max is 100000. By default is `"1000"`.                                                                                                                         | false    | `100`                                                 |

### Key handling

The connector builds `sdk.Record.Key` as `sdk.StructuredData`. The keys of this field consist of elements of
the `keyColumns` configuration field. If `keyColumns` is empty, the connector uses the primary keys of the specified
table; otherwise, if the table has no primary keys, it uses the value of the `orderingKey` field. The values
of `sdk.Record.Key` field are taken from `sdk.Payload.After` by the keys of this field.

### Table Name

For each record, the connector adds a `redshift.table` property to the metadata that contains the table name.

## Known limitations

Creating a Source or Destination connector will fail in the next cases:

- connector does not have access to Redshift;
- user does not have permission;
- table does not exist.

[Quotas and limits in Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/amazon-redshift-limits.html)
