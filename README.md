# Conduit Connector Amazon Redshift

## General

Amazon Redshift connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a Source
and a Destination Amazon Redshift connectors.

## Prerequisites

- [Go](https://go.dev/) 1.20
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.51.2

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests. To run the integration test, set the Amazon Redshift database DSN
to the environment variables as an `REDSHIFT_DSN`.

## Source

Conduit's Redshift source connector allows you to move data from a Redshift table with the specified `dsn` and `table`
configuration parameters. Upon starting, the source connector takes a snapshot of a given table in the database, then 
switches into change data capture (CDC) mode. In CDC mode, the connector will only detect new rows.

### Snapshots

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

| name             | description                                                                                                                                                                | required | example                                               | default value |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------|---------------|
| `dsn`            | [DSN](https://en.wikipedia.org/wiki/Data_source_name) to connect to Redshift.                                                                                              | **true** | `postgres://username:password@endpoint:5439/database` |               |
| `table`          | Name of the table from which the connector reads from.                                                                                                                     | **true** | `table_name`                                          |               |
| `orderingColumn` | Column used to order the rows. <br /> Keep in mind that the data will be sorted by this column, so the column must contain unique, consistent values suitable for sorting. | **true** | `id`                                                  |               |
| `snapshot`       | Whether the connector will take a snapshot of the entire table before starting cdc mode.                                                                                   | false    | `false`                                               | "true"        |
| `keyColumns`     | Comma-separated list of column names to build the `sdk.Record.Key`. See more: [Key handling](#key-handling).                                                               | false    | `id,name`                                             |               |
| `batchSize`      | Size of rows batch. Min is 1 and max is 100000.                                                                                                                            | false    | `100`                                                 | "1000"        |

### Key handling

The connector builds `sdk.Record.Key` as `sdk.StructuredData`. The keys of this field consist of elements of
the `keyColumns` configuration field. If `keyColumns` is empty, the connector uses the primary keys of the specified
table; otherwise, if the table has no primary keys, it uses the value of the `orderingColumn` field. The values
of `sdk.Record.Key` field are taken from `sdk.Payload.After` by the keys of this field.

### Table Name

For each record, the connector adds a `redshift.table` property to the metadata that contains the table name.

## Destination

Conduit's Redshift destination connector allows you to move data to a Redshift table with the specified `dsn` and `table`
configuration parameters. It takes an `sdk.Record` and parses it into a valid SQL query.

**Note**: Redshift does not support map or slice types, so they will be stored as marshaled strings.

### Configuration Options

| name         | description                                                                                                                                           | required | example                                               | default |
|--------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-------------------------------------------------------|---------|
| `dsn`        | [DSN](https://en.wikipedia.org/wiki/Data_source_name) to connect to Redshift.                                                                         | **true** | `postgres://username:password@endpoint:5439/database` | ""      |
| `table`      | Name of the table the connector writes to.                                                                                                            | **true** | `table_name`                                          | ""      |
| `keyColumns` | Comma-separated list of column names to build the where clause in case if `sdk.Record.Key` is empty.<br /> See more: [Key handling](#key-handling-1). | false    | `id,name`                                             | ""      |

### Key handling

* When inserting records (i.e. when the CDC operation is `create` or `snapshot`) the key is ignored.
* When updating records (i.e. when the CDC operations is `update`):
    * if the record key exists, it's expected to be structured
    * if the record key doesn't exist, then it will be built from the `keyColumns` in the payload's `After` field.
* When deleting records (i.e. when the CDC operation is `delete`) the key is required to be present.


### Table Name

Records are written to the table specified by the `redshift.table` property in the metadata, if it exists.
If not, the connector will fall back to the `table` configured in the connector.

## Known limitations

Creating a source or destination connector will fail in the next cases:
- connector does not have access to Redshift;
- user does not have permission;
- table does not exist.

## Useful resources
* [Quotas and limits in Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/amazon-redshift-limits.html)
