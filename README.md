# Ducklake Spark

## Overview
Ducklake is an open table format. This project provides a Spark `TableCatalog` for Ducklake
so that Spark SQL users can query Ducklake tables without moving data. The catalog loads
table metadata from Ducklake's Postgres store and points Spark to the files that Ducklake
manages in object storage.

## Current Capabilities
- Read-only path: works for `SELECT` queries over existing Ducklake tables.
- Namespace and table listing from the Ducklake metadata store.
- Works with unpartitioned tables that already exist in Ducklake.

### Not yet supported
- Any DDL (`CREATE`, `ALTER`, `DROP`) or table property changes.
- Partitioned tables.
- Many complex data types (lists, maps, structs, decimals with precision/scale, blobs, json, etc.).

## Supported column types
The catalog maps Ducklake column types to Spark SQL types.
Current coverage:

| Ducklake type | Spark SQL type |
| --- | --- |
| `boolean` | `BooleanType` |
| `int8` | `ByteType` |
| `int16` | `ShortType` |
| `int32` | `IntegerType` |
| `int64` | `LongType` |
| `uint8` | `ShortType` |
| `uint16` | `IntegerType` |
| `uint32` | `LongType` |
| `uint64` | `DecimalType(20, 0)` |
| `float32` | `FloatType` |
| `float64` | `DoubleType` |
| `time` | `StringType` |
| `timetz` | `StringType` |
| `date` | `DateType` |
| `timestamp` | `TimestampType` |
| `timestamptz` | `TimestampType` |
| `timestamp_s` | `TimestampType` |
| `timestamp_ms` | `TimestampType` |
| `varchar` | `StringType` |
| `uuid` | `StringType` |

Unsupported Ducklake types will raise an error when Spark attempts to load the table schema.

## Usage
Usage instructions will be added soon. For now this section is a placeholder.
