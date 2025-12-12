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

## Supported storage types
Ducklake Spark can read data from either the local disk or Amazon S3. Pick the storage in your
Spark catalog options.

| Storage | How to enable | Extra details |
| --- | --- | --- |
| Local filesystem | `storage=local` | No extra options. Paths must be reachable from every Spark executor. |
| Amazon S3 | `storage=s3` | Provide `s3-access-key-id`, `s3-secret-access-key`, `s3-session-token`, and `s3-region`. |

Keep the values short and plain. This makes the setup easy for readers worldwide.

## Usage
1. Start PySpark (or spark-shell) with the Ducklake Spark package and catalog settings:

   ```sh
   pyspark \
     --packages dev.oleander:ducklake-spark:0.1.2 \
     --conf spark.sql.catalog.oleander=dev.oleander.ducklake.SparkCatalog \
     --conf spark.sql.catalog.oleander.postgres-host=localhost \
     --conf spark.sql.catalog.oleander.postgres-port=5432 \
     --conf spark.sql.catalog.oleander.postgres-database=ducklake \
     --conf spark.sql.catalog.oleander.postgres-user=minkyu \
     --conf spark.sql.catalog.oleander.postgres-password=secret \
     --conf spark.sql.catalog.oleander.storage=local
   ```

   Replace the user/password with your Postgres credentials. If Postgres uses trust auth, you can
   omit the password line. Pick `storage=local` or `storage=s3` as needed. When using S3, also pass
   `s3-access-key-id`, `s3-secret-access-key`, `s3-session-token` (if temporary), and `s3-region`.

2. Use the catalog and run SQL over Ducklake tables:

   ```python
   spark.sql("USE oleander")
   spark.sql("SELECT * FROM flowers").show()
   spark.sql("SELECT name, color, evergreen FROM flowers").show()
   ```

   Spark now reads the Ducklake tables directly without copying data.
