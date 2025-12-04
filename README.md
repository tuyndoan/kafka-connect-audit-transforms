# Kafka Connect Audit Transforms

[![Version](https://img.shields.io/badge/version-2.0.1-blue.svg)](https://github.com/tuyndoan/kafka-connect-audit-transforms)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)
[![Java](https://img.shields.io/badge/java-8%2B-orange.svg)](https://www.oracle.com/java/)

**Transform Debezium CDC data into standardized audit records** for compliance, audit logging, and data warehouse storage. A production-ready Kafka Connect Single Message Transformation (SMT) plugin that works seamlessly with all Debezium connectors.

## 🚀 Features

- ✅ **ExtractAuditRecordState**: Transform Debezium CDC records into standardized audit format for compliance and audit logging
- ✅ **Multi-Database Support**: Works with SQL Server, MySQL, PostgreSQL, Oracle, MongoDB, and all Debezium-supported databases
- ✅ **Production Ready**: Comprehensive error handling, logging, and performance optimizations
- ✅ **InsertField**: Additional SMT for inserting Kafka metadata (topic, partition, offset, timestamp) or static values into records

## Overview

This project provides **ExtractAuditRecordState**, a specialized Single Message Transformation for Kafka Connect that transforms Change Data Capture (CDC) records from Debezium connectors into standardized audit record format. Perfect for organizations requiring comprehensive audit logging, compliance tracking, and data warehouse ETL pipelines.

**Primary Use Case**: Transform CDC data from any database (via Debezium) into standardized audit records stored in data warehouses like ClickHouse for compliance and analytics.

**Additional Feature**: Includes `InsertField` SMT for inserting Kafka metadata and static values into records.

## Architecture & Data Flow

### Complete End-to-End Flow

```
┌─────────────────┐
│  Source Database│
│  (SQL Server,   │
│   MySQL,        │
│   PostgreSQL,   │
│   Oracle, etc.) │
└────────┬────────┘
         │
         │ CDC Changes (INSERT/UPDATE/DELETE)
         ▼
┌─────────────────────────────────────────┐
│  Debezium Connector                     │
│  (SQL Server, MySQL, PostgreSQL, etc.)  │
│  - Captures CDC changes                 │
│  - Publishes to Kafka topics            │
│  - Topic format: {prefix}.{db}.{schema}.{table} │
└────────┬────────────────────────────────┘
         │
         │ Kafka Messages (Debezium Envelope)
         ▼
┌─────────────────────────────────────────┐
│  Apache Kafka                          │
│  - Topics: example_data_catalog_*      │
│  - Schema: Debezium Envelope           │
└────────┬────────────────────────────────┘
         │
         │ Transformed Messages
         ▼
┌─────────────────────────────────────────┐
│  ClickHouse Sink Connector             │
│  + ExtractAuditRecordState SMT         │
│    - Extracts audit info                │
│    - Transforms to audit format         │
│  + RegexRouter SMT                      │
│    - Routes to audit_records table      │
└────────┬────────────────────────────────┘
         │
         │ Standardized Audit Records
         ▼
┌─────────────────┐
│  ClickHouse     │
│  audit_records  │
│  (Data Warehouse)│
└─────────────────┘
```

### Data Transformation Flow

**Step 1: Source Database Change**
- User performs INSERT/UPDATE/DELETE in source database (SQL Server, MySQL, PostgreSQL, etc.)
- Database CDC mechanism captures the change

**Step 2: Debezium Capture**
- Debezium connector reads CDC log
- Creates Kafka message with Debezium envelope:
  ```json
  {
    "op": "c|u|d",  // create, update, delete
    "before": {...},  // original values (for UPDATE/DELETE)
    "after": {...},   // new values (for INSERT/UPDATE)
    "source": {
      "db": "EXAMPLE_DB",
      "table": "ExampleTable",
      "ts_ms": 1234567890
    }
  }
  ```

**Step 3: ExtractAuditRecordState Transformation**
- Extracts operation type from `op` field
- Extracts primary keys from record key
- Extracts before/after values
- Extracts source metadata (database, table, timestamps)
- Transforms to standardized audit format:
  ```json
  {
    "id": "uuid",
    "change_type": "INSERT|UPDATE|DELETE",
    "change_time": 1234567890,
    "primary_keys": "{\"Id\": 123}",
    "new_values": "{\"Name\": \"New\"}",
    "original_values": "{\"Name\": \"Old\"}",
    "field_includes": "{\"Code\": \"ABC\"}",
    "database": "EXAMPLE_DB",
    "table": "ExampleTable",
    ...
  }
  ```

**Step 4: Storage in ClickHouse**
- All audit records stored in single `audit_records` table
- Partitioned by month and table name
- TTL: 6 months automatic cleanup

## 🚀 Quick Start

### 1️⃣ Build the Project

```bash
mvn clean package
```

> **Output**: The JAR file will be created in `target/kafka-connect-smt-2.0.1.jar`

### 2️⃣ Install the SMT

Copy the JAR to your Kafka Connect plugin path:

```bash
cp target/kafka-connect-smt-2.0.1.jar /path/to/kafka/connect/plugins/
```

> **Note**: Restart your Kafka Connect worker after installation.

### 3️⃣ Setup ClickHouse Table

Create the audit_records table in ClickHouse:

```sql
CREATE TABLE IF NOT EXISTS audit_records 
(
    id String,
    topic String,
    partition UInt32,
    offset Nullable(Int64),
    change_type String,
    change_time DateTime64(3),
    sync_date DateTime64(3),
    primary_keys String,
    new_values String,
    original_values String,
    field_includes String,
    database String,
    schema Nullable(String),
    table String,
    connector Nullable(String),
    change_lsn Nullable(String),
    commit_lsn Nullable(String),
    event_serial_no Nullable(Int64),
    snapshot Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(sync_date), table)
ORDER BY (table, sync_date)
TTL sync_date + INTERVAL 6 MONTH;
```

See `sql/create_table_audit_records.sql` for the complete script.

### 4️⃣ Configure Debezium Source Connector

Use the example configuration in `connectors/debezium-sqlserver-source-example.json` (for SQL Server):

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/debezium-sqlserver-source-example.json
```

> **📝 Note**: This example uses SQL Server, but works with any Debezium-supported database.

### 5️⃣ Configure ClickHouse Sink Connector

Use the example configuration in `connectors/extractaudit-clickhouse-sink-example.json`:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/extractaudit-clickhouse-sink-example.json
```

> **✅ Done**: Your CDC pipeline is now configured! Messages will be transformed and stored in ClickHouse.

## 📦 Components

### 1. ExtractAuditRecordState (Primary - Audit Transformation)

**The core transformation for audit logging and compliance.** Specifically designed for processing CDC (Change Data Capture) data from Debezium connectors (supports SQL Server, MySQL, PostgreSQL, Oracle, MongoDB, and other Debezium-supported databases). It extracts audit information from CDC records and transforms them into a standardized audit record format suitable for data warehouse storage and compliance tracking.

#### ✨ Features
- **Operation Type Detection**: Uses `op` field from Debezium envelope to accurately identify operation type (INSERT, UPDATE, DELETE)
- Extracts before/after values with proper null handling
- Extracts primary keys from record key
- Handles timezone conversion for date/time fields
- Supports field inclusion/exclusion filters
- Generates unique message IDs (UUID)
- Extracts database and table information from source metadata
- **Empty Field Handling**: Empty JSON fields return `"{}"` instead of `""` for consistency
- **Robust Error Handling**: Safely handles missing fields and null values

#### 📊 Output Schema
- `id` (String): Unique identifier (UUID) for the audit record
- `topic` (String): Kafka topic name
- `partition` (Int32): Kafka partition number
- `change_type` (String): Type of change (INSERT, UPDATE, DELETE) - determined from `op` field
- `change_time` (Timestamp): Timestamp of the change (required field, extracted from `source.ts_ms` or defaults to current time)
- `primary_keys` (String): JSON string of primary key fields (returns `"{}"` if empty)
- `new_values` (String): JSON string of new/after values (returns `"{}"` if empty)
- `original_values` (String): JSON string of original/before values (returns `"{}"` if empty)
- `field_includes` (String): JSON string of included fields for UPDATE operations (returns `"{}"` if empty)
- `database` (String): Source database name (from `source.db`)
- `schema` (String, optional): Source schema name (from `source.schema`)
- `table` (String): Source table name (from `source.table`)
- `connector` (String, optional): Connector name (from `source.connector`)
- `change_lsn` (String, optional): Change Log Sequence Number (from `source.change_lsn`)
- `commit_lsn` (String, optional): Commit Log Sequence Number (from `source.commit_lsn`)
- `event_serial_no` (Int64, optional): Event serial number (from `source.event_serial_no`)
- `snapshot` (String, optional): Snapshot flag (from `source.snapshot`)
- `offset` (Int64, optional): Kafka offset (extracted from sink record if available)
- `sync_date` (Timestamp): Timestamp when the record was processed/synced (required field, defaults to current time)

**Usage:**

```json
{
  "transforms": "ExtractAudit",
  "transforms.ExtractAudit.type": "com.tuyndv.kafka.smt.ExtractAuditRecordState$Value",
  "transforms.ExtractAudit.exclude": "column1,column2",
  "transforms.ExtractAudit.include": "column3,column4",
  "transforms.ExtractAudit.timezone": "UTC"
}
```

#### ⚙️ Configuration Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `exclude` | List | `["Timestamp"]` | Fields to exclude from processing. Takes precedence over include. Default: Timestamp |
| `include` | List | `[]` | Fields to include. If specified, only these fields will be processed |
| `timezone` | String | `"UTC"` | Timezone for date/time field conversion |

#### 📚 Examples

**Basic usage:**
```json
{
  "transforms": "ExtractAudit",
  "transforms.ExtractAudit.type": "com.tuyndv.kafka.smt.ExtractAuditRecordState$Value"
}
```

**With field filtering:**
```json
{
  "transforms": "ExtractAudit",
  "transforms.ExtractAudit.type": "com.tuyndv.kafka.smt.ExtractAuditRecordState$Value",
  "transforms.ExtractAudit.exclude": "sensitive_field,internal_id",
  "transforms.ExtractAudit.timezone": "Asia/Ho_Chi_Minh"
}
```

**With field inclusion:**
```json
{
  "transforms": "ExtractAudit",
  "transforms.ExtractAudit.type": "com.tuyndv.kafka.smt.ExtractAuditRecordState$Value",
  "transforms.ExtractAudit.include": "Code, AutoCode, MethodId, BranchId, BillTypeId, CenterId"
}
```

### 2. InsertField (Additional Utility)

A utility transformation that inserts field(s) using attributes from the record metadata or a configured static value with datatype support. Useful for adding Kafka metadata or static values to records.

#### ✨ Features
- Insert Kafka topic name
- Insert Kafka partition number
- Insert Kafka offset (for sink connectors)
- Insert record timestamp
- Insert static field with type conversion support

#### 📋 Supported Types
- `byte` / `int8`
- `short` / `int16`
- `int` / `int32`
- `long` / `int64`
- `float` / `float32`
- `double` / `float64`
- `boolean`
- `string` (default)

**Usage:**

```json
{
  "transforms": "InsertField",
  "transforms.InsertField.type": "com.tuyndv.kafka.smt.InsertField$Value",
  "transforms.InsertField.topic.field": "kafka_topic",
  "transforms.InsertField.partition.field": "kafka_partition"
}
```

#### ⚙️ Configuration Options

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `topic.field` | String | `null` | Field name for Kafka topic. Suffix with `!` for required, `?` for optional (default) |
| `partition.field` | String | `null` | Field name for Kafka partition. Suffix with `!` for required, `?` for optional |
| `offset.field` | String | `null` | Field name for Kafka offset (sink connectors only). Suffix with `!` for required, `?` for optional |
| `timestamp.field` | String | `null` | Field name for record timestamp. Suffix with `!` for required, `?` for optional |
| `static.field` | String | `null` | Field name for static data field. Suffix with `!` for required, `?` for optional |
| `static.type` | String | `null` | Data type for static field (`int8`, `int16`, `int32`, `int64`, `float32`, `float64`, `boolean`, `string`) |
| `static.value` | String | `null` | Static field value |

#### 📚 Examples

**Insert topic and partition:**
```json
{
  "transforms": "InsertField",
  "transforms.InsertField.type": "com.tuyndv.kafka.smt.InsertField$Value",
  "transforms.InsertField.topic.field": "kafka_topic",
  "transforms.InsertField.partition.field": "kafka_partition"
}
```

**Insert static field with type:**
```json
{
  "transforms": "InsertField",
  "transforms.InsertField.type": "com.tuyndv.kafka.smt.InsertField$Value",
  "transforms.InsertField.static.field": "SourceSystem",
  "transforms.InsertField.static.type": "string",
  "transforms.InsertField.static.value": "SQL_SERVER"
}
```

## 🔄 Complete End-to-End Example

### Step 1: Debezium Source Connector Configuration

See `connectors/debezium-sqlserver-source-example.json` for SQL Server example:

```json
{
  "name": "debezium-sqlserver-source-example",
  "config": {
    "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
    "tasks.max": "1",
    "topic.prefix": "example_data_catalog",
    "database.names": "EXAMPLE_DB",
    "database.hostname": "your-sqlserver-host",
    "database.port": "1433",
    "database.user": "your_username",
    "database.password": "**************************",
    "table.include.list": "dbo.ExampleTable1, dbo.ExampleTable2",
    ...
  }
}
```

> **💡 Note:** This example uses SQL Server, but ExtractAuditRecordState works with any Debezium connector:
- **SQL Server**: `io.debezium.connector.sqlserver.SqlServerConnector`
- **MySQL**: `io.debezium.connector.mysql.MySqlConnector`
- **PostgreSQL**: `io.debezium.connector.postgresql.PostgresConnector`
- **Oracle**: `io.debezium.connector.oracle.OracleConnector`
- **MongoDB**: `io.debezium.connector.mongodb.MongoDbConnector`
- And other Debezium-supported databases

**What this connector does:**
- ✅ Captures CDC changes from the source database
- ✅ Creates Kafka topics: `example_data_catalog.EXAMPLE_DB.dbo.ExampleTable1`, etc.
- ✅ Publishes Debezium envelope format messages (standard format across all Debezium connectors)

### Step 2: ClickHouse Sink Connector Configuration

See `connectors/extractaudit-clickhouse-sink-example.json`:

```json
{
  "name": "extractaudit-clickhouse-sink-example",
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "topics.regex": "data_catalog_(.*)",
    "transforms": "audit, topic_to_table",
    "transforms.audit.type": "com.tuyndv.kafka.smt.ExtractAuditRecordState$Value",
    "transforms.audit.exclude": "Timestamp",
    "transforms.audit.include": "Code, AutoCode, MethodId, BranchId, BillTypeId, CenterId",
    "transforms.topic_to_table.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.topic_to_table.regex": "data_catalog_(.*)",
    "transforms.topic_to_table.replacement": "audit_records",
    "hostname": "your-clickhouse-host",
    "port": "8123",
    "database": "your_database",
    ...
  }
}
```

**What this connector does:**
- ✅ Consumes messages from topics matching `data_catalog_*`
- ✅ Applies `ExtractAuditRecordState` transformation to convert to audit format (automatically extracts offset and sync_date)
- ✅ Routes all messages to `audit_records` table in ClickHouse

### Step 3: Data Flow Example

**Original CDC Message (from Debezium):**
```json
{
  "op": "u",
  "before": {"Id": 123, "Name": "Old Name", "Code": "ABC"},
  "after": {"Id": 123, "Name": "New Name", "Code": "ABC"},
  "source": {
    "db": "EXAMPLE_DB",
    "table": "ExampleTable",
    "ts_ms": 1704067200000
  }
}
```

**After ExtractAuditRecordState Transformation:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "topic": "example_data_catalog_EXAMPLE_DB_dbo_ExampleTable",
  "partition": 0,
  "change_type": "UPDATE",
  "change_time": 1704067200000,
  "sync_date": 1704067201000,
  "primary_keys": "{\"Id\": 123}",
  "new_values": "{\"Name\": \"New Name\"}",
  "original_values": "{\"Name\": \"Old Name\"}",
  "field_includes": "{\"Code\": \"ABC\"}",
  "database": "EXAMPLE_DB",
  "table": "ExampleTable"
}
```

## 💡 Important Audit Fields Recommendation

Based on common database audit patterns, the following fields are recommended for inclusion in `transforms.audit.include`:

- **Business Identifiers**: `Code`, `AutoCode` - Unique business codes
- **Organizational Context**: `MethodId`, `BranchId`, `BillTypeId`, `CenterId` - Organizational hierarchy and classification

### ❓ Why NOT include audit user fields (CrtBy, UptBy, CreateById, etc.)?
- **If these fields change**: They are automatically captured in `new_values` and `original_values` (no need to include)
- **If these fields don't change**: Including them in `field_includes` has no audit meaning (they're just static metadata)
- **For INSERT operations**: All fields including audit fields are automatically captured in `new_values`
- **For UPDATE operations**: Only changed fields go to `new_values`/`original_values`; unchanged fields in the include list go to `field_includes` (which is only useful for business context fields, not audit fields)

### ⏰ Note on Timestamp Fields
- Do NOT include `CrtDate`, `UptDate`, `CreateDate`, `UpdateDate` in the include list
- The transformation already captures accurate timestamps:
  - `change_time`: Real CDC change timestamp from `source.ts_ms` (when the change actually occurred in the database)
  - `sync_date`: When the record was processed/synced by Kafka Connect
- Database timestamp fields (`CrtDate`/`UptDate`) can be manually set by users and may not reflect the actual change time, so they are not reliable for audit purposes

### 📝 Note on Field Naming Conventions
- Different tables may use different naming conventions for business fields:
  - `Code` vs `AutoCode` vs other identifier fields
  - `MethodId` vs `Method` vs other classification fields
- Include all relevant business/organizational context fields that exist in your database schema

> **📌 Important:** The `include` list specifies fields that will be tracked in `field_includes` for UPDATE operations (unchanged fields). All changed fields are automatically captured in `new_values` and `original_values`, regardless of the include list. The `exclude` list takes precedence and removes fields from processing entirely.

## 📋 Requirements

- ☕ Java 8 or higher
- 🔌 Apache Kafka Connect 2.1.1 or higher
- 📦 Jackson 2.13.4 (included in dependencies)

## 📦 Dependencies

- `org.apache.kafka:connect-api:2.1.1` _(provided)_
- `org.apache.kafka:connect-transforms:2.1.1`
- `com.fasterxml.jackson.core:jackson-core:2.13.4`
- `com.fasterxml.jackson.core:jackson-databind:2.13.4.2`
- `com.fasterxml.jackson.core:jackson-annotations:2.13.4`

## ⚡ Performance Optimizations

Both transformations include several performance optimizations:

- 🚀 **Schema Caching**: Schemas are cached during initialization to avoid repeated creation
- 🔄 **ObjectMapper Reuse**: ObjectMapper instances are created once and reused
- 🔒 **Thread-Safe Date Formatting**: Uses thread-safe `DateTimeFormatter` instead of `SimpleDateFormat`
- 📝 **Pattern Compilation**: Regex patterns are compiled once as static constants and reused
- 🎯 **Efficient Field Filtering**: Optimized include/exclude field filtering logic with HashSet lookups
- ✅ **Field Existence Check**: Safely checks field existence before accessing to avoid exceptions

## 🛡️ Error Handling

Both transformations include comprehensive error handling and logging:

- ⚠️ **WARN level**: For non-critical errors (fallback to default values, missing optional fields)
- ❌ **ERROR level**: For critical errors (with full exception details and context)
- 🔍 **Null Safety**: Proper null checks for key, source, before, after fields
- ✅ **Field Existence Check**: Verifies field exists in schema before accessing to prevent `DataException`
- 🔄 **Graceful Degradation**: Continues processing when possible, uses default values when needed
- 🔙 **Operation Type Fallback**: Falls back to before/after logic if `op` field is not available

## 📝 Logging

All transformations use SLF4J for logging. Log levels:

- 🔍 `DEBUG`: Configuration details and cache operations
- ⚠️ `WARN`: Non-critical errors, fallback scenarios, missing optional fields
- ❌ `ERROR`: Critical errors with full context (topic, partition, exception details)

## 🧪 Testing

The project includes unit tests that validate transformation logic using sample data from `templates/` directory:

```bash
mvn test
```

**Test cases verify:**
- ✅ Operation type detection (INSERT, UPDATE, DELETE) from `op` field
- ✅ Empty field handling (returns `"{}"` instead of `""`)
- ✅ Database and table extraction from source metadata
- ✅ Template file validation

**Sample test data files:**
- 📄 `templates/cdc-insert-sample.json` - INSERT operation example
- 📄 `templates/cdc-update-sample.json` - UPDATE operation example
- 📄 `templates/cdc-delete-sample.json` - DELETE operation example

## ✨ Best Practices

1. ✅ **Field Selection**: Only include business context fields in `include` list, not audit user fields
2. 🔒 **Exclude Sensitive Data**: Use `exclude` list to remove sensitive fields like passwords, tokens
3. 🌍 **Timezone Configuration**: Set appropriate timezone for date/time field conversion
4. 📊 **Partitioning Strategy**: ClickHouse table is partitioned by month and table name for optimal query performance
5. ⏰ **TTL Configuration**: Configure appropriate TTL based on compliance requirements (default: 6 months)
6. 📈 **Monitoring**: Monitor connector lag and error rates in Kafka Connect logs

## 🔧 Troubleshooting

### Common Issues

**❌ Issue**: Transformation fails with `DataException`
- **✅ Solution**: Check if all required fields exist in the schema. The transformation includes field existence checks, but verify your Debezium connector configuration.

**❌ Issue**: Empty `new_values` or `original_values` for UPDATE operations
- **✅ Solution**: This is expected if no fields changed. Check `field_includes` for unchanged fields that are in the include list.

**❌ Issue**: Timestamps are incorrect
- **✅ Solution**: Verify timezone configuration and ensure `source.ts_ms` is available in Debezium messages.

**❌ Issue**: Missing fields in output
- **✅ Solution**: Check exclude/include lists. Exclude takes precedence over include.

## Changelog

### Version 2.0.1

**ExtractAuditRecordState Improvements:**
- ✅ **Operation Type Detection**: Now uses `op` field from Debezium envelope for accurate operation type identification
- ✅ **Empty Field Consistency**: Empty JSON fields (NewValues, OriginalValues, FieldIncludes) now return `"{}"` instead of `""`
- ✅ **Field Existence Check**: Safely checks if fields exist in schema before accessing to prevent `DataException`
- ✅ **Null Safety**: Enhanced null handling for key, source, before, and after fields
- ✅ **Error Handling**: Improved error messages with context (topic, partition)
- ✅ **Performance**: Schema and ObjectMapper caching for better performance
- ✅ **Thread Safety**: Uses thread-safe `DateTimeFormatter` instead of `SimpleDateFormat`

**Documentation & Examples:**
- ✅ **Multi-Database Support**: Updated documentation to clarify support for all Debezium connectors (SQL Server, MySQL, PostgreSQL, Oracle, MongoDB, etc.)
- ✅ **Complete Examples**: Added comprehensive connector examples for both SMTs:
  - `extractaudit-clickhouse-sink-example.json` - ExtractAuditRecordState usage
  - `insertfield-clickhouse-metadata-example.json` - InsertField with Kafka metadata
  - `insertfield-clickhouse-static-example.json` - InsertField with static values
  - `insertfield-clickhouse-combined-example.json` - InsertField combined usage
  - `debezium-sqlserver-source-example.json` - Debezium source connector example
- ✅ **End-to-End Flow**: Added complete architecture diagram and data flow documentation
- ✅ **Best Practices**: Added troubleshooting guide and best practices section
- ✅ **Sample Data**: Added sanitized CDC sample files (INSERT, UPDATE, DELETE operations)

## License

Copyright © 2025 Tuyn Doan (doanvantuyn@gmail.com)

Licensed under the Apache License, Version 2.0
