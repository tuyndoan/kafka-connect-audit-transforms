# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.1] - 2025-12-04

### Added
- **ExtractAuditRecordState** SMT: Transform Debezium CDC records into standardized audit format
  - Support for INSERT, UPDATE, DELETE operations
  - Automatic extraction of operation type, timestamp, database, schema, and table information
  - Configurable field exclusion
  - Timezone configuration support
  - Comprehensive error handling

- **InsertField** SMT: Insert Kafka metadata or static values into records
  - Support for inserting topic, partition, offset, timestamp
  - Support for inserting static field values
  - Support for combining metadata and static fields

- Multi-database support: Works with all Debezium-supported databases
  - SQL Server
  - MySQL
  - PostgreSQL
  - Oracle
  - MongoDB
  - And all other Debezium connectors

- Production-ready features:
  - Comprehensive error handling and logging
  - Performance optimizations
  - Unit tests with sample data templates

- GitHub Actions CI/CD workflow for automated builds
- Version extraction from pom.xml for artifact naming
- Code of Conduct (Contributor Covenant 2.1)
- Contributing guidelines (CONTRIBUTING.md)
- Security policy (SECURITY.md)
- GitHub issue templates (bug report, feature request, question)
- Pull request template
- CHANGELOG.md for tracking project changes

### Fixed
- Resolved SLF4J StaticLoggerBinder warning in tests by adding slf4j-simple dependency
- Optimized CI workflow by removing redundant test step

### Changed
- Artifact names now include version number (e.g., `kafka-connect-smt-jar-2.0.1`)

## [Unreleased]

[2.0.1]: https://github.com/tuyndoan/kafka-connect-audit-transforms/releases/tag/v2.0.1
[Unreleased]: https://github.com/tuyndoan/kafka-connect-audit-transforms/compare/v2.0.1...HEAD

