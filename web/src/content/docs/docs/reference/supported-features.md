---
title: Supported Features
description: Current implementation status for all PostgreSQL objects and operations in pgmt.
---

Current implementation status for all PostgreSQL objects and operations in pgmt.

## Legend

- ✅ **Fully Implemented** - Production ready, comprehensive test coverage
- 🚧 **Partially Implemented** - Basic functionality works, some limitations or missing features
- ❌ **Planned** - Not yet implemented, planned for future releases
- 📋 **Under Consideration** - Potential future feature, not committed

## Database Objects

### Schemas

- ✅ **CREATE SCHEMA** - Full support with comments
- ✅ **DROP SCHEMA** - Safe removal with dependency checking
- ✅ **Comments** - SET/DROP COMMENT ON SCHEMA

### Tables

- ✅ **CREATE TABLE** - Full column definition support
- ✅ **DROP TABLE** - Safe removal with dependency checking
- ✅ **ALTER TABLE ADD COLUMN** - All column types and constraints
- ✅ **ALTER TABLE DROP COLUMN** - Safe column removal
- ✅ **ALTER TABLE ALTER COLUMN** - Type changes, nullability, defaults
- ✅ **Column comments** - SET/DROP COMMENT ON COLUMN
- ✅ **Table comments** - SET/DROP COMMENT ON TABLE
- ✅ **Dependency tracking** - Tables referencing custom types, other tables

### Views

- ✅ **CREATE VIEW** - Standard views
- ✅ **DROP VIEW** - Safe removal with dependency checking
- ✅ **CREATE OR REPLACE VIEW** - View definition updates
- ✅ **View comments** - SET/DROP COMMENT ON VIEW
- ✅ **Dependency tracking** - Views depending on tables, views, types, functions
- ❌ **Materialized views** - Not yet supported (CREATE MATERIALIZED VIEW)
- ❌ **Materialized view refresh** - Planned for future releases

### Custom Types

#### ENUM Types

- ✅ **CREATE TYPE (ENUM)** - Full enum definition support
- ✅ **ALTER TYPE ADD VALUE** - Add new enum values (append only)
- ✅ **DROP TYPE** - Safe removal with dependency checking
- ✅ **Type comments** - SET/DROP COMMENT ON TYPE
- ❌ **ALTER TYPE RENAME VALUE** - PostgreSQL limitation, not supported
- ❌ **ALTER TYPE DROP VALUE** - PostgreSQL limitation, not supported

#### DOMAIN Types

- ✅ **CREATE DOMAIN** - Domain types with constraints
- ✅ **DROP DOMAIN** - Safe removal with dependency checking
- ✅ **Domain comments** - SET/DROP COMMENT ON DOMAIN
- 🚧 **ALTER DOMAIN** - Basic support, some constraint operations missing

#### COMPOSITE Types

- ✅ **CREATE TYPE (COMPOSITE)** - Composite type definition
- ✅ **DROP TYPE** - Safe removal with dependency checking
- ✅ **Composite type comments** - SET/DROP COMMENT ON TYPE
- ❌ **ALTER TYPE ADD/DROP/ALTER ATTRIBUTE** - Planned for future

#### Advanced Types

- ✅ **Range types** - NUMRANGE, TSRANGE, DATERANGE, etc.
- ❌ **Array type operations** - Advanced array manipulations
- 📋 **Custom operators** - Under consideration

### Functions and Procedures

#### Core Function Support

- ✅ **CREATE FUNCTION** - SQL, PL/pgSQL, and other languages
- ✅ **CREATE PROCEDURE** - Stored procedures
- ✅ **CREATE AGGREGATE** - Custom aggregate functions
- ✅ **DROP FUNCTION/PROCEDURE** - Safe removal with overload handling
- ✅ **CREATE OR REPLACE FUNCTION** - Function updates
- ✅ **Function overloading** - Same name, different signatures
- ✅ **Function comments** - SET/DROP COMMENT ON FUNCTION

#### Function Attributes

- ✅ **IMMUTABLE/STABLE/VOLATILE** - Function volatility
- ✅ **STRICT** - Null input handling
- ✅ **SECURITY DEFINER/INVOKER** - Execution context
- ✅ **PARALLEL SAFE/UNSAFE/RESTRICTED** - Parallel execution
- ✅ **LANGUAGE specification** - SQL, PL/pgSQL, etc.

#### Dependency Tracking

- ✅ **Signature-level dependencies** - Parameters and return types
- ✅ **Custom types in signatures** - Functions using custom types
- 🚧 **Function body dependencies** - Limited by PostgreSQL catalog limitations

#### Advanced Function Features

- ❌ **OUT/INOUT/VARIADIC parameters** - Planned for future
- ❌ **Function parameter default values** - Planned for future
- ❌ **ALTER FUNCTION** - Changing existing function properties

### Sequences

- ✅ **CREATE SEQUENCE** - Full sequence definition support
- ✅ **DROP SEQUENCE** - Safe removal with dependency checking
- ✅ **ALTER SEQUENCE OWNED BY** - SERIAL column integration
- ✅ **Sequence comments** - SET/DROP COMMENT ON SEQUENCE
- ✅ **Automatic dependency tracking** - Tables with SERIAL columns
- 🚧 **ALTER SEQUENCE** - Basic support, some options missing

### Indexes

#### Index Types

- ✅ **B-tree indexes** - Standard btree indexes
- ✅ **Hash indexes** - Hash-based indexes
- ✅ **GiST indexes** - Generalized Search Trees
- ✅ **GIN indexes** - Generalized Inverted Indexes
- ✅ **SP-GiST indexes** - Space-Partitioned GiST
- ✅ **BRIN indexes** - Block Range Indexes

#### Index Features

- ✅ **Unique indexes** - Uniqueness constraints via indexes
- ✅ **Partial indexes** - WHERE clause conditions
- ✅ **Expression indexes** - Function-based indexes
- ✅ **Covering indexes** - INCLUDE columns
- ✅ **Column ordering** - ASC/DESC and NULLS FIRST/LAST
- ✅ **Custom operator classes** - Specialized sorting/searching
- ✅ **Custom collations** - Locale-specific sorting
- ✅ **Storage parameters** - fillfactor, etc.
- ✅ **Custom tablespaces** - Storage location specification
- ✅ **Index comments** - SET/DROP COMMENT ON INDEX

#### Dependency Tracking

- ✅ **Table dependencies** - Indexes depend on their tables
- ✅ **Function dependencies** - Expression indexes depend on functions

### Constraints

#### Primary Keys

- ✅ **Single-column primary keys** - Standard primary key support
- ✅ **Compound primary keys** - Multi-column primary keys
- ✅ **Primary key comments** - Managed via table catalog

#### Unique Constraints

- ✅ **CREATE/DROP UNIQUE** - Unique constraint management
- ✅ **Multi-column unique constraints** - Compound uniqueness
- ✅ **Unique constraint comments** - SET/DROP COMMENT

#### Foreign Key Constraints

- ✅ **Basic foreign keys** - Single and multi-column
- ✅ **ON DELETE actions** - CASCADE, RESTRICT, SET NULL, SET DEFAULT
- ✅ **ON UPDATE actions** - CASCADE, RESTRICT, SET NULL, SET DEFAULT
- ✅ **DEFERRABLE constraints** - Deferred constraint checking
- ✅ **INITIALLY DEFERRED** - Default deferral behavior
- ✅ **Multi-column foreign keys** - Compound foreign key support
- ✅ **Foreign key comments** - SET/DROP COMMENT
- ✅ **Automatic dependency tracking** - Foreign key relationships

#### Check Constraints

- ✅ **CREATE/DROP CHECK** - Check constraint management
- ✅ **Complex expressions** - Multi-column and function-based checks
- ✅ **Check constraint comments** - SET/DROP COMMENT

#### Exclusion Constraints

- ✅ **EXCLUDE constraints** - Advanced exclusion patterns
- ✅ **GiST/SP-GiST methods** - Geometric and range exclusions
- ✅ **B-tree exclusion** - Traditional exclusion patterns
- ✅ **Multiple columns** - Multi-column exclusion patterns
- ✅ **Exclusion operators** - Custom exclusion logic
- ✅ **Partial exclusion** - WHERE clause conditions
- ✅ **Exclusion constraint comments** - SET/DROP COMMENT

#### Constraint Modification

- ✅ **Drop/Recreate pattern** - Safe constraint modifications
- ✅ **Dependency-aware ordering** - Proper constraint application order

### Triggers

- ✅ **CREATE TRIGGER** - Full trigger creation support
- ✅ **DROP TRIGGER** - Safe trigger removal
- ✅ **CREATE OR REPLACE TRIGGER** - Trigger updates (drop/recreate)

#### Trigger Timing

- ✅ **BEFORE triggers** - Pre-operation execution
- ✅ **AFTER triggers** - Post-operation execution
- ✅ **INSTEAD OF triggers** - View operation replacement

#### Trigger Events

- ✅ **INSERT triggers** - Row insertion triggers
- ✅ **UPDATE triggers** - Row modification triggers
- ✅ **DELETE triggers** - Row deletion triggers
- ✅ **TRUNCATE triggers** - Table truncation triggers
- ✅ **Column-specific UPDATE** - UPDATE OF column_list

#### Trigger Features

- ✅ **FOR EACH ROW** - Row-level triggers
- ✅ **FOR EACH STATEMENT** - Statement-level triggers
- ✅ **WHEN conditions** - Conditional trigger execution
- ✅ **Transition tables** - OLD TABLE, NEW TABLE references
- ✅ **Trigger comments** - SET/DROP COMMENT ON TRIGGER

#### Dependency Tracking

- ✅ **Table dependencies** - Triggers depend on tables
- ✅ **Function dependencies** - Triggers depend on trigger functions
- ✅ **Reliable recreation** - Uses pg_get_triggerdef() for accuracy

### Extensions

- ✅ **CREATE EXTENSION** - Extension installation
- ✅ **DROP EXTENSION** - Extension removal
- ✅ **IF NOT EXISTS/IF EXISTS** - Safe installation/removal
- ✅ **Schema placement** - CREATE EXTENSION ... SCHEMA
- ✅ **Extension comments** - SET/DROP COMMENT ON EXTENSION

#### Extension Integration

- ✅ **Object filtering** - Extension-owned objects excluded from management
- ✅ **Clean shadow operations** - Avoids extension conflicts in shadow DB
- ✅ **Dependency tracking** - Proper migration ordering with extensions
- ✅ **Grant separation** - Extension permissions managed separately

### Comments

- ✅ **All supported objects** - Comments on tables, views, functions, etc.
- ✅ **SET COMMENT operations** - Adding/updating comments
- ✅ **DROP COMMENT operations** - Removing comments
- ✅ **Comment-only migrations** - Migrations that only change comments

## Permissions and Security

### Grants and Privileges

- ✅ **GRANT/REVOKE on tables** - Table-level permissions
- ✅ **GRANT/REVOKE on views** - View-level permissions
- ✅ **GRANT/REVOKE on schemas** - Schema-level permissions
- ✅ **GRANT/REVOKE on functions** - Function execution permissions
- ✅ **GRANT/REVOKE on sequences** - Sequence usage permissions
- ✅ **GRANT/REVOKE on types** - Custom type usage permissions
- ✅ **WITH GRANT OPTION** - Privilege delegation
- ✅ **PUBLIC grants** - Public access permissions
- ✅ **Dependency ordering** - Grants applied after object creation

### Role Management

- ❌ **CREATE/DROP ROLE** - Must be managed externally
- ✅ **Role references** - Grants can reference existing roles
- ✅ **Testing patterns** - Standard roles for development/testing

### Row-Level Security

- ❌ **RLS policies** - Planned for future releases
- ❌ **ENABLE/DISABLE RLS** - Planned for future releases

## Migration Features

### Migration Generation

- ✅ **Automatic drift detection** - Compare schema files to database state
- ✅ **Safe vs destructive classification** - Clear migration safety indicators
- ✅ **Dependency-ordered migrations** - Proper object creation/modification order
- ✅ **Explicit migration review** - Generated SQL files for human review
- ✅ **Extension ordering** - Extensions created before dependent objects

### Migration Management

- ✅ **Version-specific updates** - Update any migration, not just the latest
- ✅ **Automatic renumbering** - Older migrations get new timestamps when updated
- ✅ **Partial version matching** - Update migrations using partial version numbers
- ✅ **Backup on update** - Optional .bak file creation when updating migrations
- ✅ **Dry-run previews** - Preview migration changes without modifying files

### Migration Application

- ✅ **Chronological application** - Migrations applied in version order
- ✅ **Checksum validation** - Detects modified migration files
- ✅ **Partial application** - Apply migrations up to specific version
- ✅ **Rollback support** - Rollback-compatible migration patterns
- ✅ **Migration tracking** - pgmt_migrations table for state management
- ✅ **Configurable tracking table** - Customize migration table name and schema
- ✅ **Multi-section migrations** - Split migrations into pre-deploy, deploy, post-deploy sections

### Migration Safety

- ✅ **Shadow database isolation** - Safe operations in isolated environment
- ✅ **Dry-run support** - Preview changes without applying
- ✅ **Execution modes** - safe_only, confirm_all, force_all
- ✅ **Data preservation** - Schema-only changes, data migrations external

### Baseline Management

- ✅ **On-demand baselines** - Create baselines when needed (--create-baseline flag)
- ✅ **Baseline validation** - Ensure consistency with migration chain
- ✅ **Baseline cleanup** - Remove old baselines to save space
- ✅ **Smart reconstruction** - Rebuild state from migration chain when no baselines
- ✅ **Selective baseline creation** - Baselines only created when explicitly requested

## Development Features

### Schema Organization

- ✅ **Multi-file schemas** - Organize schema across multiple SQL files
- ✅ **Dependency syntax** - `-- require:` for explicit file dependencies
- ✅ **Alphabetical ordering** - Predictable file loading order
- ✅ **Circular dependency detection** - Prevent invalid dependency cycles
- ✅ **File validation** - Syntax and dependency validation

### Development Workflow

- ✅ **Watch mode** - Auto-apply schema changes during development
- ✅ **Apply command** - Test schema changes immediately
- ✅ **Drift detection** - Identify differences between dev and schema
- ✅ **Shadow database** - Isolated environment for safe operations

### Configuration

- ✅ **YAML configuration** - Comprehensive configuration system
- ✅ **CLI overrides** - Command-line configuration precedence
- ✅ **Environment variables** - Sensitive data in environment
- ✅ **Object filtering** - Include/exclude patterns for schemas and tables

## Operational Features

### Validation and Drift Detection

- ✅ **Schema validation** - Verify database matches expected state
- ✅ **Drift detection** - Identify unauthorized schema changes
- ✅ **CI/CD integration** - Automated validation in pipelines

### Database Support

- ✅ **PostgreSQL 13+** - Comprehensive support for modern PostgreSQL
- ✅ **Cloud PostgreSQL** - Works with all major cloud providers
- ✅ **Docker integration** - Development database automation
- 🚧 **Docker shadow databases** - Planned for enhanced isolation

### Performance and Scale

- ✅ **Large schema support** - Handles complex enterprise schemas
- ✅ **Efficient catalog queries** - Optimized PostgreSQL system catalog usage
- ✅ **Deterministic ordering** - Consistent results across runs
- 📋 **Parallel operations** - Under consideration for performance

## Limitations and Known Issues

### PostgreSQL Limitations

- **Function body dependencies**: PostgreSQL doesn't track dependencies within SQL function bodies
- **ENUM value reordering**: PostgreSQL doesn't support reordering or removing enum values
- **Complex type modifications**: Limited support for modifying composite type attributes

### pgmt Design Limitations

- **Data migrations**: Focus on schema-only changes, data transformations handled externally
- **Role management**: Roles must be created/managed outside pgmt
- **Zero-downtime deployments**: Depends on specific changes, not guaranteed

### Known Edge Cases

- **Extension schema dependencies**: Extensions created in custom schemas may not order correctly (schema before extension)
- **Extension CASCADE drops**: Dropping extensions with dependent objects requires manual CASCADE handling
- **Complex circular dependencies**: Some circular dependency patterns may require manual resolution

### Performance Considerations

- **Large migration chains**: Very long migration chains may impact reconstruction performance
- **Complex schemas**: Schemas with thousands of objects may experience slower operations
- **Shadow database overhead**: Shadow operations require additional database resources
