# TODO: Code Issues to Fix

## 🔴 CRITICAL (Fix Immediately)

### Security
- [ ] Remove hardcoded JWT token in `tests/test.py:22` and rotate credentials
- [ ] Fix SQL injection in `livedocs/manager/duckdb.py:15-17` (unsanitized file_search_path)
- [ ] Fix CORS misconfiguration in `tests/vega-api.py:7` (allows all origins)
- [ ] Fix SQL injection risks in datasources (databricks.py:58,122,393-404, clickhouse.py:165,187-191, snowflake.py:202-203,225-226)

### Import Errors (Code Won't Run)
- [ ] Fix `livedocs/utils/lib/vega.py:19-28` - imports 8 functions from wrong module (should be chart_helpers.py)
- [ ] Add missing `List` import in `livedocs/types.py:339`
- [ ] Fix type signature in `livedocs/utils/lib/cache.py:96` (datasource should be ElementDataSource, not str)

### Connection Leaks
- [ ] Add connection.close() in `livedocs/datasources/snowflake.py:81-110` write() method
- [ ] Add cursor.close() in `livedocs/datasources/snowflake.py:48-58` read() method
- [ ] Implement try-finally blocks in all datasource connectors
- [ ] Implement proper teardown() methods (currently empty in all connectors)

### Logic Errors
- [ ] Fix return type in `livedocs/main.py:908,972` - should raise exception not return string
- [ ] Fix boolean logic in `livedocs/utils/cells/chart_helpers.py:80-84` - change `or` to `and`
- [ ] Fix Polars comparison in `livedocs/utils/cells/table_helpers.py:281-286` - use `== True` not `is True`
- [ ] Fix column name in `livedocs/utils/cells/table_helpers.py:509` - change "counts" to "count"

### Test Failures
- [ ] Update `tests/test_apply_filters.py:10` to import `process_conditions` instead of non-existent `apply_filters`
- [ ] Fix `tests/test_postgres_integration.py:17,21` environment variable logic

---

## ⚠️ HIGH PRIORITY (Fix Soon)

### Thread Safety
- [ ] Add threading.Lock to DuckDB singleton in `livedocs/manager/duckdb.py:7-29`
- [ ] Fix cache iteration race condition in `livedocs/utils/lib/cache.py:173-176`
- [ ] Add try-finally for lock release in `livedocs/utils/lib/cache.py:162-167`

### Connection Management
- [ ] Add `super().__init__(mock)` calls to motherduck.py:23-27, postgres.py:29-32, snowflake.py:26-29
- [ ] Add connection timeout configuration to all datasource connectors
- [ ] Implement connection pooling (currently creates new connection per operation)

### Performance - Critical
- [ ] Replace INSERT VALUES with COPY INTO in `livedocs/datasources/databricks.py:392-404`
- [ ] Remove pandas conversion overhead in snowflake.py:315, clickhouse.py:250-251, postgres.py:655-657
- [ ] Fix row-by-row iteration in `livedocs/datasources/postgres.py:655-657` - use bulk operations
- [ ] Remove global lock from cache uploads in `livedocs/utils/lib/cache.py:139`

### Data Integrity
- [ ] Wrap TRUNCATE+INSERT in transactions (clickhouse.py:254-255, databricks.py:389, snowflake.py:319)
- [ ] Add transaction rollback handling in `livedocs/datasources/postgres.py:252-297`

---

## 📋 MEDIUM PRIORITY

### Remove Dead Code (~494 lines = 17% of codebase)
- [ ] Remove `process_bigquery_schema()` and `_map_bigquery_type()` from bigquery.py:340-386 (~47 lines)
- [ ] Remove `process_clickhouse_schema()` from clickhouse.py:352-468 (~117 lines)
- [ ] Remove `process_databricks_schema()` and `_normalize_databricks_type()` from databricks.py:425-478 (~54 lines)
- [ ] Remove `process_postgres_schema()` and `_map_postgres_type()` from postgres.py:704-749 (~46 lines)
- [ ] Remove `process_snowflake_schema()` and `_map_snowflake_type()` from snowflake.py:361-447 (~87 lines)
- [ ] Remove unused cache methods: `pop()` (lines 114-128), `upload_artifacts()` (lines 169-177), `_write_to_parquet()` (lines 130-168) from cache.py (~85 lines)
- [ ] Remove empty file `livedocs/manager/datasources.py`
- [ ] Remove unused `_get_dataframe_schema()` from main.py:1066-1123 (~58 lines)

### Error Handling
- [ ] Replace silent error swallowing in chart_helpers.py:319-320,356-357,388-389,403-404,433-434,496-497
- [ ] Replace bare except clauses in chart_helpers.py:424-428, postgres.py:295-297
- [ ] Replace generic Exception catching in bigquery.py:165-179, clickhouse.py:164-175

### DRY Violations
- [ ] Extract `_get_column_type()` to common utility (duplicated in chart_helpers.py:442-498 and table_helpers.py:357-413)
- [ ] Extract Snowflake query building from main.py:560-574 and 668-683 to helper function
- [ ] Extract table name parsing in snowflake.py:199-206 and 222-230 to method

### Performance Optimizations
- [ ] Implement schema caching in query methods (currently computed every time)
- [ ] Precompile regex patterns in chart_helpers.py:476-493 and table_helpers.py:393-412
- [ ] Add query result pagination in bigquery.py:51-62 (currently loads entire result)
- [ ] Fix template caching in main.py:108-110 (may be ineffective)
- [ ] Optimize DataFrame conversions in main.py:383,515-518,703
- [ ] Move gzip compression out of hot path in main.py:630,99,780
- [ ] Fix inefficient date parsing in common.py:159-166 (tries 12 formats sequentially)
- [ ] Fix cached signed URLs issue in internals.py:104 (URLs expire but stay cached)

### Type System
- [ ] Fix Schema type field mismatch in types.py:338 (`type` vs `livedocs_type`)
- [ ] Fix model_validator mode in types.py:217,242
- [ ] Complete `__all__` exports in types.py:539-552

---

## 🧹 LOW PRIORITY (Code Cleanup)

### Code Style
- [ ] Remove unnecessary `_ = span.finish()` assignments (result is always None)
- [ ] Remove unnecessary assignments in main.py:164,194,209
- [ ] Replace triple-quoted section dividers with regular comments in main.py:148-152,322-326,806-810
- [ ] Remove unnecessary pass statements in main.py:415,424,433,442,451
- [ ] Consolidate duplicate imports in main.py:55,64
- [ ] Remove unused constant `_LIVEDOCS_PROTECTED_VARS` in chart_helpers.py:44
- [ ] Remove unused parameters: `schema` in chart_helpers.py:188, `compare_format` in single_value_helpers.py:56
- [ ] Fix unsafe `globals()` usage in single_value_helpers.py:157
- [ ] Replace OrderedDict with dict in postgres.py:5,679 (Python 3.7+ maintains order)
- [ ] Remove commented debug code in duckdb.py:19-24
- [ ] Rename or move `tests/vega-api.py` (it's an API server, not a test)

### Test Improvements
- [ ] Rewrite test.py as proper test suite (currently no assertions, 89% commented code)
- [ ] Add missing test coverage in test_apply_filters.py (gt, lt, gte, lte, before, after, true, false, unique operators)
- [ ] Fix test_livedocs_standalone.py accessing private attributes (lines 94-95,100)
- [ ] Add connection pooling to test_postgres_integration.py:107
- [ ] Add tests for append mode, concurrent writes, NULL handling, schema mismatches
- [ ] Move vega-api.py out of tests/ to /dev_tools/ or /scripts/

### Documentation
- [ ] Document singleton parameter behavior in duckdb.py:7
- [ ] Add type annotation to `_instance` in duckdb.py:5
- [ ] Add docstrings to all public methods
- [ ] Document thread safety guarantees
- [ ] Document connection lifecycle expectations

---

## Summary Stats

- **Total Files**: 29 Python files
- **Total Lines**: ~2,900
- **Dead Code**: ~494 lines (17%)
- **Critical Issues**: 15
- **High Priority**: 32
- **Medium Priority**: 41
- **Low Priority**: 27

## Categories

- Security: 5 issues
- Connection Leaks: 8 issues
- Thread Safety: 3 issues
- Type Errors: 4 issues
- Logic Errors: 5 issues
- Performance: 18 issues
- Dead Code: 10 issues
- Test Issues: 12 issues
- Code Quality: 50+ issues
