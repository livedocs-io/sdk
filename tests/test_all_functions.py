"""
Test script that imports Livedocs class and calls all top-level functions with fake parameters.
This script is useful for testing the interface and ensuring all methods are callable.
"""

import json
import os
import tempfile

import polars as pl

from livedocs import Livedocs, LivedocsConfig
from livedocs.manager.credentials import StaticCredentialStore
from livedocs.types import Credentials
from livedocs.utils.lib.cache import QueryCache

# ANSI color codes
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"
BOLD = "\033[1m"


def setup_test_environment():
    """Set up a temporary directory and environment for testing."""
    temp_dir = tempfile.mkdtemp()
    os.environ["LIVEDOCS_FILES_PATH"] = temp_dir
    _ = os.environ.pop("VMLIB_SENTRY_DSN", None)
    return temp_dir


def create_mock_credentials():
    """Create a mock credentials bundle for testing."""
    return Credentials(
        workspace_id="test-workspace-id",
        workspace_secrets={},
        databases={},
        built_in_vars={},
    )


def create_test_dataframe():
    """Create a test DataFrame for testing."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "score": [85.5, 90.0, 88.5, 92.0, 87.5],
        }
    )


def test_all_functions():
    """Test all top-level Livedocs functions with fake parameters."""
    print("=" * 80)
    print("Testing Livedocs - All Top-Level Functions")
    print("=" * 80)

    # Track test results
    test_results = []

    # Setup
    temp_dir = setup_test_environment()
    credentials_bundle = create_mock_credentials()

    def credential_factory(report_id: str, token: str) -> StaticCredentialStore:  # noqa: ARG001
        return StaticCredentialStore(credentials_bundle)

    def cache_factory(report_id: str, token: str) -> QueryCache:
        return QueryCache(report_id, token)

    config = LivedocsConfig(
        credential_store_factory=credential_factory,
        query_cache_factory=cache_factory,
    )

    # Initialize Livedocs
    print("\n1. Initializing Livedocs...")
    livedocs = Livedocs(config=config)
    try:
        test_report_id = ""
        test_session_token = ""
        livedocs.initialize(test_report_id, test_session_token)
        print(f"   {GREEN}✓ Initialization successful{RESET}")
        test_results.append(("1. Initializing Livedocs", True, None))
    except Exception as e:
        print(f"   {RED}✗ Initialization failed: {e}{RESET}")
        test_results.append(("1. Initializing Livedocs", False, str(e)))
        return

    # Test set_var
    print("\n2. Testing set_var()...")
    try:
        livedocs.set_var("test_key", "test_value")
        print(f"   {GREEN}✓ set_var('test_key', 'test_value') successful{RESET}")
        test_results.append(("2. Testing set_var()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ set_var failed: {error_msg}{RESET}")
        test_results.append(("2. Testing set_var()", False, error_msg))

    # Test get_var
    print("\n3. Testing get_var()...")
    try:
        value = livedocs.get_var("test_key")
        print(f"   {GREEN}✓ get_var('test_key') returned: {value}{RESET}")
        test_results.append(("3. Testing get_var()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ get_var failed: {error_msg}{RESET}")
        test_results.append(("3. Testing get_var()", False, error_msg))

    # Test get_var with run_context
    print("\n4. Testing get_var('run_context')...")
    try:
        run_context = livedocs.get_var("run_context")
        print(f"   {GREEN}✓ get_var('run_context') returned: {run_context}{RESET}")
        test_results.append(("4. Testing get_var('run_context')", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ get_var('run_context') failed: {error_msg}{RESET}")
        test_results.append(("4. Testing get_var('run_context')", False, error_msg))

    # Test unset_var
    print("\n5. Testing unset_var()...")
    try:
        livedocs.unset_var("test_key")
        print(f"   {GREEN}✓ unset_var('test_key') successful{RESET}")
        test_results.append(("5. Testing unset_var()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ unset_var failed: {error_msg}{RESET}")
        test_results.append(("5. Testing unset_var()", False, error_msg))

    # Test clear_vars
    print("\n6. Testing clear_vars()...")
    try:
        livedocs.set_var("temp1", "value1")
        livedocs.set_var("temp2", "value2")
        livedocs.clear_vars()
        print(f"   {GREEN}✓ clear_vars() successful{RESET}")
        test_results.append(("6. Testing clear_vars()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ clear_vars failed: {error_msg}{RESET}")
        test_results.append(("6. Testing clear_vars()", False, error_msg))

    # Test secrets
    print("\n7. Testing secrets()...")
    try:
        secret_value = livedocs.secrets("non_existent_secret", "default_value")
        print(
            f"   {GREEN}✓ secrets('non_existent_secret', 'default_value') returned: {secret_value}{RESET}"
        )
        test_results.append(("7. Testing secrets()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ secrets failed: {error_msg}{RESET}")
        test_results.append(("7. Testing secrets()", False, error_msg))

    # Test download_file (will likely fail without backend, but we test the interface)
    print("\n8. Testing download_file()...")
    try:
        # This will likely fail without a real backend, but we test the interface
        result = livedocs.download_file(
            file_name="sales.csv",
            path=temp_dir,
            force_download=False,
        )
        print(f"   {GREEN}✓ download_file() successful: {result}{RESET}")
        test_results.append(("8. Testing download_file()", True, None))
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(
            f"   {RED}✗ download_file failed (expected without backend): {error_msg}{RESET}"
        )
        test_results.append(("8. Testing download_file()", False, error_msg))

    # Test query
    print("\n9. Testing query()...")
    try:
        test_df = create_test_dataframe()
        datasource = {
            "source_type": "dataframe",
            "dataframe_info": {
                "df_name": "test_df",
                "df_element_id": "test_element_id",
            },
        }
        context = {"test_var": "test_value"}
        df_result, _ = livedocs.query(
            query="SELECT * FROM test_df LIMIT 5",
            str_datasource=json.dumps(datasource),
            context=context,
            dataframe=test_df,
            limit=5,
            offset=0,
            use_cache=True,
        )
        print(f"   {GREEN}✓ query() successful, returned {len(df_result)} rows{RESET}")
        test_results.append(("9. Testing query()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ query failed: {error_msg}{RESET}")
        test_results.append(("9. Testing query()", False, error_msg))

    # Test save_to_database
    print("\n10. Testing save_to_database()...")
    try:
        test_df = create_test_dataframe()
        save_config = {
            "database_type": "postgres",
            "run_settings": ["production", "development"],
            "table_name": "test_table",
            "schema_name": "public",
            "database_connector_id": "test_connector_id",
        }
        # This will likely fail without a real database connection
        _ = livedocs.save_to_database(test_df, json.dumps(save_config))
        print(f"   {GREEN}✓ save_to_database() successful{RESET}")
        test_results.append(("10. Testing save_to_database()", True, None))
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(
            f"   {RED}✗ save_to_database failed (expected without backend): {error_msg}{RESET}"
        )
        test_results.append(("10. Testing save_to_database()", False, error_msg))

    # Test process_raw_text
    print("\n11. Testing process_raw_text()...")
    try:
        src = {"html": "<p>Hello {{ name }}!</p>"}
        context = {"name": "World"}
        _ = livedocs.process_raw_text(json.dumps(src), context)
        print(f"   {GREEN}✓ process_raw_text() successful{RESET}")
        test_results.append(("11. Testing process_raw_text()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ process_raw_text failed: {error_msg}{RESET}")
        test_results.append(("11. Testing process_raw_text()", False, error_msg))

    # Test enrich_prompt
    print("\n12. Testing enrich_prompt()...")
    try:
        system = "You are a helpful assistant. User name: {{ user_name }}"
        user = "Hello, my name is {{ user_name }}"
        context = {"user_name": "Alice"}
        _ = livedocs.enrich_prompt(system, user, context)
        print(f"   {GREEN}✓ enrich_prompt() successful{RESET}")
        test_results.append(("12. Testing enrich_prompt()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ enrich_prompt failed: {error_msg}{RESET}")
        test_results.append(("12. Testing enrich_prompt()", False, error_msg))

    # Test process_dependencies
    print("\n13. Testing process_dependencies()...")
    try:
        test_df = create_test_dataframe()
        dependencies = {
            "my_dataframe": {
                "field_type": "dataframe",
                "element_id": "test_element",
            }
        }
        globals_dict = {"my_dataframe": test_df}
        result = livedocs.process_dependencies(
            json.dumps(dependencies),
            datasource={"source_type": "dataframe"},
            globals_dict=globals_dict,
        )
        print(
            f"   {GREEN}✓ process_dependencies() successful, keys: {list(result.keys())}{RESET}"
        )  # noqa: B006
        test_results.append(("13. Testing process_dependencies()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ process_dependencies failed: {error_msg}{RESET}")
        test_results.append(("13. Testing process_dependencies()", False, error_msg))

    # Test process_single_value
    print("\n14. Testing process_single_value()...")
    try:
        config = {
            "value": "{{ test_value }}",
            "format": {
                "type": "number",
                "decimals": 2,
            },
        }
        context = {"test_value": 1234.5678}
        _ = livedocs.process_single_value(json.dumps(config), context)
        print(f"   {GREEN}✓ process_single_value() successful{RESET}")
        test_results.append(("14. Testing process_single_value()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ process_single_value failed: {error_msg}{RESET}")
        test_results.append(("14. Testing process_single_value()", False, error_msg))

    # Test _get_vega_spec (internal but might be useful to test)
    print("\n15. Testing _get_vega_spec()...")
    try:
        test_df = create_test_dataframe()
        datasource = {
            "source_type": "dataframe",
            "dataframe_info": {
                "df_name": "test_df",
                "df_element_id": "test_element_id",
            },
        }
        settings = {
            "chartSettings": {
                "xAxis": {
                    "field": "age",
                    "sort": "ascending",
                    "temporalFormat": None,
                    "type": "quantitative",
                },
                "yAxis": {
                    "primary": [
                        {
                            "aggregate": "none",
                            "color_by": None,
                            "field": "age",
                            "mark": "grouped_column",
                            "name": "line layer 1",
                            "temporalFormat": None,
                            "type": "quantitative",
                        }
                    ]
                },
            },
            "chartType": "main",
            "colorGroups": {"line layer 1": "#713E5A"},
            "styleSettings": {"mode": "light"},
            "subplots": {},
        }
        _ = livedocs._get_vega_spec(  # noqa: SLF001
            settings_str=json.dumps(settings),
            datasource_str=json.dumps(datasource),
            dataframe=test_df,
            use_cache=True,
        )
        print(f"   {GREEN}✓ _get_vega_spec() successful{RESET}")
        test_results.append(("15. Testing _get_vega_spec()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ _get_vega_spec failed: {error_msg}{RESET}")
        test_results.append(("15. Testing _get_vega_spec()", False, error_msg))

    # Test _get_table_response
    print("\n16. Testing _get_table_response()...")
    try:
        test_df = create_test_dataframe()
        datasource = {
            "source_type": "dataframe",
            "dataframe_info": {
                "df_name": "test_df",
                "df_element_id": "test_element_id",
            },
        }
        # Note: Type annotation says ElementDataSource but implementation uses json.loads
        # Passing JSON string to match actual implementation
        _ = livedocs._get_table_response(  # noqa: SLF001
            str_datasource=json.dumps(datasource),  # type: ignore[arg-type]
            dataframe=test_df,
            limit=5,
            offset=0,
            use_cache=True,
        )
        print(f"   {GREEN}✓ _get_table_response() successful{RESET}")
        test_results.append(("16. Testing _get_table_response()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ _get_table_response failed: {error_msg}{RESET}")
        test_results.append(("16. Testing _get_table_response()", False, error_msg))

    # Test _get_chart_schema
    print("\n17. Testing _get_chart_schema()...")
    try:
        test_df = create_test_dataframe()
        datasource = {
            "source_type": "dataframe",
            "dataframe_info": {
                "df_name": "test_df",
                "df_element_id": "test_element_id",
            },
        }
        _ = livedocs._get_chart_schema(  # noqa: SLF001
            datasource_str=json.dumps(datasource),
            dataframe=test_df,
        )
        print(f"   {GREEN}✓ _get_chart_schema() successful{RESET}")
        test_results.append(("17. Testing _get_chart_schema()", True, None))
    except Exception as e:
        error_msg = str(e)
        print(f"   {RED}✗ _get_chart_schema failed: {error_msg}{RESET}")
        test_results.append(("17. Testing _get_chart_schema()", False, error_msg))

    # Print summary
    print("\n" + "=" * 80)
    print(f"{BOLD}Test Summary{RESET}")
    print("=" * 80)

    passed = [r for r in test_results if r[1]]
    failed = [r for r in test_results if not r[1]]

    print(f"\n{GREEN}{BOLD}✓ Passed: {len(passed)}/{len(test_results)}{RESET}")
    if passed:
        for test_name, _, _ in passed:
            print(f"   {GREEN}✓ {test_name}{RESET}")

    print(f"\n{RED}{BOLD}✗ Failed: {len(failed)}/{len(test_results)}{RESET}")
    if failed:
        for test_name, _, error in failed:
            print(f"   {RED}✗ {test_name}{RESET}")
            if error:
                print(f"      {RED}Error: {error}{RESET}")

    print("\n" + "=" * 80)
    if len(failed) == 0:
        print(f"{GREEN}{BOLD}All tests passed! 🎉{RESET}")
    else:
        print(f"{RED}{BOLD}Some tests failed. See details above.{RESET}")
    print("=" * 80)


if __name__ == "__main__":
    test_all_functions()
