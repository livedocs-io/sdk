# Test for apply_filters Function

This directory contains a test for the `apply_filters` function in the Livedocs project.

## Bug Fixed

The `apply_filters` function had a critical bug where it would return after applying the first filter condition instead of applying all filters sequentially. This has been fixed by:

1. Using `result_df` instead of `df` in all filter operations
2. Assigning the filtered result back to `result_df` instead of returning immediately
3. Continuing to the next filter if an error occurs instead of returning the original DataFrame

## Test File

`test_apply_filters.py` - Comprehensive tests for the `apply_filters` function that cover:

- Basic functionality for all operators (eq, gt, lt, gte, lte, contains, startsWith, endsWith, etc.)
- Multiple filters applied sequentially
- Edge cases like empty filters, non-existent columns, and error handling

The test imports the function directly from the original module (`livedocs.utils.table_helpers`).

## How to Run the Test

You can run the test using the Python unittest framework. From the project root directory:

```bash
# Install required dependencies
pip install polars IPython

# Set required environment variables (if needed)
export CORE_BASE_URL=your_base_url_value

# Run the test file
python -m unittest tests/test_apply_filters.py

# Run a specific test case
python -m unittest tests.test_apply_filters.TestApplyFilters.test_multiple_filters
```

## Requirements

The test requires:

- `polars` library for DataFrame operations
- `IPython` and other dependencies of the Livedocs module
- Environment variables required by the Livedocs module

All tests should now pass with the fixed implementation.
