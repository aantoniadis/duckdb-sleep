# Testing the Sleep Extension

This directory contains the test suite for the DuckDB Sleep extension.

## Running Tests

The easiest way to run the tests is using the Makefile in the root directory:

```bash
# Run all tests
make test
```

This will compile the extension and run the standard DuckDB test runner against the test files in `test/sql`.

## Test Files

Tests are located in the `test/sql` directory. They use DuckDB's sqllogictest format.

- `test/sql/sleep.test`: Core functionality tests for `sleep`, `sleep_for`, and `sleep_until`.

## Adding New Tests

To add a new test:
1. Create a new `.test` file in `test/sql`.
2. Follow the sqllogictest format (statement, query, result).

Example of a test case:

```sql
# Test description
query I
SELECT sleep(0.1)
----
NULL
```

## Manual Testing

You can also manually test the extension using the generated DuckDB shell:

```bash
./build/release/duckdb
```

Then run SQL commands interactively:

```sql
SELECT sleep(1.0);
```
