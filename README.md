# DuckDB Sleep Extension

This extension adds sleep functionality to DuckDB, introducing `sleep`, `sleep_for`, and `sleep_until` scalar functions. These functions are useful for testing and simulation.

## Features

- **`sleep(seconds)`**: Pauses execution for the specified number of seconds (supports fractional seconds).
- **`sleep_for(interval)`**: Pauses execution for a specified `INTERVAL` (e.g., `INTERVAL 5 SECONDS`).
- **`sleep_until(timestamp)`**: Pauses execution until the specified `TIMESTAMP` is reached.
- **Interruption Support**: Long sleeps can be interrupted (e.g., via CTRL+C in the CLI).

## Usage

```sql
-- Sleep for 1.5 seconds
SELECT sleep(1.5);

-- Sleep for 500 milliseconds using interval
SELECT sleep_for(INTERVAL 500 MILLISECONDS);

-- Sleep until a specific time
SELECT sleep_until('2025-01-01 12:00:00'::TIMESTAMP);
```

## Building

To build the extension:

```sh
make
```

## Testing

To run the tests:

```sh
make test
```
