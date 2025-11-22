#define DUCKDB_EXTENSION_MAIN

#include "sleep_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

#include <thread>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <limits>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Constants
//===--------------------------------------------------------------------===//

// Maximum sleep duration in seconds (1 hour) to prevent accidental infinite waits
static constexpr double MAX_SLEEP_SECONDS = 3600.0;

// Interruption check interval in milliseconds
// Similar to PostgreSQL's approach of checking for interrupts periodically
static constexpr int64_t CHECK_INTERVAL_MS = 100;

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

// Check for query cancellation (similar to PostgreSQL's CHECK_FOR_INTERRUPTS)
static void CheckInterruption(ClientContext &context) {
	if (context.interrupted) {
		throw InterruptException();
	}
}

// Core sleep implementation with interruption support
// Inspired by PostgreSQL's pg_usleep but with DuckDB-specific interrupt handling
static void PerformSleep(ClientContext &context, double seconds) {
	// Validate input - check for NaN and Infinity BEFORE any other processing
	if (std::isnan(seconds)) {
		throw InvalidInputException("Sleep duration cannot be NaN");
	}
	if (std::isinf(seconds)) {
		// For infinity, cap at maximum instead of throwing (more user-friendly)
		// This prevents accidental infinite sleeps
		seconds = MAX_SLEEP_SECONDS;
	}

	// Only sleep for positive durations
	if (seconds <= 0) {
		return;
	}

	// Cap at maximum duration for safety (in case value was very large but not infinity)
	if (seconds > MAX_SLEEP_SECONDS) {
		seconds = MAX_SLEEP_SECONDS;
	}

	auto duration = std::chrono::duration<double>(seconds);
	auto end_time = std::chrono::steady_clock::now() + duration;

	// Sleep in small intervals to allow interruption
	// PostgreSQL uses nanosleep which can be interrupted by signals
	// We simulate this by checking context.interrupted periodically
	while (std::chrono::steady_clock::now() < end_time) {
		CheckInterruption(context);

		auto remaining = end_time - std::chrono::steady_clock::now();
		auto check_interval = std::chrono::milliseconds(CHECK_INTERVAL_MS);
		auto sleep_duration = (remaining < check_interval) ? remaining : check_interval;

		if (sleep_duration.count() > 0) {
			std::this_thread::sleep_for(sleep_duration);
		}
	}
}

//===--------------------------------------------------------------------===//
// Function Implementations
//===--------------------------------------------------------------------===//

// sleep(seconds)
// Compatible with PostgreSQL 8.2+
// Delays execution for at least the specified number of seconds
static void SleepFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &seconds_vector = args.data[0];
	seconds_vector.Flatten(args.size());

	auto seconds_data = FlatVector::GetData<double>(seconds_vector);
	auto &validity = FlatVector::Validity(seconds_vector);

	for (idx_t i = 0; i < args.size(); i++) {
		if (!validity.RowIsValid(i)) {
			continue; // Skip NULL values
		}

		PerformSleep(context, seconds_data[i]);
	}

	// Return NULL (void function, PostgreSQL-compatible)
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
}

// sleep_for(interval)
// Compatible with PostgreSQL 9.6+
// Delays execution for at least the specified interval
static void SleepForFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &interval_vector = args.data[0];
	interval_vector.Flatten(args.size());

	auto interval_data = FlatVector::GetData<interval_t>(interval_vector);
	auto &validity = FlatVector::Validity(interval_vector);

	for (idx_t i = 0; i < args.size(); i++) {
		if (!validity.RowIsValid(i)) {
			continue; // Skip NULL values
		}

		interval_t interval = interval_data[i];

		// Convert interval to seconds
		// Note: Months are approximated as 30 days (2,592,000 seconds)
		// This matches PostgreSQL's behavior for sleep_for
		double total_seconds = static_cast<double>(interval.days) * 86400.0 +     // days to seconds
		                       static_cast<double>(interval.months) * 2592000.0 + // months to seconds
		                       static_cast<double>(interval.micros) / 1000000.0;  // microseconds to seconds

		PerformSleep(context, total_seconds);
	}

	// Return NULL (void function, PostgreSQL-compatible)
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
}

// sleep_until(timestamp)
// Compatible with PostgreSQL 9.6+
// Delays execution until at least the specified timestamp
static void SleepUntilFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &timestamp_vector = args.data[0];
	timestamp_vector.Flatten(args.size());

	auto timestamp_data = FlatVector::GetData<timestamp_t>(timestamp_vector);
	auto &validity = FlatVector::Validity(timestamp_vector);

	for (idx_t i = 0; i < args.size(); i++) {
		if (!validity.RowIsValid(i)) {
			continue; // Skip NULL values
		}

		timestamp_t target_timestamp = timestamp_data[i];

		// Get current timestamp in microseconds (DuckDB's internal representation)
		auto current_timestamp = Timestamp::GetCurrentTimestamp();

		// Handle infinite timestamps to prevent overflow/underflow
		if (target_timestamp.value == std::numeric_limits<int64_t>::min()) {
			continue; // -infinity: return immediately
		}
		if (target_timestamp.value == std::numeric_limits<int64_t>::max()) {
			PerformSleep(context, MAX_SLEEP_SECONDS);
			continue;
		}

		// Calculate time difference in microseconds
		int64_t diff_micros = target_timestamp.value - current_timestamp.value;

		// Convert microseconds to seconds (similar to PostgreSQL's conversion)
		double seconds = static_cast<double>(diff_micros) / 1000000.0;

		PerformSleep(context, seconds);
	}

	// Return NULL (void function, PostgreSQL-compatible)
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, true);
}

//===--------------------------------------------------------------------===//
// Extension Registration
//===--------------------------------------------------------------------===//

static void LoadInternal(ExtensionLoader &loader) {
	// Register sleep(seconds)
	auto sleep = ScalarFunction("sleep", {LogicalType::DOUBLE}, LogicalType::SQLNULL, SleepFunction);
	sleep.stability = FunctionStability::VOLATILE;
	sleep.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	loader.RegisterFunction(sleep);

	// Register sleep_for(interval)
	auto sleep_for = ScalarFunction("sleep_for", {LogicalType::INTERVAL}, LogicalType::SQLNULL, SleepForFunction);
	sleep_for.stability = FunctionStability::VOLATILE;
	sleep_for.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	loader.RegisterFunction(sleep_for);

	// Register sleep_until(timestamp)
	auto sleep_until =
	    ScalarFunction("sleep_until", {LogicalType::TIMESTAMP}, LogicalType::SQLNULL, SleepUntilFunction);
	sleep_until.stability = FunctionStability::VOLATILE;
	sleep_until.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	loader.RegisterFunction(sleep_until);
}

void SleepExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string SleepExtension::Name() {
	return "sleep";
}

std::string SleepExtension::Version() const {
#ifdef EXT_VERSION_SLEEP
	return EXT_VERSION_SLEEP;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(sleep, loader) {
	duckdb::LoadInternal(loader);
}
}
