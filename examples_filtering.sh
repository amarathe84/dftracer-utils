#!/bin/bash
# Examples of using dftracer_replay with advanced filtering and query functionality

TRACE_FILE="trace_short/bert_v100-1.pfw"

echo "========================================="
echo "DFTracer Replay - Filtering Examples"
echo "========================================="
echo ""

# Example 1: Basic dry-run to see what's in the trace
echo "=== Example 1: Basic Analysis (Dry Run) ==="
echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --verbose"
echo ""
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --verbose
echo ""
echo "Press Enter to continue..."; read

# Example 2: Filter only POSIX operations
echo "=== Example 2: Filter Only POSIX Operations ==="
echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --filter-category POSIX --verbose"
echo ""
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --filter-category POSIX --verbose
echo ""
echo "Press Enter to continue..."; read

# Example 3: Filter specific functions (read operations only)
echo "=== Example 3: Filter Only Read Operations ==="
echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --filter-function read,pread,pread64,__xstat64 --verbose"
echo ""
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --filter-function "read,pread,pread64,__xstat64" --verbose
echo ""
echo "Press Enter to continue..."; read

# Example 4: Exclude specific functions
echo "=== Example 4: Exclude Stat Operations ==="
echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --exclude-function __xstat64,stat,fstat --verbose"
echo ""
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --exclude-function "__xstat64,stat,fstat" --verbose
echo ""
echo "Press Enter to continue..."; read

# Example 5: Sample 10% of events
echo "=== Example 5: Sample 10% of Events ==="
echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --sample-rate 0.1 --verbose"
echo ""
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --sample-rate 0.1 --verbose
echo ""
echo "Press Enter to continue..."; read

# Example 6: Limit to first 100 events
echo "=== Example 6: Process Only First 100 Events ==="
echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --max-events 100 --verbose"
echo ""
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --max-events 100 --verbose
echo ""
echo "Press Enter to continue..."; read

# Example 7: Filter by specific PID
echo "=== Example 7: Filter by Specific PID ==="
echo "First, let's see what PIDs are in the trace..."
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run 2>&1 | grep "PID"
echo ""
echo "Now filtering for that PID..."
# Get the first PID from the trace
PID=$(./build/bin/dftracer_replay "$TRACE_FILE" --dry-run 2>&1 | grep "Unique PIDs:" -A 10 | grep "PID" | head -1 | awk '{print $2}' | tr -d ':')
if [ -n "$PID" ]; then
    echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --filter-pid $PID --verbose"
    ./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --filter-pid "$PID" --verbose
else
    echo "Could not extract PID, running without filter"
    ./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --verbose
fi
echo ""
echo "Press Enter to continue..."; read

# Example 8: Filter by operation size
echo "=== Example 8: Filter Large I/O Operations (>= 1MB) ==="
echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --min-size 1048576 --verbose"
echo ""
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --min-size 1048576 --verbose
echo ""
echo "Press Enter to continue..."; read

# Example 9: Combine multiple filters
echo "=== Example 9: Complex Query - POSIX reads >= 4KB, sampled at 50% ==="
echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --filter-category POSIX --filter-function read,pread,pread64 --min-size 4096 --sample-rate 0.5 --verbose"
echo ""
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --filter-category POSIX --filter-function "read,pread,pread64" --min-size 4096 --sample-rate 0.5 --verbose
echo ""
echo "Press Enter to continue..."; read

# Example 10: Timestamp-based filtering
echo "=== Example 10: Time-Window Filtering ==="
echo "Getting timestamp range from trace..."
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run 2>&1 | grep "Trace timespan"
echo ""
echo "Now filtering first half of the trace by timestamp..."
# This would need actual timestamps from the trace
echo "Command: ./build/bin/dftracer_replay $TRACE_FILE --dry-run --max-events 50 --verbose"
echo "(Using max-events as proxy for time-based filtering)"
./build/bin/dftracer_replay "$TRACE_FILE" --dry-run --max-events 50 --verbose
echo ""

echo "========================================="
echo "All examples completed!"
echo "========================================="
