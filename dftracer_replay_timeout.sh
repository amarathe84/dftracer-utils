#!/bin/bash

# dftracer_replay_timeout.sh - Wrapper script for dftracer_replay with timeout support
# Usage: ./dftracer_replay_timeout.sh [TIMEOUT_SECONDS] [dftracer_replay_options...]

set -e

# Default timeout: 300 seconds (5 minutes)
DEFAULT_TIMEOUT=300

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DFTRACER_REPLAY_BIN="${SCRIPT_DIR}/build/bin/dftracer_replay"

# Function to show usage
show_usage() {
    echo "Usage: $0 [TIMEOUT_SECONDS] [dftracer_replay_options...]"
    echo ""
    echo "Examples:"
    echo "  $0 60 --dry-run --verbose trace/bert_v100-1.pfw"
    echo "  $0 --no-timing --dftracer-mode trace/bert_v100-1.pfw    # uses default ${DEFAULT_TIMEOUT}s timeout"
    echo "  $0 120 --dftracer-mode trace/bert_v100-1.pfw           # uses 120s timeout"
    echo ""
    echo "Supported dftracer_replay options:"
    echo "  --no-timing     Ignore original timing and execute as fast as possible"
    echo "  --dry-run       Parse and analyze traces without executing operations"
    echo "  --dftracer-mode Use DFTracer sleep-based replay (may hang without timeout)"
    echo "  -v, --verbose   Enable verbose output and detailed statistics"
    echo ""
    echo "Exit codes:"
    echo "  0   - Success"
    echo "  1   - No operations performed or error"
    echo "  2   - Partial success (some events failed)"
    echo "  124 - Timeout occurred"
}

# Check if dftracer_replay binary exists
if [[ ! -x "$DFTRACER_REPLAY_BIN" ]]; then
    echo "Error: dftracer_replay binary not found at: $DFTRACER_REPLAY_BIN"
    echo "Please build the project first: make build"
    exit 1
fi

# Parse arguments
timeout_seconds=""
replay_args=()

# Check for help request
if [[ $# -eq 0 ]] || [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]]; then
    show_usage
    exit 0
fi

# Check if first argument is a timeout value (positive integer)
if [[ "$1" =~ ^[0-9]+$ ]]; then
    timeout_seconds="$1"
    shift
    replay_args=("$@")
else
    # No timeout specified, use default
    timeout_seconds="$DEFAULT_TIMEOUT"
    replay_args=("$@")
fi

# Validate that we have replay arguments
if [[ ${#replay_args[@]} -eq 0 ]]; then
    echo "Error: No dftracer_replay arguments provided"
    show_usage
    exit 1
fi

# Check if timeout command is available
if ! command -v timeout >/dev/null 2>&1; then
    echo "Warning: 'timeout' command not available. Running without timeout protection."
    exec "$DFTRACER_REPLAY_BIN" "${replay_args[@]}"
fi

# Log the command being executed
echo "Running: timeout ${timeout_seconds}s dftracer_replay ${replay_args[*]}"
echo "----------------------------------------"

# Execute with timeout and capture the result
set +e  # Don't exit on non-zero return codes
timeout "${timeout_seconds}s" "$DFTRACER_REPLAY_BIN" "${replay_args[@]}"
exit_code=$?
set -e  # Re-enable exit on error

# Handle timeout exit code
if [[ $exit_code -eq 124 ]]; then
    echo ""
    echo "========================================="
    echo "TIMEOUT: dftracer_replay was terminated after ${timeout_seconds} seconds"
    echo "This may indicate the replay got stuck, possibly due to:"
    echo "- Timing/sleep issues in --dftracer-mode"
    echo "- Large trace files taking too long to process"
    echo "- Infinite loops in replay logic"
    echo ""
    echo "Try:"
    echo "- Increasing the timeout value"
    echo "- Using --no-timing flag to skip timing logic"
    echo "- Using --dry-run to test without actual execution"
    echo "========================================="
elif [[ $exit_code -eq 0 ]]; then
    echo ""
    echo "Replay completed successfully within ${timeout_seconds} seconds."
elif [[ $exit_code -eq 1 ]]; then
    echo ""
    echo "Replay completed with no operations performed or error."
elif [[ $exit_code -eq 2 ]]; then
    echo ""
    echo "Replay completed with partial success (some events failed)."
fi

exit $exit_code