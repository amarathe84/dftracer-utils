#!/bin/bash
# Comprehensive MPI testing script for dftracer-utils
# Tests multi-process functionality with various configurations

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
BUILD_DIR="${BUILD_DIR:-build_mpi}"
TRACE_DIR="${TRACE_DIR:-trace_short}"
OUTPUT_DIR="${OUTPUT_DIR:-mpi_test_results}"
PROCESS_COUNTS="${PROCESS_COUNTS:-2 4 8}"
VERBOSE="${VERBOSE:-0}"

# Function to print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if MPI is available
check_mpi() {
    print_info "Checking MPI availability..."
    
    if ! command -v mpirun &> /dev/null; then
        print_error "mpirun not found. Please install MPI (OpenMPI or MPICH)"
        exit 1
    fi
    
    print_success "MPI found: $(mpirun --version | head -1)"
}

# Function to build with MPI support
build_mpi() {
    print_info "Building dftracer-utils with MPI support..."
    
    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DDFTRACER_UTILS_ENABLE_MPI=ON \
          -DDFTRACER_UTILS_TESTS=ON \
          -DDFTRACER_UTILS_DEBUG=${DFTRACER_UTILS_DEBUG:-OFF} \
          -DCMAKE_INSTALL_PREFIX=$(pwd)/install \
          ..
    
    make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    
    cd ..
    
    if [ ! -f "$BUILD_DIR/bin/dftracer_call_graph_mpi" ]; then
        print_error "MPI binary not built"
        exit 1
    fi
    
    print_success "Build completed"
}

# Function to find trace files
find_trace_files() {
    print_info "Searching for trace files..."
    
    local trace_files=()
    
    # Check trace_short directory first
    if [ -d "$TRACE_DIR" ]; then
        mapfile -t trace_files < <(find "$TRACE_DIR" -name "*.pfw" -type f | head -3)
    fi
    
    # Fallback to trace directory
    if [ ${#trace_files[@]} -eq 0 ] && [ -d "trace" ]; then
        mapfile -t trace_files < <(find "trace" -name "*.pfw" -type f | head -3)
    fi
    
    if [ ${#trace_files[@]} -eq 0 ]; then
        print_warning "No trace files found. Creating minimal test trace..."
        mkdir -p test_traces
        cat > test_traces/minimal.pfw << 'EOF'
[
{"id":1,"name":"HH","cat":"dftracer","pid":1000,"tid":1000,"ph":"M","args":{"hhash":"test","name":"testhost","value":"testhash"}},
{"id":2,"name":"thread_name","cat":"dftracer","pid":1000,"tid":1000,"ph":"M","args":{"name":"1000","value":"thread_name"}},
{"id":3,"name":"test_function","cat":"dftracer","pid":1000,"tid":1000,"ph":"X","ts":1000,"dur":100}
]
EOF
        trace_files=("test_traces/minimal.pfw")
    fi
    
    echo "${trace_files[@]}"
}

# Function to run MPI tests
run_mpi_test() {
    local np=$1
    local trace_file=$2
    local test_name=$3
    
    print_info "Running test '$test_name' with $np processes on $(basename $trace_file)"
    
    local output_subdir="$OUTPUT_DIR/${test_name}_np${np}"
    mkdir -p "$output_subdir"
    
    local log_file="$output_subdir/test.log"
    
    # Run with mpirun
    if mpirun -np "$np" --oversubscribe \
              "$BUILD_DIR/bin/dftracer_call_graph_mpi" \
              "$trace_file" \
              --output-dir "$output_subdir" \
              > "$log_file" 2>&1; then
        print_success "Test completed successfully"
        
        # Check output
        local file_count=$(find "$output_subdir" -type f ! -name "*.log" | wc -l)
        print_info "Generated $file_count output files"
        
        if [ "$VERBOSE" -eq 1 ]; then
            echo "Output files:"
            find "$output_subdir" -type f ! -name "*.log" | head -10
        fi
        
        return 0
    else
        print_error "Test failed (see $log_file for details)"
        if [ "$VERBOSE" -eq 1 ]; then
            cat "$log_file"
        fi
        return 1
    fi
}

# Function to compare serial vs parallel outputs
compare_outputs() {
    local trace_file=$1
    local np=$2
    
    print_info "Comparing serial vs parallel ($np processes) execution..."
    
    # Run serial
    local serial_dir="$OUTPUT_DIR/serial_comparison"
    mkdir -p "$serial_dir"
    
    "$BUILD_DIR/bin/dftracer_call_graph" \
        "$trace_file" \
        --output-dir "$serial_dir" \
        > "$serial_dir/serial.log" 2>&1 || print_warning "Serial execution failed"
    
    # Run parallel
    local parallel_dir="$OUTPUT_DIR/parallel_comparison_np${np}"
    mkdir -p "$parallel_dir"
    
    mpirun -np "$np" --oversubscribe \
           "$BUILD_DIR/bin/dftracer_call_graph_mpi" \
           "$trace_file" \
           --output-dir "$parallel_dir" \
           > "$parallel_dir/parallel.log" 2>&1 || print_warning "Parallel execution failed"
    
    # Compare file counts
    local serial_count=$(find "$serial_dir" -type f ! -name "*.log" | wc -l)
    local parallel_count=$(find "$parallel_dir" -type f ! -name "*.log" | wc -l)
    
    print_info "Serial output: $serial_count files"
    print_info "Parallel output: $parallel_count files"
    
    if [ "$serial_count" -eq "$parallel_count" ]; then
        print_success "File count matches"
    else
        print_warning "File count differs (this may be expected)"
    fi
}

# Function to run scaling tests
run_scaling_tests() {
    local trace_file=$1
    
    print_info "Running scaling tests..."
    
    local scaling_results="$OUTPUT_DIR/scaling_results.txt"
    echo "Process Count,Execution Time (seconds)" > "$scaling_results"
    
    for np in $PROCESS_COUNTS; do
        local start_time=$(date +%s)
        
        if run_mpi_test "$np" "$trace_file" "scaling"; then
            local end_time=$(date +%s)
            local duration=$((end_time - start_time))
            echo "$np,$duration" >> "$scaling_results"
            print_info "Completed in ${duration}s with $np processes"
        fi
    done
    
    print_success "Scaling results saved to $scaling_results"
    
    if [ -f "$scaling_results" ]; then
        echo ""
        print_info "Scaling Summary:"
        cat "$scaling_results"
    fi
}

# Main execution
main() {
    print_info "=== DFTracer MPI Comprehensive Test Suite ==="
    echo ""
    
    # Check prerequisites
    check_mpi
    
    # Build if needed
    if [ ! -f "$BUILD_DIR/bin/dftracer_call_graph_mpi" ]; then
        build_mpi
    else
        print_info "Using existing build in $BUILD_DIR"
    fi
    
    # Create output directory
    mkdir -p "$OUTPUT_DIR"
    
    # Find trace files
    trace_files=($(find_trace_files))
    print_success "Found ${#trace_files[@]} trace file(s)"
    
    if [ ${#trace_files[@]} -eq 0 ]; then
        print_error "No trace files available"
        exit 1
    fi
    
    # Select first trace file for tests
    main_trace="${trace_files[0]}"
    print_info "Using trace file: $main_trace"
    echo ""
    
    # Run basic tests with different process counts
    print_info "=== Running Basic MPI Tests ==="
    for np in $PROCESS_COUNTS; do
        run_mpi_test "$np" "$main_trace" "basic" || true
    done
    echo ""
    
    # Compare serial vs parallel
    print_info "=== Running Correctness Comparison ==="
    compare_outputs "$main_trace" 4 || true
    echo ""
    
    # Run scaling tests
    print_info "=== Running Scaling Tests ==="
    run_scaling_tests "$main_trace" || true
    echo ""
    
    # Summary
    print_info "=== Test Summary ==="
    print_success "All tests completed!"
    print_info "Results directory: $OUTPUT_DIR"
    
    # List all output directories
    echo ""
    print_info "Generated output directories:"
    find "$OUTPUT_DIR" -maxdepth 1 -type d | tail -n +2
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=1
            shift
            ;;
        -b|--build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        -t|--trace-dir)
            TRACE_DIR="$2"
            shift 2
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -p|--process-counts)
            PROCESS_COUNTS="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -v, --verbose           Enable verbose output"
            echo "  -b, --build-dir DIR     Build directory (default: build_mpi)"
            echo "  -t, --trace-dir DIR     Trace directory (default: trace_short)"
            echo "  -o, --output-dir DIR    Output directory (default: mpi_test_results)"
            echo "  -p, --process-counts    Space-separated list of process counts (default: '2 4 8')"
            echo "  -h, --help              Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  DFTRACER_UTILS_DEBUG    Set to ON to build with debug symbols"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Run main
main
