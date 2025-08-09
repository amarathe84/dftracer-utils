#!/bin/bash

# Coverage script for dftracer-utils
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
BUILD_DIR="build_coverage"
COVERAGE_DIR="coverage"
MIN_COVERAGE=80

# Functions
print_status() {
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

check_dependencies() {
    print_status "Checking dependencies..."
    
    local missing_deps=()
    
    if ! command -v lcov &> /dev/null; then
        missing_deps+=("lcov")
    fi
    
    if ! command -v genhtml &> /dev/null; then
        missing_deps+=("genhtml")
    fi
    
    if ! command -v gcov &> /dev/null; then
        missing_deps+=("gcov")
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        print_error "Missing dependencies: ${missing_deps[*]}"
        print_status "Install missing dependencies:"
        if [[ "$OSTYPE" == "darwin"* ]]; then
            echo "  brew install lcov"
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            echo "  sudo apt-get install lcov"
            echo "  # or"
            echo "  sudo yum install lcov"
        fi
        exit 1
    fi
    
    print_success "All dependencies found"
}

clean_previous() {
    print_status "Cleaning previous coverage data..."
    
    if [ -d "$BUILD_DIR" ]; then
        rm -rf "$BUILD_DIR"
    fi
    
    if [ -d "$COVERAGE_DIR" ]; then
        rm -rf "$COVERAGE_DIR"
    fi
    
    find . -name "*.gcda" -type f -delete 2>/dev/null || true
    find . -name "*.gcno" -type f -delete 2>/dev/null || true
    
    print_success "Cleaned previous coverage data"
}

build_with_coverage() {
    print_status "Building project with coverage enabled..."
    
    mkdir -p "$BUILD_DIR"
    
    cd "$BUILD_DIR"
    
    # Check if we're in a Nix shell and use the environment variables
    local cmake_args=()
    if [ -n "$CC" ]; then
        cmake_args+=("-DCMAKE_C_COMPILER=$CC")
    fi
    if [ -n "$CXX" ]; then
        cmake_args+=("-DCMAKE_CXX_COMPILER=$CXX")
    fi
    
    cmake .. \
        -DCMAKE_BUILD_TYPE=Debug \
        -DDFTRACER_UTILS_TESTS=ON \
        -DDFTRACER_UTILS_COVERAGE=ON \
        -DCMAKE_INSTALL_PREFIX="$(pwd)/install"
    make -j$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    
    cd ..
    print_success "Build completed"
}

run_tests() {
    print_status "Running tests..."
    
    cd "$BUILD_DIR"
    
    if ctest --output-on-failure; then
        print_success "All tests passed"
    else
        print_warning "Some tests failed, but continuing with coverage analysis"
        print_status "Running at least the basic test to generate some coverage data..."
        
        if [ -f "tests/test_reader" ]; then
            print_status "Running test_reader directly..."
            ./tests/test_reader || print_warning "Direct test execution also failed"
        fi
    fi
    
    cd ..
}

generate_coverage_report() {
    print_status "Generating coverage report..."
    
    mkdir -p "$COVERAGE_DIR"
    
    local compiler_info=$(cd "$BUILD_DIR" && make --version 2>/dev/null | head -1 || echo "unknown")
    print_status "Build info: $compiler_info"
    
    lcov --capture \
         --directory "$BUILD_DIR" \
         --output-file "$COVERAGE_DIR/coverage.info" \
         --rc branch_coverage=1 \
         --rc geninfo_unexecuted_blocks=1 \
         --ignore-errors gcov
    
    lcov --remove "$COVERAGE_DIR/coverage.info" \
         '/usr/*' \
         '/System/*' \
         '/Library/*' \
         '/nix/*' \
         '*/tests/*' \
         '*/_deps/*' \
         '*/build*/*' \
         '*.cpmsource*' \
         --output-file "$COVERAGE_DIR/coverage_filtered.info" \
         --rc branch_coverage=1 \
         --rc geninfo_unexecuted_blocks=1 \
         --ignore-errors gcov
    
    genhtml "$COVERAGE_DIR/coverage_filtered.info" \
            --output-directory "$COVERAGE_DIR/html" \
            --title "dftracer-utils Coverage Report" \
            --num-spaces 4 \
            --sort \
            --rc branch_coverage=1 \
            --rc geninfo_unexecuted_blocks=1 \
            --function-coverage \
            --branch-coverage
    
    print_success "Coverage report generated in $COVERAGE_DIR/html/"
}

show_coverage_summary() {
    print_status "Coverage Summary:"
    
    local line_coverage=$(lcov --summary "$COVERAGE_DIR/coverage_filtered.info" 2>/dev/null | grep "lines" | awk '{print $2}' | sed 's/%//')
    local function_coverage=$(lcov --summary "$COVERAGE_DIR/coverage_filtered.info" 2>/dev/null | grep "functions" | awk '{print $2}' | sed 's/%//')
    local branch_coverage=$(lcov --summary "$COVERAGE_DIR/coverage_filtered.info" 2>/dev/null | grep "branches" | awk '{print $2}' | sed 's/%//')
    
    echo "  Line Coverage:     ${line_coverage}%"
    echo "  Function Coverage: ${function_coverage}%"
    echo "  Branch Coverage:   ${branch_coverage}%"
    
    if (( $(echo "$line_coverage >= $MIN_COVERAGE" | bc -l) )); then
        print_success "Coverage meets minimum threshold of ${MIN_COVERAGE}%"
    else
        print_warning "Coverage (${line_coverage}%) is below minimum threshold of ${MIN_COVERAGE}%"
    fi
    
    print_status "Open $COVERAGE_DIR/html/index.html in your browser to view detailed coverage report"
}

open_report() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open "$COVERAGE_DIR/html/index.html"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if command -v xdg-open &> /dev/null; then
            xdg-open "$COVERAGE_DIR/html/index.html"
        fi
    fi
}

main() {
    print_status "Starting coverage analysis for dftracer-utils"
    
    local open_browser=false
    while [[ $# -gt 0 ]]; do
        case $1 in
            --open)
                open_browser=true
                shift
                ;;
            --help|-h)
                echo "Usage: $0 [--open] [--help]"
                echo "  --open    Open coverage report in browser after generation"
                echo "  --help    Show this help message"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    check_dependencies
    clean_previous
    build_with_coverage
    run_tests
    generate_coverage_report
    show_coverage_summary
    
    if [ "$open_browser" = true ]; then
        open_report
    fi
    
    print_success "Coverage analysis completed successfully!"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
