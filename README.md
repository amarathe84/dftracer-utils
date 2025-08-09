# dftracer-utils

A collection of utilities for DFTracer

## Building

### Prerequisites

- CMake 3.5 or higher
- C++11 compatible compiler
- zlib development library
- SQLite3 development library
- pkg-config

### Basic Build

```bash
mkdir build && cd build
cmake ..
make
```

### Build with Tests

```bash
mkdir build && cd build
cmake -DDFTRACER_UTILS_TESTS=ON ..
make
ctest
```

## Code Coverage

This project supports code coverage reporting using lcov/gcov.

### Prerequisites for Coverage

Install lcov:
- **macOS**: `brew install lcov`
- **Ubuntu/Debian**: `sudo apt-get install lcov`
- **CentOS/RHEL**: `sudo yum install lcov`

### Generate Coverage Report

#### Option 1: Using the convenience script
```bash
./coverage.sh
```

#### Option 2: Using Make
```bash
make coverage
```

#### Option 3: Manual CMake
```bash
mkdir build_coverage && cd build_coverage
cmake -DCMAKE_BUILD_TYPE=Debug -DDFTRACER_UTILS_TESTS=ON -DDFTRACER_UTILS_COVERAGE=ON ..
make
make coverage
```

The coverage report will be generated in `build_coverage/coverage/html/index.html`.

### Coverage Makefile Targets

- `make coverage` - Build with coverage and generate HTML report
- `make coverage-clean` - Clean coverage build directory  
- `make coverage-view` - Open coverage report in browser
- `make test` - Build and run tests without coverage
- `make test-coverage` - Run tests with coverage (requires prior coverage build)

### CI/CD Integration

The project includes a GitHub Actions workflow (`.github/workflows/coverage.yml`) that:
- Builds the project with coverage enabled
- Runs tests and generates coverage reports
- Uploads results to Codecov and Coveralls

To enable coverage reporting in your repository:
1. Enable Codecov integration and add `CODECOV_TOKEN` secret
2. Enable Coveralls integration 
3. The workflow will automatically run on pushes and pull requests

## Installation

```bash
cd build
make install
```
