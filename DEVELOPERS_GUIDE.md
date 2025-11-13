# Developers Guide

## Tests

```bash
make test
```

## MPI Multi-Process Tests

Build and test with MPI support for distributed call graph generation:

```bash
# Build with MPI
make build-mpi

# Run basic MPI tests (2, 4, 8 processes)
make test-mpi

# Run comprehensive MPI test suite
make test-mpi-comprehensive
```

For detailed MPI testing information, see [MPI Testing Guide](MPI_TESTING_GUIDE.md).

## Code Coverage

This project supports code coverage reporting using lcov/gcov.

### Prerequisites for Coverage

Install lcov:
- **macOS**: `brew install lcov`
- **Ubuntu/Debian**: `sudo apt-get install lcov`
- **CentOS/RHEL**: `sudo yum install lcov`

### Generate Coverage Report

```bash
make coverage
```

The coverage report will be generated in `build_coverage/coverage/html/index.html`.

## Make Targets

- `make coverage` - Build with coverage and generate HTML report
- `make coverage-clean` - Clean coverage build directory  
- `make coverage-view` - Open coverage report in browser
- `make test` - Build and run tests without coverage
- `make test-coverage` - Run tests with coverage (requires prior coverage build)
- `make clean` - Clean up generated directories
