# dftracer-utils

A collection of utilities for DFTracer

## Building

### Prerequisites

- CMake 3.5 or higher
- C++11 compatible compiler
- zlib development library
- SQLite3 development library
- pkg-config

### Build

```bash
mkdir build && cd build
cmake ..
make
```

## Installation

```bash
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=<LOCATION>
make
make install
```

## Developers Guide

Please see [Developers Guide](DEVELOPERS_GUIDE.md) for more information how to test, run coverage, etc.

## MPI Multi-Process Support

dftracer-utils includes MPI support for distributed call graph generation. See [MPI Testing Guide](MPI_TESTING_GUIDE.md) for:
- Building with MPI support
- Running MPI tests
- CI/CD integration details
- Performance optimization tips

Quick start:
```bash
# Build with MPI
make build-mpi

# Run MPI tests
make test-mpi
```
