.PHONY: coverage coverage-clean coverage-view test test-coverage test-py test-mpi test-mpi-comprehensive build build-mpi clean clean-mpi help

# Default target
help:
	@echo "Available targets:"
	@echo "  coverage               - Build with coverage and generate HTML report"
	@echo "  coverage-clean         - Clean coverage build directory"
	@echo "  coverage-view          - Open coverage report in browser (macOS/Linux)"
	@echo "  test                   - Build and run tests without coverage"
	@echo "  test-coverage          - Run tests with coverage (requires prior coverage build)"
	@echo "  test-py                - Run Python tests in isolated venv"
	@echo "  test-mpi               - Build with MPI and run basic MPI tests"
	@echo "  test-mpi-comprehensive - Run comprehensive MPI test suite"
	@echo "  build                  - Build project normally"
	@echo "  build-mpi              - Build project with MPI support"
	@echo "  clean                  - Clean all build directories"
	@echo "  clean-mpi              - Clean MPI build directory"
	@echo "  help                   - Show this help"

# Generate coverage report
coverage:
	@echo "Building with coverage and generating report..."
	@./coverage.sh

# Clean coverage build
coverage-clean:
	@echo "Cleaning coverage build directory..."
	@rm -rf build_coverage

# View coverage report
coverage-view:
	@if [ -f "build_coverage/coverage/html/index.html" ]; then \
		if [[ "$$OSTYPE" == "darwin"* ]]; then \
			open build_coverage/coverage/html/index.html; \
		elif [[ "$$OSTYPE" == "linux-gnu"* ]]; then \
			xdg-open build_coverage/coverage/html/index.html; \
		else \
			echo "Please open build_coverage/coverage/html/index.html in your browser"; \
		fi; \
	else \
		echo "Coverage report not found. Run 'make coverage' first."; \
	fi

# Build and run tests without coverage
test:
	@echo "Building and running tests..."
	@mkdir -p build_test
	@cd build_test && cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DCMAKE_BUILD_TYPE=Debug -DDFTRACER_UTILS_TESTS=ON -DDFTRACER_UTILS_DEBUG=ON -DCMAKE_INSTALL_PREFIX=$$(pwd)/install .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4) && ctest -j8 --output-on-failure

# Run tests with coverage (requires coverage build)
test-coverage:
	@if [ -d "build_coverage" ]; then \
		cd build_coverage && ctest --output-on-failure; \
	else \
		echo "Coverage build not found. Run 'make coverage' first."; \
	fi

# Run Python tests in isolated environment
test-py:
	@echo "Running Python tests in isolated environment..."
	@rm -rf .venv_test_py
	@python3 -m venv .venv_test_py
	@.venv_test_py/bin/pip install --upgrade pip setuptools wheel
	@.venv_test_py/bin/pip install -e .[dev]
	@.venv_test_py/bin/pytest tests/python -v
	@rm -rf .venv_test_py
	@echo "Python tests completed successfully!"

format:
	@echo "Formatting code..."
	find ./include ./src ./tests -type f \( -name "*.h" -o -name "*.cpp" \) -exec clang-format -i -style=file {} +
# 	@mkdir -p build_format
# 	@cd build_format && cmake -DCMAKE_BUILD_TYPE=Release ..
# 	@if command -v clang-tidy &> /dev/null; then \
# 		find src include -type f \( -name "*.cpp" -o -name "*.h" \) -exec clang-tidy -p build {} --config-file=.clang-tidy --fix-errors --fix-notes --fix \; ; \
# 	fi

# Normal build
build:
	@echo "Building project..."
	@mkdir -p build
	@cd build && cmake .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

build-debug:
	@echo "Building project in debug mode..."
	@mkdir -p build_debug
	@cd build_debug && cmake -DDFTRACER_UTILS_DEBUG=ON .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

build-release:
	@echo "Building project in release mode..."
	@mkdir -p build
	@cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

install-debug: build
	@echo "Installing project..."
	@mkdir -p build

	@cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$$(pwd)/install .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Build with MPI support
build-mpi:
	@echo "Building project with MPI support..."
	@mkdir -p build_mpi
	@cd build_mpi && cmake -DCMAKE_BUILD_TYPE=Release -DDFTRACER_UTILS_ENABLE_MPI=ON -DCMAKE_INSTALL_PREFIX=$$(pwd)/install .. && make -j$$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Run basic MPI tests
test-mpi: build-mpi
	@echo "Running basic MPI tests..."
	@if [ -d "trace_short" ]; then \
		TRACE_FILE="trace_short/bert_v100-1.pfw"; \
	else \
		TRACE_FILE="trace/bert_v100-1.pfw"; \
	fi; \
	if [ ! -f "$$TRACE_FILE" ]; then \
		echo "Error: No trace file found. Please ensure trace files exist."; \
		exit 1; \
	fi; \
	for np in 2 4 8; do \
		echo "=== Testing with $$np MPI processes ==="; \
		mpirun -np $$np --oversubscribe build_mpi/bin/dftracer_call_graph_mpi \
			$$TRACE_FILE --output-dir build_mpi/test_output_np$$np || true; \
	done
	@echo "MPI tests completed!"

# Run comprehensive MPI test suite
test-mpi-comprehensive: build-mpi
	@echo "Running comprehensive MPI test suite..."
	@./test_mpi_comprehensive.sh

# Clean MPI build
clean-mpi:
	@echo "Cleaning MPI build directory..."
	@rm -rf build_mpi mpi_test_results

# Clean all builds
clean:
	@echo "Cleaning all build directories..."
	@rm -rf build build_debug build_test build_coverage build_mpi install .venv_test_py build_format mpi_test_results
