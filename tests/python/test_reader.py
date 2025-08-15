#!/usr/bin/env python3
"""
Comprehensive test cases for dft_utils Python bindings - mirrors C++ tests
"""

import pytest
import os
import tempfile
import gzip
import random
import string
import subprocess
import shutil
from pathlib import Path

import dftracer.utils as dft_utils

class PythonTestEnvironment:
    """Test environment manager - mirrors C++ TestEnvironment"""
    
    def __init__(self, lines=100):
        self.lines = lines
        self.temp_dir = None
        self.test_files = []
        self._setup()
    
    def _setup(self):
        """Set up temporary directory and test files"""
        self.temp_dir = tempfile.mkdtemp(prefix="dft_utils_test_")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
    
    def cleanup(self):
        """Clean up temporary files and directory"""
        for file_path in self.test_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                idx_path = file_path + ".idx"
                if os.path.exists(idx_path):
                    os.remove(idx_path)
            except OSError:
                pass
        
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
            except OSError:
                pass
    
    def create_test_gzip_file(self, filename="test_data.pfw.gz", bytes_per_line=1024):
        """Create a test gzip file with sample trace-like data"""
        file_path = os.path.join(self.temp_dir, filename)
        
        # Generate test data similar to what C++ test creates
        lines = []
        closing_len = 3  # len('"}\n')
        for i in range(1, self.lines + 1):
            # Build the JSON line up to the "data" key
            line = f'{{"name":"name_{i}","cat":"cat_{i}","dur":{(i * 123 % 10000)},"data":"'
            current_size = len(line)
            needed_padding = 0
            if bytes_per_line > current_size + closing_len:
                needed_padding = bytes_per_line - current_size - closing_len
            # Append padding safely
            if needed_padding:
                pad_chunk = 'x' * 4096
                while needed_padding >= len(pad_chunk):
                    line += pad_chunk
                    needed_padding -= len(pad_chunk)
                if needed_padding:
                    line += 'x' * needed_padding
            line += '"}\n'
            lines.append(line)
        
        # Write compressed data
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            f.writelines(lines)
        
        self.test_files.append(file_path)
        return file_path
    
    def get_index_path(self, gz_file_path):
        """Get the index file path for a gzip file"""
        return gz_file_path + ".idx"
    
    def build_index(self, gz_file_path, chunk_size_mb=1.0):
        """Build index for the gzip file using dft_reader executable"""
        dft_reader_path = self._find_dft_reader_executable()
        if not dft_reader_path:
            pytest.skip("dft_reader executable not found")
        
        idx_file = self.get_index_path(gz_file_path)
        
        # Run dft_reader to build index
        # New command format: dft_reader --index idx_file --chunk-size chunk_size_mb gz_file
        cmd = [dft_reader_path, "--index", idx_file, "--chunk-size", str(chunk_size_mb), gz_file_path]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            if not os.path.exists(idx_file):
                pytest.skip(f"Index file was not created: {result.stderr}")
            return idx_file
        except subprocess.CalledProcessError as e:
            pytest.skip(f"Failed to build index: {e.stderr}")
    
    def _find_dft_reader_executable(self):
        """Find the dft_reader executable"""
        # Check common build locations
        possible_paths = [
            "dft_reader",  # In PATH
            "./dft_reader",  # Current directory
            "../dft_reader",  # Parent directory
            "../../dft_reader",  # Grandparent directory
            "./build_test/dft_reader",  # CMake build directory
            "./build/dft_reader",  # Alternative build directory
            "./build/dft_utils/dft_reader",  # Build subdirectory
            "./cmake-build-debug/dft_reader",  # IDE build directory
            "./cmake-build-release/dft_reader",  # IDE build directory
            "./.venv/lib/python3.9/site-packages/dft_utils/bin/dft_reader",  # Python package
        ]
        
        for path in possible_paths:
            if shutil.which(path):
                return path
            if os.path.isfile(path) and os.access(path, os.X_OK):
                return path
        
        return None
    
    def is_valid(self):
        """Check if test environment is valid"""
        return self.temp_dir and os.path.exists(self.temp_dir)


class TestDFTracerReader:
    """Test cases for DFTracerReader - mirrors C++ tests"""
    
    def test_import(self):
        """Test that we can import the module and classes"""
        assert hasattr(dft_utils, 'DFTracerReader')
        assert hasattr(dft_utils, 'DFTracerLineRangeIterator')
        assert hasattr(dft_utils, 'DFTracerRawRangeIterator')
        assert hasattr(dft_utils, 'dft_reader_raw_range')
        assert hasattr(dft_utils, 'dft_reader_range')
        assert hasattr(dft_utils, 'set_log_level')
    
    def test_reader_creation_nonexistent_file(self):
        """Test reader creation with non-existent file - mirrors C++ error handling"""
        with pytest.raises(RuntimeError):
            dft_utils.DFTracerReader("nonexistent_file.pfw.gz")
    
    def test_reader_creation_missing_index(self):
        """Test reader creation when index file doesn't exist - now auto-builds"""
        with PythonTestEnvironment() as env:
            gz_file = env.create_test_gzip_file()
            
            # Reader should now auto-build index instead of failing
            with dft_utils.DFTracerReader(gz_file) as reader:
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                # Verify index file was created
                idx_file = gz_file + ".idx"
                assert os.path.exists(idx_file)
    
    def test_reader_basic_functionality(self):
        """Test reader basic functionality - mirrors C++ 'Reader - Basic functionality'"""
        with PythonTestEnvironment() as env:
            # Create and index test file
            gz_file = env.create_test_gzip_file()
            idx_file = env.build_index(gz_file, chunk_size_mb=0.5)
            
            # Test context manager (constructor and destructor)
            with dft_utils.DFTracerReader(gz_file) as reader:
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                assert reader.gzip_path == gz_file
                assert reader.index_path == idx_file
                
                # Test getter methods
                assert reader.gzip_path == gz_file
                assert reader.index_path == idx_file
            
            # Should be able to create another one
            with dft_utils.DFTracerReader(gz_file) as reader2:
                assert reader2.is_open
    
    def test_reader_properties(self):
        """Test reader properties - mirrors C++ 'Getter methods'"""
        with PythonTestEnvironment() as env:
            gz_file = env.create_test_gzip_file()
            idx_file = env.build_index(gz_file, chunk_size_mb=1.5)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                # Test property types
                assert isinstance(reader.is_open, bool)
                assert isinstance(reader.gzip_path, str)
                assert isinstance(reader.index_path, str) 
                assert isinstance(reader.get_max_bytes(), int)
                assert isinstance(reader.get_default_step(), int)
                
                # Test getter methods
                assert reader.gzip_path == gz_file
                assert reader.index_path == idx_file
                assert reader.get_max_bytes() > 0
                
                # Test setting default step
                original_step = reader.get_default_step()
                reader.set_default_step(512 * 1024)
                assert reader.get_default_step() == 512 * 1024
                reader.set_default_step(original_step)
    
    def test_reader_data_reading(self):
        """Test reader data reading - mirrors C++ 'Read byte range using streaming API'"""
        with PythonTestEnvironment() as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, chunk_size_mb=0.5)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                # Read larger range to ensure complete lines are found
                data = reader.read_line_bytes(0, bytes_per_line)

                assert 0 < len(data) <= bytes_per_line  # Should get data with boundary extension
                assert '"name"' in data  # Should contain JSON content
                assert data.endswith('\n')  # Should end with complete line
    
    def test_reader_iteration_methods(self):
        """Test reader iteration - mirrors C++ iteration patterns"""
        with PythonTestEnvironment(lines=1000) as env:  # Larger file for better iteration testing
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, chunk_size_mb=0.5)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                # Test direct iteration
                chunk_count = 0
                total_bytes = 0
                for chunk in reader:
                    chunk_count += 1
                    total_bytes += len(chunk)
                    assert len(chunk) > 0
                    if chunk_count >= 3:  # Just test first few chunks
                        break
                
                assert chunk_count > 0
                assert total_bytes > 0
                
                # Test default iterator
                chunk_count = 0
                for chunk in reader.iter():
                    chunk_count += 1
                    assert len(chunk) > 0
                    if chunk_count >= 2:
                        break
                
                assert chunk_count > 0
                
                # Test custom iterator
                chunk_count = 0
                for chunk in reader.iter(256 * 1024):  # 256KB chunks
                    chunk_count += 1
                    assert len(chunk) > 0
                    if chunk_count >= 2:
                        break
                
                assert chunk_count > 0
    
    def test_reader_range_operations(self):
        """Test reader range operations - mirrors C++ range reading"""
        with PythonTestEnvironment(lines=2000) as env:  # Large enough for range testing
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, chunk_size_mb=0.5)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                
                # Test reading a range - use larger range to ensure complete lines
                if max_bytes > bytes_per_line * 4:
                    start = 0  # Start from beginning
                    end = start + bytes_per_line * 4  # Read 4 lines worth
                    data = reader.read_line_bytes(start, end)
                    assert 0 < len(data) <= end - start
                    # Note: reader might return complete records that are less than or equal to the range


                # Test range iterator - align with line boundaries
                if max_bytes > bytes_per_line * 4:
                    start = 0  # Start at line boundary
                    end = start + bytes_per_line * 3  # Read 3 lines worth
                    step = bytes_per_line
                    
                    chunk_count = 0
                    total_bytes = 0
                    for chunk in dft_utils.dft_reader_range(reader, start, end, step):
                        chunk_count += 1
                        total_bytes += len(chunk)
                        assert 0 < len(chunk) <= step < end - start
                        if chunk_count >= 3:
                            break
                    
                    assert chunk_count > 0
                    assert total_bytes > 0
    
    def test_context_manager(self):
        """Test reader as context manager"""
        with PythonTestEnvironment() as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file)
            
            # Test with statement
            with dft_utils.DFTracerReader(gz_file) as reader:
                assert reader.is_open
                max_bytes = reader.get_max_bytes()
                assert max_bytes > 0
            
            # Reader should be cleaned up after exiting with block
    
    def test_log_level_functions(self):
        """Test log level functions - mirrors C++ logger tests"""
        # Test string-based log level
        dft_utils.set_log_level("info")
        
        # Test integer-based log level  
        dft_utils.set_log_level_int(2)  # info level
        
        # Test getting log levels
        level_str = dft_utils.get_log_level_string()
        level_int = dft_utils.get_log_level_int()
        
        assert isinstance(level_str, str)
        assert isinstance(level_int, int)
        assert level_int >= 0
        
        # Test various log levels like C++ does
        assert dft_utils.set_log_level_int(0) == 0  # trace
        assert dft_utils.get_log_level_string() == "trace"
        
        assert dft_utils.set_log_level_int(1) == 0  # debug
        assert dft_utils.get_log_level_string() == "debug"
        
        assert dft_utils.set_log_level_int(2) == 0  # info
        assert dft_utils.get_log_level_string() == "info"

    def test_raw_read_functionality(self):
        """Test raw read functionality - mirrors C++ raw reading tests"""
        with PythonTestEnvironment(lines=1000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, chunk_size_mb=0.5)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                
                if max_bytes > 100:
                    # Test read method - should return closer to requested size
                    start = 50
                    end = start + 50  # 50 bytes
                    raw_data = reader.read(start, end)
                    assert len(raw_data) > 0
                    assert len(raw_data) == 50  # Raw read should be exact size
                    
                    # Compare with line bytes read - should be less or (empty) due to line boundary extension
                    regular_data = reader.read_line_bytes(start, end)
                    assert len(regular_data) <= len(raw_data)

                    # Regular read should end with complete JSON line
                    if regular_data:
                        assert regular_data.endswith('\\n')

                    # Both should start with same data (up to regular_data length)
                    assert raw_data[:len(regular_data)] == regular_data

    def test_raw_iterator_functionality(self):
        """Test raw iterator functionality - mirrors C++ raw iterator tests"""
        with PythonTestEnvironment(lines=2000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, chunk_size_mb=0.5)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                if max_bytes <= 1024:
                    pytest.skip("File too small for raw iterator testing")
                
                # Test default raw iterator
                raw_iter = reader.raw_iter()
                assert hasattr(raw_iter, '__iter__')
                assert hasattr(raw_iter, '__next__')
                
                # Test custom step raw iterator
                step_size = 512
                custom_raw_iter = reader.raw_iter(step_size)
                
                # Test iteration
                chunk_count = 0
                total_bytes = 0
                for chunk in custom_raw_iter:
                    chunk_count += 1
                    total_bytes += len(chunk)
                    assert len(chunk) > 0
                    assert len(chunk) <= step_size  # Raw iterator should respect step size closely
                    if chunk_count >= 5:  # Just test first few chunks
                        break
                
                assert chunk_count > 0
                assert total_bytes > 0

    def test_raw_vs_line_iterator_comparison(self):
        """Test comparison between raw and line-aware iterators"""
        with PythonTestEnvironment(lines=2000) as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, chunk_size_mb=0.5)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                
                step_size = bytes_per_line
                
                # Test line-aware iterator
                line_chunks = []
                line_iter = reader.iter(step_size)
                for i, chunk in enumerate(line_iter):
                    line_chunks.append(chunk)
                    if i >= 2:  # Get 3 chunks
                        break
                
                # Reset reader and test raw iterator
                raw_chunks = []
                raw_iter = reader.raw_iter(step_size)
                for i, chunk in enumerate(raw_iter):
                    raw_chunks.append(chunk)
                    if i >= 2:  # Get 3 chunks
                        break
                
                assert len(line_chunks) == len(raw_chunks)
                
                # Line-aware chunks should generally be larger due to boundary extension
                for line_chunk, raw_chunk in zip(line_chunks, raw_chunks):
                    assert len(line_chunk) > 0
                    assert len(raw_chunk) > 0
                    # Line chunks should end with complete lines
                    assert line_chunk.endswith('\n')
                    # Raw chunks should be closer to requested step size
                    assert len(raw_chunk) <= step_size + 10  # Some tolerance

    def test_line_boundary_detection(self):
        """Test Line boundary detection"""
        with PythonTestEnvironment(lines=1000) as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, chunk_size_mb=0.5)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                # Small range should provide minimum requested bytes
                content = reader.read_line_bytes(0, bytes_per_line)

                assert len(content) <= bytes_per_line  # Should get what was requested

                # Verify that output ends with complete line
                assert content.endswith('\n')  # Should end with newline

                # Should contain complete JSON objects
                assert content.rfind('}') != -1  # Should contain closing braces
                last_brace_pos = content.rfind('}')
                assert last_brace_pos < len(content) - 1  # '}' should not be the last character
                assert content[last_brace_pos + 1] == '\n'  # Should be followed by newline

    def test_edge_cases(self):
        """Test edge cases"""
        with PythonTestEnvironment() as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, chunk_size_mb=0.5)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                
                if max_bytes > 10:
                    # Read near the end (not complete line)
                    data = reader.read_line_bytes(max_bytes - 10, max_bytes)
                    assert len(data) == 0
                    
                    # Read single byte range (not complete line)
                    if max_bytes > 1:
                        data = reader.read_line_bytes(0, 1)
                        assert len(data) == 0
                
                # Test raw read edge cases
                if max_bytes > 10:
                    # Single byte raw read
                    raw_data = reader.read(0, 1)
                    assert len(raw_data) == 1
                    
                    # Read near end with raw
                    raw_data = reader.read(max_bytes - 5, max_bytes - 1)
                    assert len(raw_data) == 4


class TestDFTracerRangeIterator:
    """Test cases for DFTracerRangeIterator"""
    
    def test_range_iterator_properties(self):
        """Test range iterator properties"""
        with PythonTestEnvironment(lines=1000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                if max_bytes <= 1024:
                    pytest.skip("File too small for range testing")
                
                start = 512
                end = start + 512
                step = 128
                
                range_iter = dft_utils.dft_reader_range(reader, start, end, step)
                
                assert range_iter.start == start
                assert range_iter.end <= end  # May be adjusted to file size
                assert range_iter.step == step
                assert range_iter.current == start
    
    def test_range_iterator_iteration(self):
        """Test range iterator iteration"""
        with PythonTestEnvironment(lines=2000) as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                if max_bytes <= 2048:
                    pytest.skip("File too small for range testing")
                
                start = 1024
                end = start + bytes_per_line * 12
                step = bytes_per_line

                chunk_count = 0
                total_bytes = 0
                
                for chunk in dft_utils.dft_reader_range(reader, start, end, step):
                    chunk_count += 1
                    total_bytes += len(chunk)
                    assert 0 < len(chunk) <= step < end - start
                    # if chunk_count >= 3:  # Limit test iterations
                    #     break
                
                assert chunk_count > 0
                assert total_bytes > 0


if __name__ == "__main__":
    pytest.main([__file__])
