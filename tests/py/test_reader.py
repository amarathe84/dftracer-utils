#!/usr/bin/env python3
"""
Test cases for dft_utils Python bindings
"""

import pytest
import os
import tempfile
import gzip
import random
import string
from pathlib import Path

import dft_utils


class PythonTestEnvironment:
    """Test environment manager"""
    
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
                os.rmdir(self.temp_dir)
            except OSError:
                pass
    
    def create_test_gzip_file(self, filename="test_data.pfw.gz"):
        """Create a test gzip file with sample data"""
        file_path = os.path.join(self.temp_dir, filename)
        
        # Generate test data (similar to JSON lines format)
        lines = []
        for i in range(self.lines):
            # Create realistic trace-like data
            line = f'{{"id": {i}, "timestamp": {1000000 + i * 1000}, "event": "test_event_{i}", "data": "{"".join(random.choices(string.ascii_letters + string.digits, k=50))}"}}\\n'
            lines.append(line)
        
        # Write compressed data
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            f.writelines(lines)
        
        self.test_files.append(file_path)
        return file_path
    
    def get_index_path(self, gz_file_path):
        """Get the index file path for a gzip file"""
        return gz_file_path + ".idx"
    
    def is_valid(self):
        """Check if test environment is valid"""
        return self.temp_dir and os.path.exists(self.temp_dir)


class TestDFTracerReader:
    """Test cases for DFTracerReader"""
    
    def test_import(self):
        """Test that we can import the module and classes"""
        assert hasattr(dft_utils, 'DFTracerReader')
        assert hasattr(dft_utils, 'DFTracerLineRangeIterator')
        assert hasattr(dft_utils, 'DFTracerRawRangeIterator')
        assert hasattr(dft_utils, 'dft_reader_raw_range')
        assert hasattr(dft_utils, 'dft_reader_range')
        assert hasattr(dft_utils, 'set_log_level')
    
    def test_reader_creation_nonexistent_file(self):
        """Test reader creation with non-existent file"""
        with pytest.raises(RuntimeError, match="Gzip file does not exist"):
            dft_utils.DFTracerReader("nonexistent_file.pfw.gz")
    
    def test_reader_creation_missing_index(self):
        """Test reader creation when index file doesn't exist"""
        with PythonTestEnvironment() as env:
            gz_file = env.create_test_gzip_file()
            
            # Should raise error when index doesn't exist
            with pytest.raises(RuntimeError):
                dft_utils.DFTracerReader(gz_file)
    
    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"), 
                       reason="No test trace file available")
    def test_reader_with_real_file(self):
        """Test reader with real trace file (if available)"""
        trace_file = "trace.pfw.gz"
        index_file = trace_file + ".idx"
        
        if not os.path.exists(index_file):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            assert reader.is_open
            assert reader.get_max_bytes() > 0
            assert reader.gzip_path == trace_file
            assert reader.index_path == index_file
    
    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"), 
                       reason="No test trace file available")
    def test_reader_properties(self):
        """Test reader properties"""
        trace_file = "trace.pfw.gz" 
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        reader = dft_utils.DFTracerReader(trace_file)
        try:
            assert isinstance(reader.is_open, bool)
            assert isinstance(reader.gzip_path, str)
            assert isinstance(reader.index_path, str) 
            assert isinstance(reader.get_max_bytes(), int)
            assert isinstance(reader.get_default_step(), int)
            
            # Test setting default step
            original_step = reader.get_default_step()
            reader.set_default_step(512 * 1024)
            assert reader.get_default_step() == 512 * 1024
            reader.set_default_step(original_step)
        finally:
            reader.close()
    
    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available") 
    def test_reader_iteration(self):
        """Test reader iteration methods"""
        trace_file = "trace.pfw.gz"
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            # Test direct iteration
            chunk_count = 0
            total_bytes = 0
            for chunk in reader:
                chunk_count += 1
                total_bytes += len(chunk)
                if chunk_count >= 3:  # Just test first few chunks
                    break
            
            assert chunk_count > 0
            assert total_bytes > 0
            
            # Test default iterator
            chunk_count = 0
            for chunk in reader.iter():
                chunk_count += 1
                if chunk_count >= 2:
                    break
            
            assert chunk_count > 0
            
            # Test custom iterator
            chunk_count = 0
            for chunk in reader.iter(256 * 1024):  # 256KB chunks
                chunk_count += 1
                if chunk_count >= 2:
                    break
            
            assert chunk_count > 0
    
    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available")
    def test_reader_range_operations(self):
        """Test reader range operations"""
        trace_file = "trace.pfw.gz"
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            max_bytes = reader.get_max_bytes()
            
            # Test reading a small range
            if max_bytes > 1024 * 1024:  # Only if file is large enough
                start = 1024 * 1024  # 1MB
                end = start + 512 * 1024  # 512KB range
                data = reader.read(start, end)
                assert len(data) > 0
                # Note: reader might return complete records that extend beyond the range
                # so we just verify we got some data
            
            # Test range iterator
            if max_bytes > 5 * 1024 * 1024:  # Only if file is large enough
                start = 2 * 1024 * 1024  # 2MB
                end = start + 1024 * 1024  # 1MB range
                step = 256 * 1024  # 256KB steps
                
                chunk_count = 0
                total_bytes = 0
                for chunk in dft_utils.dft_reader_range(reader, start, end, step):
                    chunk_count += 1
                    total_bytes += len(chunk)
                    if chunk_count >= 3:
                        break
                
                assert chunk_count > 0
                assert total_bytes > 0
    
    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available")
    def test_context_manager(self):
        """Test reader as context manager"""
        trace_file = "trace.pfw.gz"
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        # Test with statement
        with dft_utils.DFTracerReader(trace_file) as reader:
            assert reader.is_open
            max_bytes = reader.get_max_bytes()
            assert max_bytes > 0
        
        # Reader should be closed after exiting with block
        # Note: We can't easily test this since is_open might still return True
        # but the underlying resources should be cleaned up
    
    def test_log_level_functions(self):
        """Test log level functions"""
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

    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available")
    def test_raw_read_functionality(self):
        """Test raw read functionality"""
        trace_file = "trace.pfw.gz"
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            max_bytes = reader.get_max_bytes()
            
            if max_bytes > 1024:  # Only if file is large enough
                # Test read_raw method
                start = 100
                end = start + 50  # 50 bytes
                raw_data = reader.read_raw(start, end)
                assert len(raw_data) > 0
                # Raw read should be closer to requested size (doesn't extend to JSON boundaries)
                assert len(raw_data) <= 60  # Should be close to 50 bytes
                
                # Compare with regular read - regular should be larger due to JSON boundary extension
                regular_data = reader.read(start, end)
                assert len(regular_data) >= len(raw_data)
                
                # Note: Raw and regular reads may start at different positions
                # Raw reads exactly from requested byte position
                # Regular reads align to JSON boundaries
                # So we just verify that both returned valid data

    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available")
    def test_raw_iterator_functionality(self):
        """Test raw iterator functionality"""
        trace_file = "trace.pfw.gz"
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            max_bytes = reader.get_max_bytes()
            if max_bytes <= 1024 * 1024:  # Need sufficient data
                pytest.skip("File too small for raw iterator testing")
            
            # Test default raw iterator
            raw_iter = reader.raw_iter()
            assert hasattr(raw_iter, '__iter__')
            assert hasattr(raw_iter, '__next__')
            
            # Test custom step raw iterator
            step_size = 256 * 1024  # 256KB
            custom_raw_iter = reader.raw_iter(step_size)
            
            # Test iteration
            chunk_count = 0
            total_bytes = 0
            for chunk in custom_raw_iter:
                chunk_count += 1
                total_bytes += len(chunk)
                assert len(chunk) > 0
                if chunk_count >= 3:  # Just test first few chunks
                    break
            
            assert chunk_count > 0
            assert total_bytes > 0

    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available")
    def test_raw_vs_line_iterator_comparison(self):
        """Test comparison between raw and line-aware iterators"""
        trace_file = "trace.pfw.gz"
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            max_bytes = reader.get_max_bytes()
            if max_bytes <= 2 * 1024 * 1024:  # Need sufficient data
                pytest.skip("File too small for iterator comparison")
            
            step_size = 512 * 1024  # 512KB
            
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
                # Line chunks should typically be larger (due to JSON boundary extension)
                # but this isn't guaranteed for every chunk
                
                # Note: Line-aware and raw iterators may start at different positions
                # Line-aware iterators align to JSON boundaries
                # Raw iterators read exactly from byte positions
                # Both should return valid data, but content may differ in alignment

    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available")
    def test_raw_iterator_edge_cases(self):
        """Test raw iterator edge cases"""
        trace_file = "trace.pfw.gz"
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            max_bytes = reader.get_max_bytes()
            if max_bytes <= 1024:
                pytest.skip("File too small for edge case testing")
            
            # Test very small step size
            small_step = 64  # 64 bytes
            small_iter = reader.raw_iter(small_step)
            
            chunk_count = 0
            for chunk in small_iter:
                chunk_count += 1
                assert len(chunk) > 0
                assert len(chunk) <= small_step + 10  # Some tolerance
                if chunk_count >= 5:  # Test first few chunks
                    break
            
            assert chunk_count > 0
            
            # Test large step size
            if max_bytes > 1024 * 1024:
                large_step = 1024 * 1024  # 1MB
                large_iter = reader.raw_iter(large_step)
                
                large_chunk = next(large_iter)
                assert len(large_chunk) > 0

    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available")
    def test_raw_methods_available(self):
        """Test that raw methods are available in the API"""
        trace_file = "trace.pfw.gz"
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            # Test that raw methods exist
            assert hasattr(reader, 'read_raw')
            assert hasattr(reader, 'raw_iter')
            
            # Test that they are callable
            assert callable(reader.read_raw)
            assert callable(reader.raw_iter)
            
            # Test basic functionality without errors
            max_bytes = reader.get_max_bytes()
            if max_bytes > 100:
                data = reader.read_raw(0, 50)
                assert isinstance(data, str)
                assert len(data) > 0
                
                raw_iter = reader.raw_iter()
                assert raw_iter is not None
                
                custom_iter = reader.raw_iter(1024)
                assert custom_iter is not None


class TestDFTracerRangeIterator:
    """Test cases for DFTracerRangeIterator"""
    
    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available")
    def test_range_iterator_properties(self):
        """Test range iterator properties"""
        trace_file = "trace.pfw.gz"
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            max_bytes = reader.get_max_bytes()
            if max_bytes <= 1024 * 1024:
                pytest.skip("File too small for range testing")
            
            start = 1024 * 1024
            end = start + 512 * 1024 
            step = 128 * 1024
            
            range_iter = dft_utils.dft_reader_range(reader, start, end, step)
            
            assert range_iter.start == start
            assert range_iter.end <= end  # May be adjusted to file size
            assert range_iter.step == step
            assert range_iter.current == start
    
    @pytest.mark.skipif(not os.path.exists("trace.pfw.gz"),
                       reason="No test trace file available")
    def test_range_iterator_iteration(self):
        """Test range iterator iteration"""
        trace_file = "trace.pfw.gz" 
        if not os.path.exists(trace_file + ".idx"):
            pytest.skip("Index file not found")
        
        with dft_utils.DFTracerReader(trace_file) as reader:
            max_bytes = reader.get_max_bytes()
            if max_bytes <= 2 * 1024 * 1024:
                pytest.skip("File too small for range testing")
            
            start = 1024 * 1024
            end = start + 1024 * 1024
            step = 256 * 1024
            
            chunk_count = 0
            total_bytes = 0
            
            for chunk in dft_utils.dft_reader_range(reader, start, end, step):
                chunk_count += 1
                total_bytes += len(chunk)
                assert len(chunk) > 0
                if chunk_count >= 3:  # Limit test iterations
                    break
            
            assert chunk_count > 0
            assert total_bytes > 0


if __name__ == "__main__":
    pytest.main([__file__])
