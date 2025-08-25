#!/usr/bin/env python3
"""
Comprehensive test cases for dft_utils Python bindings - mirrors C++ tests
"""

import pytest
import os

import dftracer.utils as dft_utils
from .common import Environment

class TestDFTracerBytesReader:
    """Test cases for DFTracerBytesReader - returns raw bytes as strings"""
    
    def test_import(self):
        """Test that we can import the module and classes"""
        assert hasattr(dft_utils, 'DFTracerReader')
        assert hasattr(dft_utils, 'DFTracerBytesReader')
        assert hasattr(dft_utils, 'DFTracerLineBytesReader')
        assert hasattr(dft_utils, 'DFTracerLinesReader')
        assert hasattr(dft_utils, 'DFTracerIndexer')
        assert hasattr(dft_utils, 'CheckpointInfo')
        assert hasattr(dft_utils, 'DFTracerLineBytesRangeIterator')
        assert hasattr(dft_utils, 'DFTracerBytesRangeIterator')
        assert hasattr(dft_utils, 'DFTracerLinesRangeIterator')
        assert hasattr(dft_utils, 'dft_reader_range')
    
    def test_bytes_reader_creation_nonexistent_file(self):
        """Test bytes reader creation with non-existent file"""
        with pytest.raises(RuntimeError):
            dft_utils.DFTracerBytesReader("nonexistent_file.pfw.gz")
    
    def test_bytes_reader_creation_missing_index(self):
        """Test bytes reader creation when index file doesn't exist - now auto-builds"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            # Reader should now auto-build index instead of failing
            with dft_utils.DFTracerBytesReader(gz_file) as reader:
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                # Verify index file was created
                idx_file = gz_file + ".idx"
                assert os.path.exists(idx_file)
    
    def test_bytes_reader_creation_from_indexer(self):
        """Test bytes reader creation from indexer"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            indexer = env.create_indexer(gz_file)
            
            # Test creating bytes reader from indexer
            bytes_reader = dft_utils.DFTracerBytesReader(indexer)
            assert bytes_reader.is_open
            assert bytes_reader.get_max_bytes() > 0
            assert bytes_reader.gzip_path == gz_file
    
    def test_bytes_reader_basic_functionality(self):
        """Test bytes reader basic functionality"""
        with Environment() as env:
            # Create and index test file
            gz_file = env.create_test_gzip_file()
            idx_file = env.build_index(gz_file, checkpoint_size_bytes=512*1024)  # 512KB
            
            # Test context manager (constructor and destructor)
            with dft_utils.DFTracerBytesReader(gz_file) as reader:
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                assert reader.gzip_path == gz_file
                assert reader.index_path == idx_file
                
                # Test getter methods
                assert reader.gzip_path == gz_file
                assert reader.index_path == idx_file
            
            # Should be able to create another one
            with dft_utils.DFTracerBytesReader(gz_file) as reader2:
                assert reader2.is_open
    
    def test_bytes_reader_properties(self):
        """Test bytes reader properties"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            idx_file = env.build_index(gz_file, checkpoint_size_bytes=1536*1024)
            
            with dft_utils.DFTracerBytesReader(gz_file) as reader:
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
    
    def test_bytes_reader_data_reading(self):
        """Test bytes reader data reading"""
        with Environment() as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerBytesReader(gz_file) as reader:
                # DFTracerBytesReader returns raw bytes as a string
                data = reader.read(0, bytes_per_line)

                assert isinstance(data, str)  # Should return string
                assert len(data) == bytes_per_line  # Should get exact bytes requested
                
                # Check content
                if data:
                    assert '"name"' in data  # Should contain JSON content
    
    def test_bytes_reader_iteration_methods(self):
        """Test bytes reader iteration"""
        with Environment(lines=1000) as env:  # Larger file for better iteration testing
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerBytesReader(gz_file) as reader:
                # Test direct iteration
                chunk_count = 0
                total_bytes = 0
                for chunk in reader:
                    chunk_count += 1
                    assert isinstance(chunk, str)  # Bytes reader returns strings
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
                    assert isinstance(chunk, str)
                    assert len(chunk) > 0
                    if chunk_count >= 2:
                        break
                
                assert chunk_count > 0
                
                # Test custom iterator
                chunk_count = 0
                for chunk in reader.iter(256 * 1024):  # 256KB chunks
                    chunk_count += 1
                    assert isinstance(chunk, str)
                    assert len(chunk) > 0
                    if chunk_count >= 2:
                        break
                
                assert chunk_count > 0
    
    def test_bytes_reader_range_operations(self):
        """Test bytes reader range operations"""
        with Environment(lines=2000) as env:  # Large enough for range testing
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerBytesReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                
                # Test reading a range
                if max_bytes > bytes_per_line * 4:
                    start = 0  # Start from beginning
                    end = start + bytes_per_line * 4  # Read 4 lines worth
                    data = reader.read(start, end)  # DFTracerBytesReader.read() returns string
                    assert isinstance(data, str)
                    assert len(data) == bytes_per_line * 4  # Should get exact bytes

                # Test range iterator
                if max_bytes > bytes_per_line * 4:
                    start = 0
                    end = start + bytes_per_line * 3  # Read 3 lines worth
                    step = bytes_per_line
                    
                    chunk_count = 0
                    total_bytes = 0
                    # dft_reader_range signature: (reader, start, end, mode, step)
                    for chunk in dft_utils.dft_reader_range(reader, start, end, "bytes", step):
                        chunk_count += 1
                        assert isinstance(chunk, str)
                        total_bytes += len(chunk)
                        assert len(chunk) > 0
                        if chunk_count >= 3:
                            break
                    
                    assert chunk_count > 0
                    assert total_bytes > 0
    
    def test_bytes_reader_context_manager(self):
        """Test bytes reader as context manager"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file)
            
            # Test with statement
            with dft_utils.DFTracerBytesReader(gz_file) as reader:
                assert reader.is_open
                max_bytes = reader.get_max_bytes()
                assert max_bytes > 0
            
            # Reader should be cleaned up after exiting with block

    def test_bytes_vs_line_read_functionality(self):
        """Test bytes vs line read functionality"""
        with Environment(lines=1000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            # Test with bytes reader for raw data
            bytes_reader = dft_utils.DFTracerBytesReader(gz_file)
            max_bytes = bytes_reader.get_max_bytes()
            
            if max_bytes > 100:
                # Test bytes read method - should return exact size
                start = 50
                end = start + 50  # 50 bytes
                raw_data = bytes_reader.read(start, end)
                assert isinstance(raw_data, str)
                assert len(raw_data) == 50  # Bytes read should be exact size
                
                # Compare with line reader - should return list of complete lines
                with dft_utils.DFTracerReader(gz_file) as line_reader:
                    line_data = line_reader.read(start, end)
                    assert isinstance(line_data, list)  # Line reader returns list
                    
                    # Join line data for comparison
                    if line_data:
                        joined_line_data = '\n'.join(line_data) + '\n'
                        # Line data should end with complete lines
                        assert joined_line_data.endswith('\n')
                        
                        # Both should contain similar content at the start
                        assert raw_data.startswith(joined_line_data[:min(len(raw_data), len(joined_line_data))]) or joined_line_data.startswith(raw_data[:min(len(raw_data), len(joined_line_data))])

    def test_iterator_functionality(self):
        """Test iterator functionality"""
        with Environment(lines=2000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                if max_bytes <= 1024:
                    pytest.skip("File too small for iterator testing")
                
                # Test default iterator
                iter_obj = reader.iter()
                assert hasattr(iter_obj, '__iter__')
                assert hasattr(iter_obj, '__next__')
                
                # Test custom step iterator
                step_size = 512 * 1024  # 512KB for line-based reader
                custom_iter = reader.iter(step_size)
                
                # Test iteration
                chunk_count = 0
                total_lines = 0
                for chunk in custom_iter:
                    chunk_count += 1
                    if isinstance(chunk, list):
                        total_lines += len(chunk)
                    assert len(chunk) > 0
                    if chunk_count >= 5:
                        break
                
                assert chunk_count > 0
                assert total_lines > 0

    def test_line_vs_bytes_reader_comparison(self):
        """Test comparison between line-aware and bytes readers"""
        with Environment(lines=2000) as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            # Test line-aware reader (DFTracerReader/DFTracerLineBytesReader)
            with dft_utils.DFTracerReader(gz_file) as line_reader:
                step_size = bytes_per_line
                
                # Test line-aware iterator
                line_chunks = []
                line_iter = line_reader.iter(step_size)
                for i, chunk in enumerate(line_iter):
                    line_chunks.append(chunk)
                    if i >= 2:  # Get 3 chunks
                        break
                
                # Test bytes reader for comparison
                bytes_reader = dft_utils.DFTracerBytesReader(gz_file)
                bytes_chunks = []
                bytes_iter = bytes_reader.iter(step_size)
                for i, chunk in enumerate(bytes_iter):
                    bytes_chunks.append(chunk)
                    if i >= 2:  # Get 3 chunks
                        break
                
                assert len(line_chunks) == len(bytes_chunks)
                
                # Line-aware chunks should be lists, bytes chunks should be strings
                for line_chunk, bytes_chunk in zip(line_chunks, bytes_chunks):
                    assert isinstance(line_chunk, list)  # Line reader returns list of strings
                    assert isinstance(bytes_chunk, str)  # Bytes reader returns string
                    assert len(line_chunk) > 0
                    assert len(bytes_chunk) > 0

    def test_line_boundary_detection(self):
        """Test Line boundary detection"""
        with Environment(lines=1000) as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                # Small range should provide minimum requested bytes
                content_lines = reader.read(0, bytes_per_line)

                assert isinstance(content_lines, list)  # DFTracerReader returns list of strings
                assert len(content_lines) > 0  # Should get some lines
                
                # Join lines to test content
                content = '\n'.join(content_lines) + '\n' if content_lines else ''
                
                if content:
                    # Should contain complete JSON objects
                    assert content.rfind('}') != -1  # Should contain closing braces
                    # Verify JSON structure
                    assert '"name"' in content
                    assert '"data"' in content

    def test_edge_cases(self):
        """Test edge cases"""
        with Environment() as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                
                if max_bytes > 10:
                    # Read near the end (not complete line)
                    data = reader.read(max_bytes - 10, max_bytes)
                    assert len(data) == 0  # Should return empty list for incomplete lines
                    
                    # Read single byte range (not complete line)
                    if max_bytes > 1:
                        data = reader.read(0, 1)
                        assert len(data) == 0  # Should return empty list for incomplete lines
                
                # Test bytes read edge cases (using DFTracerBytesReader)
                bytes_reader = dft_utils.DFTracerBytesReader(gz_file)
                if max_bytes > 10:
                    # Single byte raw read
                    raw_data = bytes_reader.read(0, 1)
                    assert len(raw_data) == 1
                    
                    # Read near end with raw
                    raw_data = bytes_reader.read(max_bytes - 5, max_bytes - 1)
                    assert len(raw_data) == 4


class TestDFTracerLineBytesReader:
    """Test cases for DFTracerLineBytesReader - returns list of strings (lines)"""
    
    def test_line_bytes_reader_creation(self):
        """Test line bytes reader creation"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            with dft_utils.DFTracerLineBytesReader(gz_file) as reader:
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                assert reader.gzip_path == gz_file
    
    def test_line_bytes_reader_data_reading(self):
        """Test line bytes reader data reading - returns list of strings"""
        with Environment() as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerLineBytesReader(gz_file) as reader:
                # DFTracerLineBytesReader returns list of strings (lines)
                data = reader.read(0, bytes_per_line)

                assert isinstance(data, list)  # Should return list of strings
                assert len(data) > 0  # Should get some lines
                
                # Check content
                for line in data:
                    assert isinstance(line, str)
                    if line:
                        assert '"name"' in line  # Should contain JSON content
    
    def test_line_bytes_reader_iteration(self):
        """Test line bytes reader iteration"""
        with Environment(lines=1000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerLineBytesReader(gz_file) as reader:
                chunk_count = 0
                total_lines = 0
                for chunk in reader:
                    chunk_count += 1
                    assert isinstance(chunk, list)  # Should return list of strings
                    total_lines += len(chunk)
                    assert len(chunk) > 0
                    if chunk_count >= 3:
                        break
                
                assert chunk_count > 0
                assert total_lines > 0
    
    def test_line_bytes_reader_range_operations(self):
        """Test line bytes reader range operations"""
        with Environment(lines=2000) as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerLineBytesReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                
                if max_bytes > bytes_per_line * 4:
                    start = 0
                    end = start + bytes_per_line * 3
                    step = bytes_per_line
                    
                    chunk_count = 0
                    total_lines = 0
                    for chunk in dft_utils.dft_reader_range(reader, start, end, "line_bytes", step):
                        chunk_count += 1
                        assert isinstance(chunk, list)
                        total_lines += len(chunk)
                        assert len(chunk) > 0
                        if chunk_count >= 3:
                            break
                    
                    assert chunk_count > 0
                    assert total_lines > 0


class TestDFTracerLinesReader:
    """Test cases for DFTracerLinesReader - operates on line numbers"""
    
    def test_lines_reader_creation(self):
        """Test lines reader creation"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            with dft_utils.DFTracerLinesReader(gz_file) as reader:
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                assert reader.get_num_lines() > 0
                assert reader.gzip_path == gz_file
    
    def test_lines_reader_data_reading(self):
        """Test lines reader data reading - operates on line numbers"""
        with Environment(lines=100) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerLinesReader(gz_file) as reader:
                num_lines = reader.get_num_lines()
                if num_lines > 10:
                    # Read lines 1-5 (test actual behavior)
                    data = reader.read(1, 6)

                    assert isinstance(data, list)  # Should return list of strings
                    assert len(data) >= 1  # Should get at least 1 line
                    assert len(data) <= 6  # Range appears to be inclusive
                    
                    # Check content
                    for line in data:
                        assert isinstance(line, str)
                        if line:
                            assert '"name"' in line  # Should contain JSON content
    
    def test_lines_reader_iteration(self):
        """Test lines reader iteration"""
        with Environment(lines=1000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerLinesReader(gz_file) as reader:
                chunk_count = 0
                total_lines = 0
                for chunk in reader:
                    chunk_count += 1
                    assert isinstance(chunk, list)  # Should return list of strings
                    total_lines += len(chunk)
                    assert len(chunk) > 0
                    if chunk_count >= 3:
                        break
                
                assert chunk_count > 0
                assert total_lines > 0
    
    def test_lines_reader_range_operations(self):
        """Test lines reader range operations"""
        with Environment(lines=2000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerLinesReader(gz_file) as reader:
                num_lines = reader.get_num_lines()
                
                if num_lines > 20:
                    start = 1  # Lines reader indexing
                    end = start + 10
                    step = 5
                    
                    chunk_count = 0
                    total_lines = 0
                    for chunk in dft_utils.dft_reader_range(reader, start, end, "lines", step):
                        chunk_count += 1
                        assert isinstance(chunk, list)
                        total_lines += len(chunk)
                        assert len(chunk) > 0
                        if chunk_count >= 2:
                            break
                    
                    assert chunk_count > 0
                    assert total_lines > 0


class TestDFTracerJsonLinesReader:
    """Test cases for DFTracerJsonLinesReader - returns list of JSON objects"""
    
    def test_import(self):
        """Test that we can import JSON lines reader classes"""
        assert hasattr(dft_utils, 'DFTracerJsonLinesReader')
        assert hasattr(dft_utils, 'DFTracerJsonLinesBytesReader')
    
    def test_json_lines_reader_creation(self):
        """Test JSON lines reader creation"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            with dft_utils.DFTracerJsonLinesReader(gz_file) as reader:
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                assert reader.get_num_lines() > 0
                assert reader.gzip_path == gz_file
    
    def test_json_lines_reader_creation_from_indexer(self):
        """Test JSON lines reader creation from indexer"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            indexer = env.create_indexer(gz_file)
            
            reader = dft_utils.DFTracerJsonLinesReader(indexer)
            assert reader.is_open
            assert reader.get_max_bytes() > 0
            assert reader.get_num_lines() > 0
            assert reader.gzip_path == gz_file
    
    def test_json_lines_reader_data_reading(self):
        """Test JSON lines reader data reading - returns list of dictionaries"""
        with Environment(lines=100) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerJsonLinesReader(gz_file) as reader:
                num_lines = reader.get_num_lines()
                if num_lines > 10:
                    # Read lines 1-5 (JSON lines reader operates on line numbers)
                    data = reader.read(1, 6)

                    assert isinstance(data, list)  # Should return list of dictionaries
                    assert len(data) >= 1  # Should get at least 1 JSON object
                    
                    # Check that each item is a dictionary
                    for json_obj in data:
                        assert isinstance(json_obj, dict)
                        # Test accessing JSON fields
                        assert "name" in json_obj  # Should have name field from test data
                        assert "data" in json_obj  # Should have data field from test data
                        
                        # Test actual field values
                        name_value = json_obj["name"]
                        assert isinstance(name_value, str)
                        assert name_value.startswith("name_")
                        
                        # Test nested data access
                        data_value = json_obj["data"]
                        assert isinstance(data_value, str)
    
    def test_json_lines_reader_iteration(self):
        """Test JSON lines reader iteration"""
        with Environment(lines=1000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerJsonLinesReader(gz_file) as reader:
                chunk_count = 0
                total_json_objects = 0
                
                for chunk in reader:
                    chunk_count += 1
                    assert isinstance(chunk, list)  # Should return list of dictionaries
                    total_json_objects += len(chunk)
                    assert len(chunk) > 0
                    
                    # Test each JSON object in the chunk
                    for json_obj in chunk:
                        assert isinstance(json_obj, dict)
                        assert "name" in json_obj
                        assert "data" in json_obj
                    
                    if chunk_count >= 3:
                        break
                
                assert chunk_count > 0
                assert total_json_objects > 0
    
    def test_json_lines_reader_properties(self):
        """Test JSON lines reader properties"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file)
            
            with dft_utils.DFTracerJsonLinesReader(gz_file) as reader:
                # Test property types
                assert isinstance(reader.is_open, bool)
                assert isinstance(reader.gzip_path, str)
                assert isinstance(reader.index_path, str)
                assert isinstance(reader.get_max_bytes(), int)
                assert isinstance(reader.get_num_lines(), int)
                assert isinstance(reader.get_default_step(), int)
                
                # Test values
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                assert reader.get_num_lines() > 0
                assert reader.gzip_path == gz_file
    
    def test_json_lines_reader_nested_data(self):
        """Test JSON lines reader with deeply nested JSON structures"""
        with Environment(lines=50) as env:
            # Create a test file with complex nested JSON
            gz_file = env.create_test_gzip_file_with_nested_json()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerJsonLinesReader(gz_file) as reader:
                num_lines = reader.get_num_lines()
                if num_lines > 2:
                    data = reader.read(1, 3)
                    
                    assert isinstance(data, list)
                    assert len(data) >= 1
                    
                    for json_obj in data:
                        assert isinstance(json_obj, dict)
                        
                        # Test top-level fields
                        assert "id" in json_obj
                        assert "metadata" in json_obj
                        assert "events" in json_obj
                        assert "config" in json_obj
                        
                        # Test nested object access
                        metadata = json_obj["metadata"]
                        assert isinstance(metadata, dict)
                        assert "timestamp" in metadata
                        assert "user" in metadata
                        
                        # Test deeply nested object
                        user = metadata["user"]
                        assert isinstance(user, dict)
                        assert "profile" in user
                        
                        profile = user["profile"]
                        assert isinstance(profile, dict)
                        assert "settings" in profile
                        
                        settings = profile["settings"]
                        assert isinstance(settings, dict)
                        assert "theme" in settings
                        assert isinstance(settings["theme"], str)
                        
                        # Test array access
                        events = json_obj["events"]
                        assert isinstance(events, list)
                        assert len(events) > 0
                        
                        # Test objects in arrays
                        for event in events:
                            assert isinstance(event, dict)
                            assert "type" in event
                            assert "data" in event
                            
                            # Test nested object in array
                            event_data = event["data"]
                            assert isinstance(event_data, dict)
                            assert "payload" in event_data
                            
                            # Test mixed types in nested structures
                            payload = event_data["payload"]
                            assert isinstance(payload, dict)
                            assert "values" in payload
                            
                            values = payload["values"]
                            assert isinstance(values, list)
                            # Values should contain mixed types (int, float, str, dict)
                            
                        # Test config with various nested types
                        config = json_obj["config"]
                        assert isinstance(config, dict)
                        assert "features" in config
                        assert "limits" in config
                        
                        features = config["features"]
                        assert isinstance(features, dict)
                        for feature_name, feature_config in features.items():
                            assert isinstance(feature_name, str)
                            assert isinstance(feature_config, dict)
                            assert "enabled" in feature_config
                            assert isinstance(feature_config["enabled"], bool)


class TestDFTracerJsonLinesBytesReader:
    """Test cases for DFTracerJsonLinesBytesReader - returns list of JSON objects from byte ranges"""
    
    def test_json_lines_bytes_reader_creation(self):
        """Test JSON lines bytes reader creation"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            
            with dft_utils.DFTracerJsonLinesBytesReader(gz_file) as reader:
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                assert reader.get_num_lines() > 0
                assert reader.gzip_path == gz_file
    
    def test_json_lines_bytes_reader_creation_from_indexer(self):
        """Test JSON lines bytes reader creation from indexer"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            indexer = env.create_indexer(gz_file)
            
            reader = dft_utils.DFTracerJsonLinesBytesReader(indexer)
            assert reader.is_open
            assert reader.get_max_bytes() > 0
            assert reader.get_num_lines() > 0
            assert reader.gzip_path == gz_file
    
    def test_json_lines_bytes_reader_data_reading(self):
        """Test JSON lines bytes reader data reading - operates on byte ranges"""
        with Environment(lines=100) as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerJsonLinesBytesReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                if max_bytes > bytes_per_line * 4:
                    # Read bytes 0 to 4 lines worth (JSON lines bytes reader operates on byte ranges)
                    data = reader.read(0, bytes_per_line * 4)

                    assert isinstance(data, list)  # Should return list of dictionaries
                    assert len(data) >= 1  # Should get at least 1 JSON object
                    
                    # Check that each item is a dictionary
                    for json_obj in data:
                        assert isinstance(json_obj, dict)
                        # Test accessing JSON fields
                        assert "name" in json_obj  # Should have name field from test data
                        assert "data" in json_obj  # Should have data field from test data
                        
                        # Test actual field values
                        name_value = json_obj["name"]
                        assert isinstance(name_value, str)
                        assert name_value.startswith("name_")
                        
                        # Test nested data access
                        data_value = json_obj["data"]
                        assert isinstance(data_value, str)
    
    def test_json_lines_bytes_reader_iteration(self):
        """Test JSON lines bytes reader iteration"""
        with Environment(lines=1000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerJsonLinesBytesReader(gz_file) as reader:
                chunk_count = 0
                total_json_objects = 0
                
                for chunk in reader:
                    chunk_count += 1
                    assert isinstance(chunk, list)  # Should return list of dictionaries
                    total_json_objects += len(chunk)
                    assert len(chunk) > 0
                    
                    # Test each JSON object in the chunk
                    for json_obj in chunk:
                        assert isinstance(json_obj, dict)
                        assert "name" in json_obj
                        assert "data" in json_obj
                    
                    if chunk_count >= 3:
                        break
                
                assert chunk_count > 0
                assert total_json_objects > 0
    
    def test_json_lines_bytes_vs_lines_reader_comparison(self):
        """Test comparison between JSON lines bytes and JSON lines readers"""
        with Environment(lines=500) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            # Test JSON lines reader (operates on line numbers)
            with dft_utils.DFTracerJsonLinesReader(gz_file) as lines_reader:
                num_lines = lines_reader.get_num_lines()
                if num_lines > 10:
                    lines_data = lines_reader.read(1, 6)  # Lines 1-5
                    assert isinstance(lines_data, list)
                    lines_count = len(lines_data)
            
            # Test JSON lines bytes reader (operates on byte ranges)
            with dft_utils.DFTracerJsonLinesBytesReader(gz_file) as bytes_reader:
                max_bytes = bytes_reader.get_max_bytes()
                if max_bytes > 1024:
                    bytes_data = bytes_reader.read(0, 1024 * 5)  # First 5KB
                    assert isinstance(bytes_data, list)
                    bytes_count = len(bytes_data)
            
            # Both should return valid JSON objects
            if 'lines_count' in locals() and 'bytes_count' in locals():
                assert lines_count > 0
                assert bytes_count > 0
                # Note: counts may differ since they operate on different ranges
    
    def test_json_lines_bytes_reader_properties(self):
        """Test JSON lines bytes reader properties"""
        with Environment() as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file)
            
            with dft_utils.DFTracerJsonLinesBytesReader(gz_file) as reader:
                # Test property types
                assert isinstance(reader.is_open, bool)
                assert isinstance(reader.gzip_path, str)
                assert isinstance(reader.index_path, str)
                assert isinstance(reader.get_max_bytes(), int)
                assert isinstance(reader.get_num_lines(), int)
                assert isinstance(reader.get_default_step(), int)
                
                # Test values
                assert reader.is_open
                assert reader.get_max_bytes() > 0
                assert reader.get_num_lines() > 0
                assert reader.gzip_path == gz_file
    
    def test_json_lines_bytes_reader_range_operations(self):
        """Test JSON lines bytes reader range operations"""
        with Environment(lines=2000) as env:
            bytes_per_line = 1024
            gz_file = env.create_test_gzip_file(bytes_per_line=bytes_per_line)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerJsonLinesBytesReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                
                if max_bytes > bytes_per_line * 8:
                    start = 0
                    end = bytes_per_line * 6  # 6 lines worth of bytes
                    step = bytes_per_line * 2  # 2 lines worth per chunk
                    
                    chunk_count = 0
                    total_json_objects = 0
                    
                    # Use dft_reader_range for JSON lines bytes reader would need to be supported
                    # For now, test direct iteration
                    reader.set_default_step(step)
                    for chunk in reader.iter(step):
                        chunk_count += 1
                        assert isinstance(chunk, list)
                        total_json_objects += len(chunk)
                        
                        # Test each JSON object
                        for json_obj in chunk:
                            assert isinstance(json_obj, dict)
                            assert "name" in json_obj
                        
                        if chunk_count >= 3:
                            break
                    
                    assert chunk_count > 0
                    assert total_json_objects > 0
    
    def test_json_lines_bytes_reader_nested_data(self):
        """Test JSON lines bytes reader with deeply nested JSON structures"""
        with Environment(lines=50) as env:
            # Create a test file with complex nested JSON
            gz_file = env.create_test_gzip_file_with_nested_json()
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            with dft_utils.DFTracerJsonLinesBytesReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                if max_bytes > 2048:  # Ensure we have enough data
                    data = reader.read(0, 2048)
                    
                    assert isinstance(data, list)
                    assert len(data) >= 1
                    
                    for json_obj in data:
                        assert isinstance(json_obj, dict)
                        
                        # Test the same nested structure as JsonLinesReader
                        # This ensures both readers produce identical dictionary structures
                        
                        # Test top-level fields
                        assert "id" in json_obj
                        assert "metadata" in json_obj
                        assert "events" in json_obj
                        assert "config" in json_obj
                        
                        # Test deeply nested access
                        settings = json_obj["metadata"]["user"]["profile"]["settings"]
                        assert isinstance(settings, dict)
                        assert "privacy" in settings
                        
                        privacy = settings["privacy"]
                        assert isinstance(privacy, dict)
                        assert "options" in privacy
                        assert isinstance(privacy["options"], list)
                        assert len(privacy["options"]) == 3
                        
                        # Test mixed types in arrays
                        events = json_obj["events"]
                        assert isinstance(events, list)
                        for event in events:
                            payload = event["data"]["payload"]
                            values = payload["values"]
                            assert isinstance(values, list)
                            
                            # Check mixed types in the values array
                            if len(values) >= 4:
                                assert isinstance(values[0], int)      # i
                                assert isinstance(values[1], float)    # i * 2.5
                                assert isinstance(values[2], str)      # f"string_{i}"
                                assert isinstance(values[3], dict)     # {"nested": True, "count": i}
                                
                                nested_in_array = values[3]
                                assert "nested" in nested_in_array
                                assert "count" in nested_in_array
                                assert isinstance(nested_in_array["nested"], bool)
                                assert isinstance(nested_in_array["count"], int)


class TestDFTracerRangeIterator:
    """Test cases for DFTracerRangeIterator"""
    
    def test_range_iterator_properties(self):
        """Test range iterator properties"""
        with Environment(lines=1000) as env:
            gz_file = env.create_test_gzip_file()
            env.build_index(gz_file)
            
            with dft_utils.DFTracerReader(gz_file) as reader:
                max_bytes = reader.get_max_bytes()
                if max_bytes <= 1024:
                    pytest.skip("File too small for range testing")
                
                start = 512
                end = start + 512
                step = 128
                
                range_iter = dft_utils.dft_reader_range(reader, start, end, "line_bytes", step)
                
                assert range_iter.start == start
                assert range_iter.end <= end  # May be adjusted to file size
                assert range_iter.step == step
                assert range_iter.current == start
    
    def test_range_iterator_iteration(self):
        """Test range iterator iteration"""
        with Environment(lines=2000) as env:
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
                
                for chunk in dft_utils.dft_reader_range(reader, start, end, "line_bytes", step):
                    chunk_count += 1
                    total_bytes += len(chunk)
                    assert 0 < len(chunk) <= step < end - start
                    # if chunk_count >= 3:  # Limit test iterations
                    #     break
                
                assert chunk_count > 0
                assert total_bytes > 0


if __name__ == "__main__":
    pytest.main([__file__])
