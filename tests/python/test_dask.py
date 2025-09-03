#!/usr/bin/env python3
"""
End-to-end Dask integration tests for  utilities
Tests combining indexer and reader functionality with Dask distributed computing
"""

from fileinput import filename
import pytest
import os

try:
    import dask
    import dask.dataframe as dd
    import pandas as pd
    DASK_AVAILABLE = True
except ImportError:
    DASK_AVAILABLE = False

import dftracer.utils as dft_utils
from .common import Environment


@pytest.mark.skipif(not DASK_AVAILABLE, reason="Dask not available")
class TestDaskIntegration:
    """End-to-end tests with Dask distributed computing"""
    
    def test_dask_basic_import(self):
        """Test that Dask imports work correctly"""
        assert DASK_AVAILABLE
        assert hasattr(dask, '__version__')
        assert hasattr(dd, 'DataFrame')
    
    def test_parallel_indexer_creation(self):
        """Test creating multiple indexers in parallel with Dask"""
        with Environment(lines=1000) as env:
            # Create multiple test files
            gz_files = []
            for i in range(3):
                gz_file = env.create_test_gzip_file(f"test_{i}.pfw.gz", bytes_per_line=512)
                gz_files.append(gz_file)
            
            def create_and_build_indexer(gz_file):
                """Helper function to create and build an indexer"""
                try:
                    indexer = dft_utils.Indexer(gz_file, checkpoint_size=256*1024)
                    if indexer.need_rebuild():
                        indexer.build()
                    return {
                        'file': gz_file,
                        'max_bytes': indexer.get_max_bytes(),
                        'num_lines': indexer.get_num_lines(),
                        'success': True
                    }
                except Exception as e:
                    return {
                        'file': gz_file,
                        'error': str(e),
                        'success': False
                    }
            
            # Use Dask delayed for parallel processing
            delayed_tasks = [dask.delayed(create_and_build_indexer)(gz_file) for gz_file in gz_files]
            results = dask.compute(*delayed_tasks)
            
            # Verify all indexers were created successfully
            assert len(results) == 3
            for result in results:
                assert result['success']
                assert result['max_bytes'] > 0
                assert result['num_lines'] > 0
                
                # Verify index file was created
                idx_file = result['file'] + '.idx'
                assert os.path.exists(idx_file)
    
    def test_parallel_reader_operations(self):
        """Test parallel reading operations with all reader types including JSON"""
        with Environment(lines=2000) as env:
            gz_file = env.create_test_gzip_file(bytes_per_line=1024)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            def read_chunk(gz_file_path, start_bytes, end_bytes, reader_type):
                """Helper function to read a chunk - creates its own indexer for thread safety"""
                try:
                    # Each task creates its own indexer instance to avoid sharing
                    indexer = dft_utils.Indexer(gz_file_path, checkpoint_size=512*1024)
                    
                    reader = dft_utils.Reader(gz_file_path, indexer=indexer)
                    
                    if reader_type == 'bytes':
                        data = reader.read(start_bytes, end_bytes)
                    elif reader_type == 'line_bytes':
                        data = reader.read_line_bytes(start_bytes, end_bytes)
                    elif reader_type == 'json_bytes':
                        data = reader.read_line_bytes_json(start_bytes, end_bytes)
                    else:
                        raise ValueError(f"Unknown reader type: {reader_type}")
                    
                    return {
                        'type': reader_type,
                        'data_type': type(data).__name__,
                        'data_len': len(data),
                        'success': True
                    }
                except Exception as e:
                    return {
                        'type': reader_type,
                        'error': str(e),
                        'success': False
                    }
            
            # Get file info from a temporary indexer
            temp_indexer = dft_utils.Indexer(gz_file, checkpoint_size=512*1024)
            max_bytes = temp_indexer.get_max_bytes()
            chunk_size = max_bytes // 4
            
            # Create tasks for all reader types
            tasks = []
            reader_types = ['bytes', 'line_bytes', 'json_bytes']
            
            for i in range(2):  # 2 chunks
                start = i * chunk_size
                end = min((i + 1) * chunk_size, max_bytes)
                if start < end:
                    for reader_type in reader_types:
                        task = dask.delayed(read_chunk)(gz_file, start, end, reader_type)
                        tasks.append(task)
            
            # Execute all tasks in parallel
            results = dask.compute(*tasks)
            
            # Verify results
            successful_results = [r for r in results if r['success']]
            failed_results = [r for r in results if not r['success']]
            
            # Print debug info about any failures
            if failed_results:
                print(f"Failed operations: {len(failed_results)}")
                for failed in failed_results:
                    print(f"  {failed['type']}: {failed.get('error', 'Unknown error')}")
            
            # All operations should succeed with the fix
            assert len(successful_results) == len(results), f"Some operations failed: {failed_results}"
            
            # Verify different readers return different data types
            bytes_results = [r for r in successful_results if r['type'] == 'bytes']
            line_bytes_results = [r for r in successful_results if r['type'] == 'line_bytes']
            json_bytes_results = [r for r in successful_results if r['type'] == 'json_bytes']
            
            if bytes_results:
                assert all(r['data_type'] == 'bytes' for r in bytes_results)
            if line_bytes_results:
                assert all(r['data_type'] == 'list' for r in line_bytes_results)
            if json_bytes_results:
                assert all(r['data_type'] == 'list' for r in json_bytes_results)
    
    def test_dask_dataframe_integration(self):
        """Test integrating  JSON readers with Dask DataFrames"""
        with Environment(lines=500) as env:
            gz_file = env.create_test_gzip_file(bytes_per_line=1024)
            env.build_index(gz_file, checkpoint_size_bytes=512*1024)
            
            def extract_json_data(gz_file_path, start_bytes, end_bytes):
                """Extract JSON data and convert to DataFrame-friendly format"""
                try:
                    indexer = dft_utils.Indexer(gz_file_path, checkpoint_size=512*1024)
                    reader = dft_utils.Reader(gz_file_path, indexer=indexer)
                    json_objects = reader.read_line_bytes_json(start_bytes, end_bytes)
                    
                    # Convert to list of dictionaries suitable for DataFrame
                    records = []
                    for obj in json_objects:
                        if obj is not None and "name" in obj:  # JSON objects
                            record = {
                                'name': obj.get('name', ''),
                                'cat': obj.get('cat', ''),
                                'dur': obj.get('dur', 0),
                                'data_length': len(obj.get('data', ''))
                            }
                            records.append(record)
                    
                    return records
                except Exception:
                    return []
            
            # Get file info and create chunks
            temp_indexer = dft_utils.Indexer(gz_file, checkpoint_size=512*1024)
            max_bytes = temp_indexer.get_max_bytes()
            chunk_size = max_bytes // 4
            
            # Create delayed tasks to extract data from each chunk
            tasks = []
            for i in range(4):
                start = i * chunk_size
                end = min((i + 1) * chunk_size, max_bytes)
                if start < end:
                    task = dask.delayed(extract_json_data)(gz_file, start, end)
                    tasks.append(task)
            
            # Compute all chunks
            chunk_results = dask.compute(*tasks)
            
            # Flatten results into single list
            all_records = []
            for chunk_records in chunk_results:
                all_records.extend(chunk_records)
            
            if all_records:
                # Create Dask DataFrame from the extracted data
                df = dd.from_pandas(pd.DataFrame(all_records), npartitions=2)
                
                # Test basic DataFrame operations
                assert len(df.columns) == 4
                assert 'name' in df.columns
                assert 'dur' in df.columns
                
                # Test aggregation operations
                name_count = df['name'].count().compute()
                assert name_count > 0
                
                avg_duration = df['dur'].mean().compute()
                assert isinstance(avg_duration, (int, float))


    def test_multiple_batch_sizes_no_duplication(self):
        """Test JSON processing with multiple batch sizes to ensure no duplicates and complete data recovery"""
        import dask.bag as db
        
        with Environment(lines=200) as env:  # More records for better boundary testing
            gz_file = env.create_test_gzip_file(bytes_per_line=512)
            env.build_index(gz_file, checkpoint_size_bytes=256*1024)
            
            temp_indexer = dft_utils.Indexer(gz_file, checkpoint_size=256*1024)
            max_bytes = temp_indexer.get_max_bytes()
            
            # Test various batch sizes including boundary-critical ones
            batch_sizes = [
                4 * 1024,     # 4KB - smaller than typical record boundaries
                8 * 1024,     # 8KB - medium size
                16 * 1024,    # 16KB - the problematic boundary size from original issue
                32 * 1024,    # 32KB - larger chunks
                64 * 1024,    # 64KB - very large chunks
            ]
            
            def generate_batches(filename, max_bytes, batch_size):
                for start in range(0, max_bytes, batch_size):
                    end = min(start + batch_size, max_bytes)
                    yield filename, start, end
            
            def process_batch(batch_info):
                """Process one batch and return processed records"""
                filename, start, end = batch_info
                index_file = f"{filename}.idx"
                reader = dft_utils.Reader(filename, index_file)
                json_lines = reader.read_line_bytes_json(start, end)
                
                processed_records = []
                for json_obj in json_lines:
                    if json_obj is not None and "name" in json_obj:
                        final_dict = {
                            "name": json_obj["name"],
                            "cat": json_obj.get("cat", ""),
                            "ts": json_obj.get("ts", 0),
                            "pid": json_obj.get("pid", 0),
                            "tid": json_obj.get("tid", 0),
                            "dur": json_obj.get("dur", 0),
                        }
                        processed_records.append(final_dict)
                
                return processed_records
            
            # Get reference data (full file read) and verify against environment
            full_reader = dft_utils.Reader(gz_file, indexer=temp_indexer)
            reference_data = full_reader.read_line_bytes_json(0, max_bytes)
            reference_names = sorted([obj["name"] for obj in reference_data if obj and "name" in obj])
            expected_count = len(reference_names)
            
            # Double-check against environment.lines
            assert expected_count == env.lines, f"Reference data mismatch: got {expected_count} records, environment has {env.lines} lines"
            
            print(f"\n=== Testing {expected_count} records ({env.lines} env.lines) across multiple batch sizes ===")
            
            for batch_size in batch_sizes:
                print(f"\n--- Testing batch size: {batch_size} bytes ({batch_size//1024}KB) ---")
                
                # Generate batches for this batch size
                batches = list(generate_batches(gz_file, max_bytes, batch_size))
                print(f"Created {len(batches)} batches")
                
                # Process with Dask
                results = (
                    db.from_sequence(batches, npartitions=min(len(batches), 8))
                    .map(process_batch)
                    .flatten()
                    .filter(lambda x: "name" in x)
                    .compute()
                )
                
                # Verify count
                actual_count = len(results)
                print(f"Retrieved {actual_count}/{expected_count} records (env.lines: {env.lines})")
                
                # Check for duplicates
                unique_records = set()
                duplicates = []
                actual_names = []
                
                for record in results:
                    name = record.get('name', '')
                    actual_names.append(name)
                    
                    # Create unique key
                    key = (record.get('name', ''), record.get('cat', ''), record.get('dur', 0))
                    if key in unique_records:
                        duplicates.append(record)
                    else:
                        unique_records.add(key)
                
                # Verify no duplicates
                assert len(duplicates) == 0, f"Batch size {batch_size}: Found {len(duplicates)} duplicates!"
                
                # Verify complete data recovery  
                actual_names_sorted = sorted(actual_names)
                missing_names = set(reference_names) - set(actual_names_sorted)
                extra_names = set(actual_names_sorted) - set(reference_names)
                
                if missing_names:
                    print(f"  Missing records: {sorted(missing_names)}")
                if extra_names:
                    print(f"  Extra records: {sorted(extra_names)}")
                    
                assert len(missing_names) == 0, f"Batch size {batch_size}: Missing {len(missing_names)} records: {sorted(missing_names)}"
                assert len(extra_names) == 0, f"Batch size {batch_size}: Found {len(extra_names)} extra records: {sorted(extra_names)}"
                assert actual_count == expected_count, f"Batch size {batch_size}: Expected {expected_count} records, got {actual_count}"
                assert actual_count == env.lines, f"Batch size {batch_size}: Expected {env.lines} env.lines, got {actual_count}"
                
                print(f"  Batch size {batch_size//1024}KB: {actual_count} records, no duplicates, complete recovery")
            
            print("\nAll batch sizes passed: No duplicates, complete data recovery")

    def test_boundary_edge_cases(self):
        """Test edge cases around chunk boundaries that previously caused issues"""
        import dask.bag as db
        
        with Environment(lines=100) as env:
            gz_file = env.create_test_gzip_file(bytes_per_line=512)
            env.build_index(gz_file, checkpoint_size_bytes=256*1024)
            
            temp_indexer = dft_utils.Indexer(gz_file, checkpoint_size=256*1024)
            max_bytes = temp_indexer.get_max_bytes()
            
            def process_batch(batch_info):
                """Process one batch and return processed records"""
                filename, start, end = batch_info
                index_file = f"{filename}.idx"
                reader = dft_utils.Reader(filename, index_file)
                json_lines = reader.read_line_bytes_json(start, end)
                
                processed_records = []
                for json_obj in json_lines:
                    if json_obj is not None and "name" in json_obj:
                        final_dict = {
                            "name": json_obj["name"],
                            "cat": json_obj.get("cat", ""),
                            "dur": json_obj.get("dur", 0),
                        }
                        processed_records.append(final_dict)
                
                return processed_records
            
            # Get reference data and verify against environment
            full_reader = dft_utils.Reader(gz_file, indexer=temp_indexer)
            reference_data = full_reader.read_line_bytes_json(0, max_bytes)
            expected_count = len([obj for obj in reference_data if obj and "name" in obj])
            
            # Double-check against environment.lines
            assert expected_count == env.lines, f"Reference data mismatch: got {expected_count} records, environment has {env.lines} lines"
            
            # Test the specific problematic 16KB boundary from the original issue
            batch_size = 16 * 1024  # 16KB - the original problematic boundary
            batches = []
            for start in range(0, max_bytes, batch_size):
                end = min(start + batch_size, max_bytes)
                batches.append((gz_file, start, end))
            
            print(f"\n=== Testing boundary edge case: 16KB chunks ===")
            print(f"Created {len(batches)} batches, expecting {expected_count} total records (env.lines: {env.lines})")
            
            # Process with Dask
            results = (
                db.from_sequence(batches, npartitions=len(batches))
                .map(process_batch)
                .flatten()
                .filter(lambda x: "name" in x)
                .compute()
            )
            
            # Verify results
            actual_count = len(results)
            
            # Check for duplicates
            unique_records = set()
            duplicates = []
            
            for record in results:
                key = (record.get('name', ''), record.get('cat', ''), record.get('dur', 0))
                if key in unique_records:
                    duplicates.append(record)
                else:
                    unique_records.add(key)
            
            print(f"Results: {actual_count} records, {len(duplicates)} duplicates")
            
            # Assertions
            assert len(duplicates) == 0, f"Found {len(duplicates)} duplicates in boundary test!"
            assert actual_count == expected_count, f"Expected {expected_count} records, got {actual_count}"
            assert actual_count == env.lines, f"Expected {env.lines} env.lines, got {actual_count}"
            
            print("Boundary edge case test passed: Complete data recovery, no duplicates")

    def test_problem(self):
        """Test JSON processing with multiple batch sizes to ensure no duplicates and complete data recovery"""
        import dask.bag as db

        import glob

        def get_size(filename):
            size = 0
            if filename.endswith(".pfw"):
                size = os.stat(filename).st_size
            elif filename.endswith(".pfw.gz"):
                index_file = f"{filename}.idx"
                indexer = dft_utils.Indexer(filename, index_file)
                size = indexer.get_max_bytes()
            return filename, int(size)
        
        def generate_batches(filename, max_bytes):
            batch_size = 1024 * 16
            for start in range(0, max_bytes, batch_size):
                # this range is intended since DFTracerJsonLinesBytesReader do
                # line boundary algorithm internally to chop incomplete line
                end = min(start + batch_size, max_bytes)
                yield filename, start, end

        # def generate_batches(filename, max_bytes):
        #     batch_size = 1024 * 16
        #     for start in range(0, max_bytes, batch_size):
        #         # this range is intended since DFTracerJsonLinesBytesReader do
        #         # line boundary algorithm internally to chop incomplete line
        #         end = min(start + batch_size, max_bytes)
        #         yield filename, start, end
    
        #     # Handle leftover data if max_bytes is not divisible by batch_size
        #     if max_bytes % batch_size != 0:
        #         start = (max_bytes // batch_size) * batch_size
        #         end = max_bytes
        #         yield filename, start, end

        def load_indexed_gzip_files(filename, start, end):
          index_file = f"{filename}.idx"
          reader = dft_utils.Reader(filename, index_file)
          json_lines = reader.read_line_bytes_json(start, end)
          return json_lines

        def load_object(data):
            dict = {}
            for key in data.keys():
                dict[key] = data[key]
            return dict

        all_files = glob.glob("/Users/rayandrew/Projects/dftracer/dftracer-utils/traces/2025-08-02-13-44-46-588838-FHKS/*.pfw.gz")

        sizes = db.from_sequence(all_files).map(get_size).compute()
        total_size = sum(size for _, size in sizes)

        total_bytes = 0
        json_line_delayed = []
        for filename, max_bytes in sizes:
            total_bytes += max_bytes
            for _, start, end in generate_batches(filename, max_bytes):
                json_line_delayed.append((filename, start, end))


        # First, get reference count by reading whole files
        print("=== Getting reference count ===")
        reference_count = 0
        for filename in all_files:
            index_file = f"{filename}.idx"
            indexer = dft_utils.Indexer(filename, index_file)
            max_bytes = indexer.get_max_bytes()
            reader = dft_utils.Reader(filename, index_file)
            json_objects = reader.read_line_bytes_json(0, max_bytes)
            reference_count += len(json_objects)
            print(f"File {os.path.basename(filename)}: {len(json_objects)} records")
        
        print(f"Total reference records: {reference_count}")
        
        # Test: Compare read_line_bytes vs read_line_bytes_json for one file
        test_filename = all_files[0]
        test_indexer = dft_utils.Indexer(test_filename, f"{test_filename}.idx")
        test_reader = dft_utils.Reader(test_filename, f"{test_filename}.idx")
        test_max_bytes = test_indexer.get_max_bytes()
        
        # Test single batch processing vs full file processing
        batch_size = 1024 * 16
        batch_records = 0
        batch_string_records = 0
        for start in range(0, test_max_bytes, batch_size):
            end = min(start + batch_size, test_max_bytes)
            # Test with read_line_bytes_json (processor version)
            batch_json = test_reader.read_line_bytes_json(start, end)
            batch_records += len(batch_json)
            
            # Test with read_line_bytes (string version) - returns list of strings
            batch_strings = test_reader.read_line_bytes(start, end)
            batch_string_records += len(batch_strings)
        
        full_json = test_reader.read_line_bytes_json(0, test_max_bytes)
        full_strings = test_reader.read_line_bytes(0, test_max_bytes)
        
        print(f"Test file {os.path.basename(test_filename)}:")
        print(f"  Full JSON: {len(full_json)}, Full String: {len(full_strings)}")
        print(f"  Batched JSON: {batch_records}, Batched String: {batch_string_records}")
        print(f"  Missing JSON: {len(full_json) - batch_records}, Missing String: {len(full_strings) - batch_string_records}")
        
        # Test read_lines_json (line-based) vs read_line_bytes_json (byte-based)
        # This will tell us if the issue is specific to byte-based chunking
        full_lines_json = test_reader.read_lines_json(1, test_indexer.get_num_lines())
        
        # Test chunked line-based reading
        total_lines = test_indexer.get_num_lines()
        lines_per_chunk = max(1, total_lines // 7)  # Create ~7 chunks like byte chunking
        chunked_lines_records = 0
        
        for start_line in range(1, total_lines + 1, lines_per_chunk):
            end_line = min(start_line + lines_per_chunk - 1, total_lines)
            chunk_lines_json = test_reader.read_lines_json(start_line, end_line)
            chunked_lines_records += len(chunk_lines_json)
        
        print(f"  Processor difference (JSON vs String): {len(full_json) - len(full_strings)}")
        print(f"  Line-based vs Byte-based JSON: {len(full_lines_json)} vs {len(full_json)}")
        print(f"  Chunked line-based: {chunked_lines_records} vs Full line-based: {len(full_lines_json)}")
        print(f"  Line chunking loss: {len(full_lines_json) - chunked_lines_records}")
        print(f"  → Does line-chunking also lose records like byte-chunking?")
        
        print(f"=== Processing {len(json_line_delayed)} batches ===")

        # Use from_delayed instead of concat to handle the delayed results properly
        json_line_bags = [dask.delayed(load_indexed_gzip_files)(filename, start, end) 
                          for filename, start, end in json_line_delayed]
        
        gz_bag = (
            db.from_delayed(json_line_bags)
            .map(load_object)
            .flatten()
            .filter(lambda x: "name" in x)
        )

        results = gz_bag.compute()
        batched_count = len(results)
        print(f"Batched processing results: {batched_count} records")
        print(f"Missing records: {reference_count - batched_count}")
        
        assert batched_count == reference_count, f"Expected {reference_count} records, got {batched_count}"

        print("\nAll batch sizes passed: No duplicates, complete data recovery")


if __name__ == "__main__":
    pytest.main([__file__])
