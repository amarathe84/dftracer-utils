#!/usr/bin/env python3
"""
Test script for DFTracerReader Python binding
"""

import sys
import os

try:
    from dft_reader import DFTracerReader, dft_reader_range, set_log_level
    print("✓ Successfully imported DFTracerReader")
except ImportError as e:
    print(f"✗ Failed to import DFTracerReader: {e}")
    sys.exit(1)

def test_with_real_file():
    """Test with real file if available"""
    
    # Check if we have a real trace file in the parent directory
    test_files = [
        "../../trace.pfw.gz",
        "../trace.pfw.gz", 
        "trace.pfw.gz"
    ]
    
    for test_file in test_files:
        if os.path.exists(test_file):
            print(f"Found test file: {test_file}")
            
            # Check if index exists
            index_file = test_file + ".idx"
            if not os.path.exists(index_file):
                print(f"✗ Index file not found: {index_file}")
                print("  You may need to create the index first using the reader tool")
                continue
                
            try:
                with DFTracerReader(test_file) as reader:
                    print(f"✓ Successfully opened real file: {test_file}")
                    print(f"✓ Database is open: {reader.is_open}")
                    
                    # Try to read a small range
                    try:
                        # Test get_max_bytes
                        max_bytes = reader.get_max_bytes()
                        print(f"✓ Maximum bytes available: {max_bytes:,} ({max_bytes / (1024 * 1024):.2f} MB)")
                        
                        # Test iterator functionality
                        print("Testing iterator functionality...")
                        
                        # Test direct iteration (for chunk in reader)
                        chunk_count = 0
                        total_bytes = 0
                        for chunk in reader:
                            chunk_count += 1
                            total_bytes += len(chunk)
                            if chunk_count >= 2:  # Just test first 2 chunks
                                print(f"✓ Direct iteration: processed {chunk_count} chunks, {total_bytes} bytes")
                                break
                        
                        # Test default iterator (1MB chunks)
                        chunk_count = 0
                        total_bytes = 0
                        for chunk in reader.iterator():
                            chunk_count += 1
                            total_bytes += len(chunk)
                            if chunk_count >= 3:  # Just test first 3 chunks
                                print(f"✓ Default iterator: processed {chunk_count} chunks, {total_bytes} bytes")
                                break
                        
                        # Test custom iterator (100KB chunks)
                        chunk_count = 0
                        total_bytes = 0
                        custom_stride = 100 * 1024  # 100KB
                        for chunk in reader.iter(custom_stride):
                            chunk_count += 1
                            total_bytes += len(chunk)
                            if chunk_count >= 5:  # Just test first 5 chunks
                                print(f"✓ Custom iterator ({custom_stride} bytes): processed {chunk_count} chunks, {total_bytes} bytes")
                                break
                        
                        # Test range iterator
                        chunk_count = 0
                        total_bytes = 0
                        for chunk in dft_reader_range(reader, 5 * 1024 * 1024, 7 * 1024 * 1024, 256 * 1024):
                            chunk_count += 1
                            total_bytes += len(chunk)
                            if chunk_count >= 3:  # Just test first 3 chunks
                                print(f"✓ Range iterator (5MB-7MB, 256KB steps): processed {chunk_count} chunks, {total_bytes} bytes")
                                break
                        
                        data = reader.read_mb(3400, 3500)
                        print(f"✓ Successfully read {len(data) / (1024 * 1024)} MB")
                        # print("Last 10 lines:")
                        # print("\n".join(data.splitlines()[-10:]))
                        return True
                    except Exception as e:
                        print(f"✗ Failed to read data: {e}")
                        return False
                        
            except Exception as e:
                print(f"✗ Failed to open real file: {e}")
                return False
    
    print("! No real test files found, skipping real file test")
    return True

if __name__ == "__main__":
    print("Testing DFTracerReader Python binding...")
    print("=" * 50)
    
    success = True
    
    success &= test_with_real_file()
    print()
    
    if success:
        print("✓ All tests passed!")
        sys.exit(0)
    else:
        print("✗ Some tests failed!")
        sys.exit(1)
