#!/usr/bin/env python3
"""
Test cases for DFTracer utils Python bindings
"""

import pytest
import dftracer.utils as dft_utils


class TestDFTracerUtils:
    """Test cases for DFTracer utils extension"""
    
    def test_import_utils_functions(self):
        """Test that we can import the utils functions"""
        assert hasattr(dft_utils, 'set_log_level')
        assert hasattr(dft_utils, 'set_log_level_int')
        assert hasattr(dft_utils, 'get_log_level_string')
        assert hasattr(dft_utils, 'get_log_level_int')
    
    def test_log_level_string_operations(self):
        """Test string-based log level operations"""
        # Test valid log levels
        valid_levels = ["trace", "debug", "info", "warn", "error", "critical", "off"]
        
        for level in valid_levels:
            # Set log level using string
            dft_utils.set_log_level(level)
            
            # Get log level as string and verify it matches
            current_level = dft_utils.get_log_level_string()
            assert current_level == level
            
            # Get log level as int and verify it's valid
            current_level_int = dft_utils.get_log_level_int()
            assert isinstance(current_level_int, int)
            assert 0 <= current_level_int <= 6
    
    def test_log_level_int_operations(self):
        """Test integer-based log level operations"""
        # Test valid log level integers
        valid_int_levels = [0, 1, 2, 3, 4, 5, 6]  # trace through off
        expected_strings = ["trace", "debug", "info", "warn", "error", "critical", "off"]
        
        for i, level_int in enumerate(valid_int_levels):
            # Set log level using integer
            result = dft_utils.set_log_level_int(level_int)
            assert result == 0  # Should return 0 on success
            
            # Get log level as int and verify it matches
            current_level_int = dft_utils.get_log_level_int()
            assert current_level_int == level_int
            
            # Get log level as string and verify it matches expected
            current_level_str = dft_utils.get_log_level_string()
            assert current_level_str == expected_strings[i]
    
    def test_log_level_consistency(self):
        """Test consistency between string and int log level operations"""
        test_cases = [
            ("trace", 0),
            ("debug", 1),
            ("info", 2),
            ("warn", 3),
            ("error", 4),
            ("critical", 5),
            ("off", 6)
        ]
        
        for level_str, level_int in test_cases:
            # Set using string, check int
            dft_utils.set_log_level(level_str)
            assert dft_utils.get_log_level_int() == level_int
            assert dft_utils.get_log_level_string() == level_str
            
            # Set using int, check string
            dft_utils.set_log_level_int(level_int)
            assert dft_utils.get_log_level_string() == level_str
            assert dft_utils.get_log_level_int() == level_int
    
    def test_log_level_invalid_string(self):
        """Test behavior with invalid string log levels"""
        # Test invalid log level strings
        invalid_levels = ["invalid", "TRACE", "Debug", "INFO", "unknown", ""]
        
        for invalid_level in invalid_levels:
            # should be fine if the level is not recognized
            dft_utils.set_log_level(invalid_level)
    
    def test_log_level_invalid_int(self):
        """Test behavior with invalid integer log levels"""
        # Test invalid log level integers
        invalid_int_levels = [-1, 7, 10, 100, -100]
        
        for invalid_level in invalid_int_levels:
            # should be fine if the level is not recognized
            dft_utils.set_log_level_int(invalid_level)
    
    def test_log_level_return_types(self):
        """Test return types of log level functions"""
        # Set a known log level
        dft_utils.set_log_level("info")
        
        # Test return types
        level_str = dft_utils.get_log_level_string()
        level_int = dft_utils.get_log_level_int()
        set_result = dft_utils.set_log_level_int(2)  # info level
        
        assert isinstance(level_str, str)
        assert isinstance(level_int, int)
        assert isinstance(set_result, int)
        
        # Test specific values
        assert level_str == "info"
        assert level_int == 2
        assert set_result == 0  # Success return code
    
    def test_log_level_state_persistence(self):
        """Test that log level state persists across multiple calls"""
        # Set initial level
        dft_utils.set_log_level("debug")
        initial_str = dft_utils.get_log_level_string()
        initial_int = dft_utils.get_log_level_int()
        
        # Call getters multiple times
        for _ in range(5):
            assert dft_utils.get_log_level_string() == initial_str
            assert dft_utils.get_log_level_int() == initial_int
        
        # Change level and test persistence
        dft_utils.set_log_level_int(4)  # error level
        new_str = dft_utils.get_log_level_string()
        new_int = dft_utils.get_log_level_int()
        
        for _ in range(5):
            assert dft_utils.get_log_level_string() == new_str
            assert dft_utils.get_log_level_int() == new_int
        
        assert new_str == "error"
        assert new_int == 4
    
    def test_log_level_case_sensitivity(self):
        """Test that log level strings are case sensitive"""
        # These should work (lowercase)
        valid_levels = ["trace", "debug", "info", "warn", "error", "critical", "off"]
        for level in valid_levels:
            dft_utils.set_log_level(level)
            assert dft_utils.get_log_level_string() == level
        
        # These should fail (different cases)
        invalid_cases = ["TRACE", "Debug", "INFO", "Warn", "Error", "CRITICAL", "OFF"]
        for level in invalid_cases:
            # should not throw error
            dft_utils.set_log_level(level)
    
    def test_log_level_boundary_values(self):
        """Test log level boundary values"""
        # Test minimum valid value
        dft_utils.set_log_level_int(0)  # trace
        assert dft_utils.get_log_level_int() == 0
        assert dft_utils.get_log_level_string() == "trace"
        
        # Test maximum valid value
        dft_utils.set_log_level_int(6)  # off
        assert dft_utils.get_log_level_int() == 6
        assert dft_utils.get_log_level_string() == "off"
        
        # Test just outside boundaries, should not throw error
        dft_utils.set_log_level_int(-1)
        dft_utils.set_log_level_int(7)
    
    def test_log_level_thread_safety_simulation(self):
        """Test log level operations in rapid succession (simulating thread safety)"""
        # Rapidly set and get log levels
        levels = ["trace", "debug", "info", "warn", "error", "critical", "off"]
        
        for _ in range(10):  # Multiple iterations
            for i, level in enumerate(levels):
                dft_utils.set_log_level(level)
                assert dft_utils.get_log_level_string() == level
                assert dft_utils.get_log_level_int() == i
                
                # Also test int-based setting
                dft_utils.set_log_level_int(i)
                assert dft_utils.get_log_level_string() == level
                assert dft_utils.get_log_level_int() == i
    
    def test_log_level_documentation_examples(self):
        """Test examples that might be used in documentation"""
        # Example 1: Basic usage
        dft_utils.set_log_level("info")
        assert dft_utils.get_log_level_string() == "info"
        
        # Example 2: Using integer levels
        dft_utils.set_log_level_int(1)  # debug
        assert dft_utils.get_log_level_string() == "debug"
        
        # Example 3: Checking current level
        current_level = dft_utils.get_log_level_string()
        assert current_level in ["trace", "debug", "info", "warn", "error", "critical", "off"]
        
        # Example 4: Invalid level handling, should not throw error
        dft_utils.set_log_level("invalid_level")


if __name__ == "__main__":
    pytest.main([__file__])
