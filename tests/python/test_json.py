#!/usr/bin/env python3
"""
Test cases for JsonDocument Python bindings
"""

import pytest
import dftracer.utils as dft_utils


class TestJsonDocument:
    """Test cases for JsonDocument class"""
    
    def test_import(self):
        """Test that we can import JsonDocument"""
        assert hasattr(dft_utils, 'JsonDocument')
    
    def test_create_from_simple_json(self):
        """Test creating JsonDocument from simple JSON string"""
        json_str = '{"name": "test", "value": 42, "active": true}'
        doc = dft_utils.JsonDocument(json_str)
        
        # Test basic access
        assert doc["name"] == "test"
        assert doc["value"] == 42
        assert doc["active"] == True
    
    def test_create_from_nested_json(self):
        """Test creating JsonDocument from nested JSON"""
        json_str = '{"user": {"name": "Alice", "age": 30}, "items": [1, 2, 3]}'
        doc = dft_utils.JsonDocument(json_str)
        
        # Test nested object access
        user = doc["user"]
        assert isinstance(user, dft_utils.JsonDocument)
        assert user["name"] == "Alice"
        assert user["age"] == 30
        
        # Test array access
        items = doc["items"]
        assert isinstance(items, dft_utils.JsonArray)
        assert len(items) == 3
        assert items[0] == 1
        assert items[1] == 2
        assert items[2] == 3
    
    def test_dict_like_operations(self):
        """Test dict-like operations"""
        json_str = '{"a": 1, "b": "hello", "c": null, "d": false}'
        doc = dft_utils.JsonDocument(json_str)
        
        # Test __contains__
        assert "a" in doc
        assert "b" in doc
        assert "c" in doc
        assert "d" in doc
        assert "nonexistent" not in doc
        
        # Test __len__
        assert len(doc) == 4
        
        # Test keys() - should return iterator
        keys = doc.keys()
        keys_list = list(keys)
        assert len(keys_list) == 4
        assert "a" in keys_list
        assert "b" in keys_list
        assert "c" in keys_list
        assert "d" in keys_list
        
        # Test values() - should return iterator
        values = doc.values()
        values_list = list(values)
        assert len(values_list) == 4
        assert 1 in values_list
        assert "hello" in values_list
        assert None in values_list
        assert False in values_list
    
    def test_get_method(self):
        """Test get method with defaults"""
        json_str = '{"existing": "value"}'
        doc = dft_utils.JsonDocument(json_str)
        
        # Test existing key
        assert doc.get("existing") == "value"
        
        # Test non-existing key with default None
        assert doc.get("nonexistent") is None
        
        # Test non-existing key with custom default
        assert doc.get("nonexistent", "default") == "default"
        assert doc.get("nonexistent", 42) == 42
    
    def test_key_error_handling(self):
        """Test KeyError handling"""
        json_str = '{"existing": "value"}'
        doc = dft_utils.JsonDocument(json_str)
        
        # Test accessing non-existent key raises KeyError
        with pytest.raises(KeyError):
            doc["nonexistent"]
    
    def test_iteration(self):
        """Test iteration over keys"""
        json_str = '{"a": 1, "b": 2, "c": 3}'
        doc = dft_utils.JsonDocument(json_str)
        
        # Test __iter__ - should return iterator over keys
        keys_from_iter = list(doc)  # Convert iterator to list
        
        assert len(keys_from_iter) == 3
        assert "a" in keys_from_iter
        assert "b" in keys_from_iter
        assert "c" in keys_from_iter
        
        # Should be same as keys()
        keys_from_method = list(doc.keys())
        assert set(keys_from_iter) == set(keys_from_method)
        
        # Test for loop iteration
        keys_from_for_loop = []
        for key in doc:
            keys_from_for_loop.append(key)
        
        assert set(keys_from_for_loop) == set(keys_from_iter)
    
    def test_values_method(self):
        """Test values() method"""
        json_str = '{"name": "Alice", "age": 30, "active": true, "balance": null}'
        doc = dft_utils.JsonDocument(json_str)
        
        # Test values() method
        values = doc.values()
        values_list = list(values)
        
        assert len(values_list) == 4
        assert "Alice" in values_list
        assert 30 in values_list
        assert True in values_list
        assert None in values_list
        
        # Test that values correspond to keys
        keys_list = list(doc.keys())
        for key in keys_list:
            assert doc[key] in values_list
    
    def test_items_method(self):
        """Test items() method"""
        json_str = '{"name": "Bob", "score": 95, "active": false}'
        doc = dft_utils.JsonDocument(json_str)
        
        # Test items() method
        items = doc.items()
        items_list = list(items)
        
        assert len(items_list) == 3
        
        # Check that each item is a tuple
        for item in items_list:
            assert isinstance(item, tuple)
            assert len(item) == 2
            key, value = item
            assert isinstance(key, str)
            # Check that the value matches what we get from direct access
            assert doc[key] == value
        
        # Check specific items
        items_dict = dict(items_list)
        assert items_dict["name"] == "Bob"
        assert items_dict["score"] == 95
        assert items_dict["active"] == False
        
        # Test that items() can be used to reconstruct the dictionary
        reconstructed = dict(doc.items())
        assert reconstructed["name"] == doc["name"]
        assert reconstructed["score"] == doc["score"]
        assert reconstructed["active"] == doc["active"]
    
    def test_string_representations(self):
        """Test __str__ and __repr__"""
        json_str = '{"name": "test", "value": 42}'
        doc = dft_utils.JsonDocument(json_str)
        
        # Test __str__ - should return minified JSON
        str_repr = str(doc)
        assert isinstance(str_repr, str)
        assert "name" in str_repr
        assert "test" in str_repr
        assert "value" in str_repr
        assert "42" in str_repr
        
        # Test __repr__ - should include JsonDocument wrapper
        repr_str = repr(doc)
        assert isinstance(repr_str, str)
        assert repr_str.startswith("JsonDocument(")
        assert repr_str.endswith(")")
        assert "object" in repr_str
    
    def test_data_types(self):
        """Test handling of different JSON data types"""
        json_str = '''
        {
            "string": "hello",
            "integer": 123,
            "float": 45.67,
            "boolean_true": true,
            "boolean_false": false,
            "null_value": null,
            "array": [1, 2, 3],
            "object": {"nested": "value"}
        }
        '''
        doc = dft_utils.JsonDocument(json_str)
        
        # Test string
        assert doc["string"] == "hello"
        assert isinstance(doc["string"], str)
        
        # Test integer
        assert doc["integer"] == 123
        assert isinstance(doc["integer"], int)
        
        # Test float
        assert doc["float"] == 45.67
        assert isinstance(doc["float"], float)
        
        # Test booleans
        assert doc["boolean_true"] is True
        assert doc["boolean_false"] is False
        assert isinstance(doc["boolean_true"], bool)
        assert isinstance(doc["boolean_false"], bool)
        
        # Test null
        assert doc["null_value"] is None
        
        # Test array
        array = doc["array"]
        assert isinstance(array, dft_utils.JsonArray)
        assert len(array) == 3
        assert array[0] == 1
        assert array[1] == 2
        assert array[2] == 3

        # Test nested object
        nested = doc["object"]
        assert isinstance(nested, dft_utils.JsonDocument)
        assert nested["nested"] == "value"
    
    def test_empty_and_invalid_json(self):
        """Test handling of empty and invalid JSON"""
        # Test empty object
        empty_doc = dft_utils.JsonDocument('{}')
        assert len(empty_doc) == 0
        assert list(empty_doc.keys()) == []
        
        # Test invalid JSON should raise an exception
        with pytest.raises((RuntimeError, Exception)):
            dft_utils.JsonDocument('invalid json')
        
        # Test incomplete JSON should raise an exception
        with pytest.raises((RuntimeError, Exception)):
            dft_utils.JsonDocument('{"incomplete":')
    
    def test_non_object_json(self):
        """Test behavior with non-object JSON (arrays, primitives)"""
        # Test with array at root - should not support dict operations
        array_doc = dft_utils.JsonDocument('[1, 2, 3]')
        
        # Should not be able to access as dict
        assert len(array_doc) == 0  # Non-object should return 0 length
        assert "0" not in array_doc  # Array indices don't work as object keys
        
        # Test with primitive at root
        string_doc = dft_utils.JsonDocument('"hello"')
        assert len(string_doc) == 0  # Non-object should return 0 length
        
        number_doc = dft_utils.JsonDocument('42')
        assert len(number_doc) == 0  # Non-object should return 0 length
    
    def test_lazy_conversion(self):
        """Test that nested objects are converted lazily"""
        json_str = '''
        {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deep"
                    }
                }
            },
            "array": [
                {"item": 1},
                {"item": 2}
            ]
        }
        '''
        doc = dft_utils.JsonDocument(json_str)
        
        # Each level should return a JsonDocument instance
        level1 = doc["level1"]
        assert isinstance(level1, dft_utils.JsonDocument)
        
        level2 = level1["level2"]
        assert isinstance(level2, dft_utils.JsonDocument)
        
        level3 = level2["level3"]
        assert isinstance(level3, dft_utils.JsonDocument)
        
        assert level3["value"] == "deep"
        
        # Array items should also be converted properly
        array = doc["array"]
        assert isinstance(array, dft_utils.JsonArray)
        assert len(array) == 2
        
        item1 = array[0]
        assert isinstance(item1, dft_utils.JsonDocument)
        assert item1["item"] == 1
        
        item2 = array[1]
        assert isinstance(item2, dft_utils.JsonDocument)
        assert item2["item"] == 2
    
    def test_lazy_arrays(self):
        """Test that arrays are handled lazily with JsonArray wrapper"""
        json_str = '''
        {
            "simple_array": [1, 2, 3, "hello", true, null],
            "mixed_array": [
                {"name": "object1"},
                [1, 2, 3],
                "string",
                42
            ],
            "nested_arrays": [
                [1, 2],
                [3, 4],
                [5, 6]
            ]
        }
        '''
        doc = dft_utils.JsonDocument(json_str)
        
        # Test simple array access
        simple_array = doc["simple_array"]
        assert hasattr(simple_array, '__len__')  # Should be JsonArray, not list
        assert hasattr(simple_array, '__getitem__')
        assert hasattr(simple_array, '__iter__')
        
        # Test JsonArray length
        assert len(simple_array) == 6
        
        # Test JsonArray indexing
        assert simple_array[0] == 1
        assert simple_array[1] == 2
        assert simple_array[2] == 3
        assert simple_array[3] == "hello"
        assert simple_array[4] == True
        assert simple_array[5] is None
        
        # Test JsonArray iteration (should be lazy)
        items = list(simple_array)
        assert len(items) == 6
        assert items[0] == 1
        assert items[3] == "hello"
        
        # Test mixed array with nested objects and arrays
        mixed_array = doc["mixed_array"]
        assert len(mixed_array) == 4
        
        # First element should be JsonDocument
        obj = mixed_array[0]
        assert hasattr(obj, 'keys')  # Should be JsonDocument
        assert obj["name"] == "object1"
        
        # Second element should be JsonArray
        nested_arr = mixed_array[1]
        assert hasattr(nested_arr, '__len__')  # Should be JsonArray
        assert len(nested_arr) == 3
        assert nested_arr[0] == 1
        
        # Test deeply nested arrays
        nested_arrays = doc["nested_arrays"]
        assert len(nested_arrays) == 3
        
        first_sub_array = nested_arrays[0]
        assert hasattr(first_sub_array, '__len__')  # Should be JsonArray
        assert len(first_sub_array) == 2
        assert first_sub_array[0] == 1
        assert first_sub_array[1] == 2
    
    def test_array_error_handling(self):
        """Test JsonArray error handling"""
        json_str = '{"array": [1, 2, 3]}'
        doc = dft_utils.JsonDocument(json_str)
        array = doc["array"]
        
        # Test index out of range
        with pytest.raises(IndexError):
            array[10]
        
        # Test negative indexing (not supported)
        with pytest.raises((IndexError, RuntimeError)):
            array[-1]
    
    def test_array_string_representations(self):
        """Test JsonArray string representations"""
        json_str = '{"array": [1, "hello", true]}'
        doc = dft_utils.JsonDocument(json_str)
        array = doc["array"]
        
        # Test __str__ and __repr__
        str_repr = str(array)
        assert isinstance(str_repr, str)
        assert "1" in str_repr
        assert "hello" in str_repr
        
        repr_str = repr(array)
        assert isinstance(repr_str, str)
        assert repr_str.startswith("JsonArray(")
        assert repr_str.endswith(")")


if __name__ == "__main__":
    pytest.main([__file__])
