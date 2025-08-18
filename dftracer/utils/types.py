"""Type definitions for dftracer.utils package."""

from typing import Union, List

try:
    from typing import TypeGuard
except ImportError:
    try:
        from typing_extensions import TypeGuard
    except ImportError:
        def TypeGuard(x):
            return bool

from .dftracer_utils_ext import JsonDocument, JsonArray

# Type aliases for better typing support
JsonValue = Union['JsonDocument', 'JsonArray', str, int, float, bool, None]
"""Type representing any JSON value - can be JsonDocument, JsonArray, or primitive types."""

JsonArrayLike = Union['JsonArray', List['JsonValue']]
"""Type representing array-like JSON structures - either JsonArray or Python list."""

# Type guards for runtime type checking
def is_json_document(obj) -> TypeGuard[JsonDocument]:
    """Check if object is a JsonDocument."""
    try:
        from .dftracer_utils_ext import JsonDocument
        return isinstance(obj, JsonDocument)
    except ImportError:
        return False

def is_json_array(obj) -> TypeGuard[JsonArray]:
    """Check if object is a JsonArray."""
    try:
        from .dftracer_utils_ext import JsonArray
        return isinstance(obj, JsonArray)
    except ImportError:
        return False

def is_json_value(obj) -> TypeGuard[JsonValue]:
    """Check if object is a valid JSON value type."""
    return (
        is_json_document(obj) or 
        is_json_array(obj) or 
        isinstance(obj, (str, int, float, bool, type(None)))
    )
