# Import all available components from the extension
try:
    # Try the new naming convention first
    from .dft_utils_reader_ext import (
        DFTracerReader,  # noqa: F401
        DFTracerLineRangeIterator,  # noqa: F401  
        DFTracerRawRangeIterator,  # noqa: F401
        LineIterator,  # noqa: F401
        RawIterator,  # noqa: F401
        dft_reader_range,  # noqa: F401
        dft_reader_raw_range,  # noqa: F401
        set_log_level,  # noqa: F401
        set_log_level_int,  # noqa: F401
        get_log_level_string,  # noqa: F401
        get_log_level_int,  # noqa: F401
    )
    # Create alias for backward compatibility
    DFTracerRangeIterator = DFTracerLineRangeIterator  # noqa: F401
    
except ImportError:
    # Fall back to old naming convention
    from .dft_utils_reader_ext import (
        DFTracerReader,  # noqa: F401
        DFTracerRangeIterator,  # noqa: F401
        LineIterator,  # noqa: F401
        RawIterator,  # noqa: F401
        dft_reader_range,  # noqa: F401
        set_log_level,  # noqa: F401
        set_log_level_int,  # noqa: F401
        get_log_level_string,  # noqa: F401
        get_log_level_int,  # noqa: F401
    )
    
    # Create aliases for backward compatibility
    DFTracerLineRangeIterator = DFTracerRangeIterator  # noqa: F401
    DFTracerRawRangeIterator = DFTracerRangeIterator  # noqa: F401

    # Create a wrapper function for raw range since it's not available in the extension
    def dft_reader_raw_range(reader, start, end, step=4*1024*1024):  # noqa: F401
        """Create a raw range iterator - currently returns same as dft_reader_range"""
        return dft_reader_range(reader, start, end, step)

__version__ = "1.0.0"
__all__ = [
    "DFTracerReader",
    "DFTracerRangeIterator",
    "DFTracerLineRangeIterator",
    "DFTracerRawRangeIterator", 
    "LineIterator",
    "RawIterator",
    "dft_reader_range",
    "dft_reader_raw_range",
    "set_log_level",
    "set_log_level_int", 
    "get_log_level_string",
    "get_log_level_int"
]
