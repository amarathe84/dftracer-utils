from typing import Optional, Literal

from .reader_ext import (
    DFTracerReader,  # noqa: F401 (alias to DFTracerLineBytesReader)
    DFTracerBytesReader,  # noqa: F401
    DFTracerLineBytesReader,  # noqa: F401
    DFTracerLinesReader,  # noqa: F401
    DFTracerBytesIterator,  # noqa: F401
    DFTracerLineBytesIterator,  # noqa: F401
    DFTracerLinesIterator,  # noqa: F401
    DFTracerBytesRangeIterator,  # noqa: F401
    DFTracerLineBytesRangeIterator,  # noqa: F401
    DFTracerLinesRangeIterator,  # noqa: F401
    dft_reader_range, # noqa: F401
)

from .utils_ext import (
    set_log_level,  # noqa: F401
    set_log_level_int,  # noqa: F401
    get_log_level_string,  # noqa: F401
    get_log_level_int,  # noqa: F401
)

def dft_reader(gzip_path: str, index_path: Optional[str] = None, mode: Literal["line_bytes", "bytes", "lines"] = "line_bytes"):
    if mode == "line_bytes":
        return DFTracerLineBytesReader(gzip_path, index_path)
    elif mode == "bytes":
        return DFTracerBytesReader(gzip_path, index_path)
    elif mode == "lines":
        return DFTracerLinesReader(gzip_path, index_path)
    else:
        raise ValueError(f"Unknown mode: {mode}")

__version__ = "1.0.0"
__all__ = [
    "DFTracerReader",
    "DFTracerBytesReader",
    "DFTracerLineBytesReader", 
    "DFTracerLinesReader",
    "DFTracerBytesIterator",
    "DFTracerLineBytesIterator",
    "DFTracerLinesIterator",
    "DFTracerBytesRangeIterator",
    "DFTracerLineBytesRangeIterator",
    "DFTracerLinesRangeIterator",
    "dft_reader",
    "dft_reader_range",
    "set_log_level",
    "set_log_level_int", 
    "get_log_level_string",
    "get_log_level_int"
]
