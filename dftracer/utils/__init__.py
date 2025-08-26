from typing import Optional, Literal, Union

from .dftracer_utils_ext import (
    # Reader classes and iterators
    DFTracerReader,  # noqa: F401 (alias to DFTracerLineBytesReader)
    DFTracerBytesReader,  # noqa: F401
    DFTracerLineBytesReader,  # noqa: F401
    DFTracerLinesReader,  # noqa: F401
    DFTracerJsonLinesReader,  # noqa: F401
    DFTracerJsonLinesBytesReader,  # noqa: F401
    DFTracerBytesIterator,  # noqa: F401
    DFTracerLineBytesIterator,  # noqa: F401
    DFTracerLinesIterator,  # noqa: F401
    DFTracerJsonLinesIterator,  # noqa: F401
    DFTracerJsonLinesBytesIterator,  # noqa: F401
    DFTracerBytesRangeIterator,  # noqa: F401
    DFTracerLineBytesRangeIterator,  # noqa: F401
    DFTracerLinesRangeIterator,  # noqa: F401
    DFTracerJsonLinesRangeIterator,  # noqa: F401
    DFTracerJsonLinesBytesRangeIterator,  # noqa: F401
    dft_reader_range, # noqa: F401
    # Indexer classes
    DFTracerIndexer,  # noqa: F401
    IndexCheckpoint,  # noqa: F401
    # Utility functions
    _register_logging,  # noqa: F401
)

_register_logging()

def dft_reader(
    gzip_path_or_indexer: Union[str, DFTracerIndexer], 
    index_path: Optional[str] = None, 
    mode: Literal["line_bytes", "bytes", "lines", "json_lines", "json_lines_bytes"] = "line_bytes"
):
    """Create a DFTracer reader with the specified mode.
    
    Args:
        gzip_path_or_indexer: Either a path to gzip file or a DFTracerIndexer instance
        index_path: Path to index file (ignored if indexer is provided)
        mode: Reader mode - "line_bytes", "bytes", "lines", "json_lines", or "json_lines_bytes"
        
    Returns:
        Appropriate DFTracer reader instance
    """
    if isinstance(gzip_path_or_indexer, DFTracerIndexer):
        # Create reader from indexer
        indexer = gzip_path_or_indexer
        if mode == "line_bytes":
            return DFTracerLineBytesReader(indexer)
        elif mode == "bytes":
            return DFTracerBytesReader(indexer)
        elif mode == "lines":
            return DFTracerLinesReader(indexer)
        elif mode == "json_lines":
            return DFTracerJsonLinesReader(indexer)
        elif mode == "json_lines_bytes":
            return DFTracerJsonLinesBytesReader(indexer)
        else:
            raise ValueError(f"Unknown mode: {mode}")
    else:
        # Create reader from paths
        gzip_path = gzip_path_or_indexer
        if mode == "line_bytes":
            return DFTracerLineBytesReader(gzip_path, index_path)
        elif mode == "bytes":
            return DFTracerBytesReader(gzip_path, index_path)
        elif mode == "lines":
            return DFTracerLinesReader(gzip_path, index_path)
        elif mode == "json_lines":
            return DFTracerJsonLinesReader(gzip_path, index_path)
        elif mode == "json_lines_bytes":
            return DFTracerJsonLinesBytesReader(gzip_path, index_path)
        else:
            raise ValueError(f"Unknown mode: {mode}")

__version__ = "1.0.0"
__all__ = [
    "DFTracerReader",
    "DFTracerBytesReader",
    "DFTracerLineBytesReader", 
    "DFTracerLinesReader",
    "DFTracerJsonLinesReader",
    "DFTracerJsonLinesBytesReader",
    "DFTracerBytesIterator",
    "DFTracerLineBytesIterator",
    "DFTracerLinesIterator",
    "DFTracerJsonLinesIterator",
    "DFTracerJsonLinesBytesIterator",
    "DFTracerBytesRangeIterator",
    "DFTracerLineBytesRangeIterator",
    "DFTracerLinesRangeIterator",
    "DFTracerJsonLinesRangeIterator",
    "DFTracerJsonLinesBytesRangeIterator",
    "DFTracerIndexer",
    "IndexCheckpoint",
    "dft_reader",
    "dft_reader_range",
]
