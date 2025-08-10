"""
PyReader - Python bindings for DFTracer gzip reader

This package provides efficient random access to gzipped files using 
SQLite indexes created by the DFTracer indexer.

Main classes:
    GzipReader: For reading ranges from indexed gzip files
    
Main functions:
    create_index: Create SQLite index for a gzip file
    read_gzip_range: Convenience function for one-shot reading
    read_gzip_range_mb: Convenience function for MB-range reading
"""

__version__ = "1.0.0"
__author__ = "DFTracer Team"

# Import main functionality
try:
    from .reader import (
        GzipReader,
        create_index,
        read_gzip_range,
        read_gzip_range_mb,
        ReaderError,
        IndexerError
    )
    
    __all__ = [
        'GzipReader',
        'create_index', 
        'read_gzip_range',
        'read_gzip_range_mb',
        'ReaderError',
        'IndexerError'
    ]
    
except ImportError as e:
    # This can happen during build or if Cython extension isn't built yet
    import warnings
    warnings.warn(f"Could not import reader module: {e}")
    __all__ = []
