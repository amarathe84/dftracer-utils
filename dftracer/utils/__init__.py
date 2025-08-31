from typing import Optional, Literal, Union

from .dftracer_utils_ext import (
    # Core classes
    DFTracerReader,  # noqa: F401
    DFTracerIndexer,  # noqa: F401
    IndexCheckpoint,  # noqa: F401
)

def dft_reader(
    gzip_path_or_indexer: Union[str, DFTracerIndexer], 
    index_path: Optional[str] = None
):
    """Create a DFTracer reader.
    
    Args:
        gzip_path_or_indexer: Either a path to gzip file or a DFTracerIndexer instance
        index_path: Path to index file (ignored if indexer is provided)
        
    Returns:
        DFTracerReader instance
    """
    if isinstance(gzip_path_or_indexer, DFTracerIndexer):
        # Create reader from indexer
        return DFTracerReader(gzip_path_or_indexer.gz_path, indexer=gzip_path_or_indexer)
    else:
        # Create reader from paths
        return DFTracerReader(gzip_path_or_indexer, index_path)

__version__ = "1.0.0"
__all__ = [
    "DFTracerReader",
    "DFTracerIndexer",
    "IndexCheckpoint",
    "dft_reader",
]
