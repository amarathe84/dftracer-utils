from typing import Optional, Literal, Union

from .dftracer_utils_ext import (
    # Core classes
    Reader,  # noqa: F401
    Indexer,  # noqa: F401
    IndexerCheckpoint,  # noqa: F401
)

def dft_reader(
    gzip_path_or_indexer: Union[str, Indexer], 
    index_path: Optional[str] = None
):
    """Create a  reader.
    
    Args:
        gzip_path_or_indexer: Either a path to gzip file or a Indexer instance
        index_path: Path to index file (ignored if indexer is provided)
        
    Returns:
        Reader instance
    """
    if isinstance(gzip_path_or_indexer, Indexer):
        # Create reader from indexer
        return Reader(gzip_path_or_indexer.gz_path, indexer=gzip_path_or_indexer)
    else:
        # Create reader from paths
        return Reader(gzip_path_or_indexer, index_path)

__version__ = "1.0.0"
__all__ = [
    "Reader",
    "Indexer",
    "IndexerCheckpoint",
    "dft_reader",
]
