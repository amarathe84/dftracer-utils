"""Type stubs for dftracer_utils_ext module."""

from typing import Optional, List, Dict, Any, Iterator

# ========== INDEXER ==========

class IndexCheckpoint:
    """Information about a checkpoint in the index."""
    checkpoint_idx: int
    uc_offset: int
    uc_size: int
    c_offset: int
    c_size: int
    bits: int
    num_lines: int

class DFTracerIndexer:
    """DFTracer indexer for creating and managing gzip file indices."""
    
    def __init__(
        self, 
        gz_path: str, 
        idx_path: Optional[str] = None,
        checkpoint_size: int = 1048576,
        force_rebuild: bool = False
    ) -> None:
        """Create a DFTracer indexer for a gzip file."""
        ...
    
    def build(self) -> None:
        """Build the index."""
        ...
    
    def need_rebuild(self) -> bool:
        """Check if index needs rebuilding."""
        ...
    
    def exists(self) -> bool:
        """Check if the index file exists."""
        ...
    
    def get_max_bytes(self) -> int:
        """Get maximum byte position."""
        ...
    
    def get_num_lines(self) -> int:
        """Get number of lines."""
        ...

    def get_checkpoints(self) -> List[IndexCheckpoint]:
        """Get all checkpoints."""
        ...

    def find_checkpoint(self, target_offset: int) -> Optional[IndexCheckpoint]:
        """Find checkpoint for target offset."""
        ...
    
    @property
    def gz_path(self) -> str:
        """Get gzip path."""
        ...
    
    @property
    def idx_path(self) -> str:
        """Get index path."""
        ...
    
    @property
    def checkpoint_size(self) -> int:
        """Get checkpoint size."""
        ...
    
    def __enter__(self) -> 'DFTracerIndexer':
        """Enter the runtime context for the with statement."""
        ...
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the runtime context for the with statement."""
        ... 

# ========== READER ==========

class DFTracerReader:
    """DFTracer reader for reading from gzip files with zero-copy operations."""
    
    def __init__(
        self, 
        gz_path: str,
        idx_path: Optional[str] = None,
        checkpoint_size: int = 1048576,
        indexer: Optional[DFTracerIndexer] = None
    ) -> None:
        """Create a DFTracer reader."""
        ...
    
    def get_max_bytes(self) -> int:
        """Get the maximum byte position available in the file."""
        ...
    
    def get_num_lines(self) -> int:
        """Get the number of lines in the file."""
        ...
    
    def reset(self) -> None:
        """Reset the reader to initial state."""
        ...
    
    def read(self, start_bytes: int, end_bytes: int) -> bytes:
        """Read raw bytes and return as bytes."""
        ...
        
    def read_lines(self, start_line: int, end_line: int) -> List[str]:
        """Zero-copy read lines and return as list[str]."""
        ...
        
    def read_line_bytes(self, start_bytes: int, end_bytes: int) -> List[str]:
        """Read line bytes and return as list[str]."""
        ...
        
    def read_lines_json(self, start_line: int, end_line: int) -> List[Dict[str, Any]]:
        """Read lines and parse as JSON, return as list[dict]."""
        ...
        
    def read_line_bytes_json(self, start_bytes: int, end_bytes: int) -> List[Dict[str, Any]]:
        """Read line bytes and parse as JSON, return as list[dict]."""
        ...
    
    @property
    def gz_path(self) -> str:
        """Path to the gzip file."""
        ...
    
    @property
    def idx_path(self) -> str:
        """Path to the index file."""
        ...
    
    @property
    def checkpoint_size(self) -> int:
        """Checkpoint size in bytes."""
        ...
        
    @property
    def buffer_size(self) -> int:
        """Internal buffer size for read operations."""
        ...
    
    @buffer_size.setter
    def buffer_size(self, size: int) -> None:
        """Set internal buffer size for read operations."""
        ...
    
    def __enter__(self) -> 'DFTracerReader':
        """Enter the runtime context for the with statement."""
        ...
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the runtime context for the with statement."""
        ... 
