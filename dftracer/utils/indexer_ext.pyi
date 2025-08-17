"""Type stubs for indexer_ext module."""

from typing import Optional, List
from typing_extensions import Self

class CheckpointInfo:
    """Information about a checkpoint in the compressed file."""
    
    checkpoint_idx: int
    uc_offset: int
    uc_size: int
    c_offset: int
    c_size: int
    bits: int
    dict_compressed: List[int]
    num_lines: int

class DFTracerIndexer:
    """DFT indexer for managing compressed trace file indices."""
    
    def __init__(
        self,
        gz_path: str,
        idx_path: Optional[str] = None,
        checkpoint_size: int = 33_554_432,  # 32MB
        force_rebuild: bool = False
    ) -> None:
        """Create a DFTracer indexer for a gzip file and its index.
        
        Args:
            gz_path: Path to the gzipped trace file
            idx_path: Path to the index file (defaults to gz_path + ".idx")
            checkpoint_size: Checkpoint size for indexing in bytes
            force_rebuild: Force rebuild even if index exists and is valid
        """
        ...
    
    def build(self) -> None:
        """Build or rebuild the index."""
        ...
    
    def need_rebuild(self) -> bool:
        """Check if a rebuild is needed.
        
        Returns:
            True if rebuild is needed, False if not needed
        """
        ...
    
    def is_valid(self) -> bool:
        """Check if the indexer is valid.
        
        Returns:
            True if indexer is valid, False otherwise
        """
        ...
    
    def get_max_bytes(self) -> int:
        """Get the maximum uncompressed bytes in the indexed file.
        
        Returns:
            Maximum uncompressed bytes, or 0 if index doesn't exist
        """
        ...
    
    def get_num_lines(self) -> int:
        """Get the total number of lines in the indexed file.
        
        Returns:
            Total number of lines, or 0 if index doesn't exist
        """
        ...
    
    def find_file_id(self, gz_path: str) -> int:
        """Find the database file ID for a given gzip path.
        
        Args:
            gz_path: Path to the gzipped file
            
        Returns:
            File ID, or -1 if not found
        """
        ...
    
    def get_checkpoints(self) -> List[CheckpointInfo]:
        """Get all checkpoints for this file as a list.
        
        Returns:
            Vector of all checkpoints ordered by uncompressed offset
        """
        ...
    
    def find_checkpoints_by_line_range(
        self, 
        start_line: int, 
        end_line: int
    ) -> List[CheckpointInfo]:
        """Find checkpoints that contain data for a specific line range.
        
        Args:
            start_line: Starting line number (1-based)
            end_line: Ending line number (1-based, inclusive)
            
        Returns:
            Vector of checkpoints that cover the specified line range
        """
        ...
    
    def find_checkpoint(
        self, 
        target_offset: int
    ) -> Optional[CheckpointInfo]:
        """Find the best checkpoint for a given uncompressed offset.
        
        Args:
            target_offset: Target uncompressed offset
            
        Returns:
            CheckpointInfo if found, None otherwise
        """
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
    
    def __enter__(self) -> Self:
        """Enter context manager."""
        ...
    
    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        """Exit context manager."""
        ...
        ...
