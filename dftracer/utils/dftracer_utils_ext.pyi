"""Type stubs for dftracer_utils_ext module."""

from typing import Optional, Union, List, overload, Dict, Any, Iterator
from typing_extensions import Self, Literal

# ========== JSON FUNCTIONALITY ==========

class JsonDocument:
    """Lazy JSON document wrapper for high-performance dict-like access."""
    
    def __init__(self, json_str: str) -> None:
        """Create JsonDocument from JSON string."""
        ...
    
    def __getitem__(self, key: str) -> Any:
        """Get item by key."""
        ...
    
    def __contains__(self, key: str) -> bool:
        """Check if key exists."""
        ...
    
    def __len__(self) -> int:
        """Get number of keys."""
        ...
    
    def __str__(self) -> str:
        """String representation."""
        ...
    
    def __repr__(self) -> str:
        """String representation."""
        ...
    
    def __iter__(self) -> Iterator[str]:
        """Iterator over keys."""
        ...
    
    def keys(self) -> List[str]:
        """Get all keys."""
        ...
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value with optional default."""
        ...

# ========== INDEXER FUNCTIONALITY ==========

class CheckpointInfo:
    """Information about a checkpoint in the index."""
    checkpoint_idx: int
    uc_offset: int
    uc_size: int
    c_offset: int
    c_size: int
    bits: int
    dict_compressed: bytes

class DFTracerIndexer:
    """DFTracer indexer for creating and managing gzip file indices."""
    
    def __init__(
        self, 
        gz_path: str, 
        idx_path: Optional[str] = None,
        checkpoint_size: int = 1048576
    ) -> None:
        """Create a DFTracer indexer for a gzip file."""
        ...
    
    def index(self) -> None:
        """Index the gzip file."""
        ...
    
    def open(self) -> None:
        """Open the index database."""
        ...
    
    def close(self) -> None:
        """Close the index database."""
        ...
    
    def __enter__(self) -> Self:
        """Enter context manager."""
        ...
    
    def __exit__(self, *args: Any) -> bool:
        """Exit context manager."""
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
    def is_open(self) -> bool:
        """Whether the database is open."""
        ...

# ========== READER FUNCTIONALITY ==========

class DFTracerBytesIterator:
    """Iterator for reading bytes chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> str: ...

class DFTracerLineBytesIterator:
    """Iterator for reading line bytes chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> List[str]: ...

class DFTracerLinesIterator:
    """Iterator for reading lines chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> List[str]: ...

class DFTracerJsonLinesIterator:
    """Iterator for reading JSON lines chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> List[JsonDocument]: ...

class DFTracerJsonLinesBytesIterator:
    """Iterator for reading JSON lines bytes chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> List[JsonDocument]: ...

class DFTracerBytesRangeIterator:
    """Range iterator for reading bytes chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> str: ...
    
    @property
    def start(self) -> int: ...
    @property
    def end(self) -> int: ...
    @property
    def step(self) -> int: ...
    @property
    def current(self) -> int: ...

class DFTracerLineBytesRangeIterator:
    """Range iterator for reading line bytes chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> List[str]: ...
    
    @property
    def start(self) -> int: ...
    @property
    def end(self) -> int: ...
    @property
    def step(self) -> int: ...
    @property
    def current(self) -> int: ...

class DFTracerLinesRangeIterator:
    """Range iterator for reading lines chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> List[str]: ...
    
    @property
    def start(self) -> int: ...
    @property
    def end(self) -> int: ...
    @property
    def step(self) -> int: ...
    @property
    def current(self) -> int: ...

class DFTracerJsonLinesRangeIterator:
    """Range iterator for reading JSON lines chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> List[JsonDocument]: ...
    
    @property
    def start(self) -> int: ...
    @property
    def end(self) -> int: ...
    @property
    def step(self) -> int: ...
    @property
    def current(self) -> int: ...

class DFTracerJsonLinesBytesRangeIterator:
    """Range iterator for reading JSON lines bytes chunks from DFTracer traces."""
    
    def __iter__(self) -> Self: ...
    def __next__(self) -> List[JsonDocument]: ...
    
    @property
    def start(self) -> int: ...
    @property
    def end(self) -> int: ...
    @property
    def step(self) -> int: ...
    @property
    def current(self) -> int: ...

class DFTracerBytesReader:
    """DFTracer reader for reading bytes from gzip files."""
    
    @overload
    def __init__(
        self, 
        gzip_path: str, 
        index_path: Optional[str] = None
    ) -> None: ...
    
    @overload
    def __init__(self, indexer: DFTracerIndexer) -> None: ...
    
    def get_max_bytes(self) -> int:
        """Get the maximum byte position available in the file."""
        ...
    
    def get_num_lines(self) -> int:
        """Get the number of lines in the file."""
        ...
    
    def iter(self, step: int = 4194304) -> DFTracerBytesIterator:
        """Get iterator with optional step size."""
        ...
    
    def __iter__(self) -> Self:
        """Get iterator for the reader."""
        ...
    
    def __next__(self) -> str:
        """Get next chunk with default step."""
        ...
    
    def read(self, start: int, end: int) -> str:
        """Read a range from the gzip file."""
        ...
    
    def __enter__(self) -> Self:
        """Enter context manager."""
        ...
    
    def __exit__(self, *args: Any) -> bool:
        """Exit context manager."""
        ...
    
    @property
    def gzip_path(self) -> str:
        """Path to the gzip file."""
        ...
    
    @property
    def index_path(self) -> str:
        """Path to the index file."""
        ...
    
    @property
    def is_open(self) -> bool:
        """Whether the database is open."""
        ...

class DFTracerLineBytesReader:
    """DFTracer reader for reading line bytes from gzip files."""
    
    @overload
    def __init__(
        self, 
        gzip_path: str, 
        index_path: Optional[str] = None
    ) -> None: ...
    
    @overload
    def __init__(self, indexer: DFTracerIndexer) -> None: ...
    
    def get_max_bytes(self) -> int:
        """Get the maximum byte position available in the file."""
        ...
    
    def get_num_lines(self) -> int:
        """Get the number of lines in the file."""
        ...
    
    def iter(self, step: int = 4194304) -> DFTracerLineBytesIterator:
        """Get iterator with optional step size."""
        ...
    
    def __iter__(self) -> Self:
        """Get iterator for the reader."""
        ...
    
    def __next__(self) -> List[str]:
        """Get next chunk with default step."""
        ...
    
    def read(self, start: int, end: int) -> List[str]:
        """Read a range from the gzip file."""
        ...
    
    def __enter__(self) -> Self:
        """Enter context manager."""
        ...
    
    def __exit__(self, *args: Any) -> bool:
        """Exit context manager."""
        ...
    
    @property
    def gzip_path(self) -> str:
        """Path to the gzip file."""
        ...
    
    @property
    def index_path(self) -> str:
        """Path to the index file."""
        ...
    
    @property
    def is_open(self) -> bool:
        """Whether the database is open."""
        ...

class DFTracerLinesReader:
    """DFTracer reader for reading lines from gzip files."""
    
    @overload
    def __init__(
        self, 
        gzip_path: str, 
        index_path: Optional[str] = None
    ) -> None: ...
    
    @overload
    def __init__(self, indexer: DFTracerIndexer) -> None: ...
    
    def get_max_bytes(self) -> int:
        """Get the maximum byte position available in the file."""
        ...
    
    def get_num_lines(self) -> int:
        """Get the number of lines in the file."""
        ...
    
    def iter(self, step: int = 1) -> DFTracerLinesIterator:
        """Get iterator with optional step size."""
        ...
    
    def __iter__(self) -> Self:
        """Get iterator for the reader."""
        ...
    
    def __next__(self) -> List[str]:
        """Get next chunk with default step."""
        ...
    
    def read(self, start: int, end: int) -> List[str]:
        """Read a range from the gzip file."""
        ...
    
    def __enter__(self) -> Self:
        """Enter context manager."""
        ...
    
    def __exit__(self, *args: Any) -> bool:
        """Exit context manager."""
        ...
    
    @property
    def gzip_path(self) -> str:
        """Path to the gzip file."""
        ...
    
    @property
    def index_path(self) -> str:
        """Path to the index file."""
        ...
    
    @property
    def is_open(self) -> bool:
        """Whether the database is open."""
        ...

class DFTracerJsonLinesReader:
    """DFTracer reader for reading JSON lines from gzip files."""
    
    @overload
    def __init__(
        self, 
        gzip_path: str, 
        index_path: Optional[str] = None
    ) -> None: ...
    
    @overload
    def __init__(self, indexer: DFTracerIndexer) -> None: ...
    
    def get_max_bytes(self) -> int:
        """Get the maximum byte position available in the file."""
        ...
    
    def get_num_lines(self) -> int:
        """Get the number of lines in the file."""
        ...
    
    def iter(self, step: int = 1) -> DFTracerJsonLinesIterator:
        """Get iterator with optional step size."""
        ...
    
    def __iter__(self) -> Self:
        """Get iterator for the reader."""
        ...
    
    def __next__(self) -> List[JsonDocument]:
        """Get next chunk with default step."""
        ...
    
    def read(self, start: int, end: int) -> List[JsonDocument]:
        """Read a range from the gzip file and return as Python list of dictionaries."""
        ...
    
    def __enter__(self) -> Self:
        """Enter context manager."""
        ...
    
    def __exit__(self, *args: Any) -> bool:
        """Exit context manager."""
        ...
    
    @property
    def gzip_path(self) -> str:
        """Path to the gzip file."""
        ...
    
    @property
    def index_path(self) -> str:
        """Path to the index file."""
        ...
    
    @property
    def is_open(self) -> bool:
        """Whether the database is open."""
        ...

class DFTracerJsonLinesBytesReader:
    """DFTracer reader for reading JSON lines bytes from gzip files."""
    
    @overload
    def __init__(
        self, 
        gzip_path: str, 
        index_path: Optional[str] = None
    ) -> None: ...
    
    @overload
    def __init__(self, indexer: DFTracerIndexer) -> None: ...
    
    def get_max_bytes(self) -> int:
        """Get the maximum byte position available in the file."""
        ...
    
    def get_num_lines(self) -> int:
        """Get the number of lines in the file."""
        ...
    
    def iter(self, step: int = 4194304) -> DFTracerJsonLinesBytesIterator:
        """Get iterator with optional step size."""
        ...
    
    def __iter__(self) -> Self:
        """Get iterator for the reader."""
        ...
    
    def __next__(self) -> List[JsonDocument]:
        """Get next chunk with default step."""
        ...
    
    def read(self, start: int, end: int) -> List[JsonDocument]:
        """Read a range from the gzip file and return as Python list of dictionaries."""
        ...
    
    def __enter__(self) -> Self:
        """Enter context manager."""
        ...
    
    def __exit__(self, *args: Any) -> bool:
        """Exit context manager."""
        ...
    
    @property
    def gzip_path(self) -> str:
        """Path to the gzip file."""
        ...
    
    @property
    def index_path(self) -> str:
        """Path to the index file."""
        ...
    
    @property
    def is_open(self) -> bool:
        """Whether the database is open."""
        ...

# Convenience alias
DFTracerReader = DFTracerLineBytesReader

@overload
def dft_reader_range(
    reader: DFTracerLineBytesReader, 
    start: int, 
    end: int, 
    mode: Literal["line_bytes"] = "line_bytes",
    step: int = 4194304
) -> DFTracerLineBytesRangeIterator: ...

@overload
def dft_reader_range(
    reader: DFTracerBytesReader, 
    start: int, 
    end: int, 
    mode: Literal["bytes"] = "bytes",
    step: int = 4194304
) -> DFTracerBytesRangeIterator: ...

@overload
def dft_reader_range(
    reader: DFTracerLinesReader, 
    start: int, 
    end: int, 
    mode: Literal["lines"] = "lines",
    step: int = 1
) -> DFTracerLinesRangeIterator: ...

# ========== UTILITY FUNCTIONS ==========

def set_log_level(level: str) -> None:
    """Set the global log level using a string (trace, debug, info, warn, error, critical, off)."""
    ...

def set_log_level_int(level: int) -> None:
    """Set the global log level using an integer (0=trace, 1=debug, 2=info, 3=warn, 4=error, 5=critical, 6=off)."""
    ...

def get_log_level_string() -> str:
    """Get the current global log level as a string."""
    ...

def get_log_level_int() -> int:
    """Get the current global log level as an integer."""
    ...