# DFTracer Reader Python Binding

A Python binding for the DFTracer utilities reader, providing efficient access to compressed trace files with indexing support.

## Features

- **Fast random access**: Read specific byte or megabyte ranges from compressed files
- **Index-based navigation**: Uses SQLite index for efficient seeking
- **Context manager support**: Automatic resource management
- **Line-aware reading**: Ensures complete JSON lines are returned
- **Cross-platform**: Works on Linux and macOS

## Installation

```bash
pip install --no-build-isolation -ve .
```

## Quick Start

```python
from dft_reader import DFTracerReader

# Using context manager (recommended)
with DFTracerReader("trace.pfw.gz") as reader:
    # Read first 1KB
    data = reader.read(0, 1024)
    print(f"Read {len(data)} bytes")
    
    # Read first megabyte
    data = reader.read_mb(0, 1)
    print(f"Read {len(data)} bytes")
```

## API Reference

### DFTracerReader

The main class for reading compressed trace files.

#### Constructor

```python
DFTracerReader(gzip_path: str, index_path: Optional[str] = None)
```

**Parameters:**
- `gzip_path`: Path to the compressed trace file (.pfw.gz)
- `index_path`: Optional path to the index file. If not provided, defaults to `{gzip_path}.idx`

**Example:**
```python
# Auto-detect index file
reader = DFTracerReader("trace.pfw.gz")

# Custom index file
reader = DFTracerReader("trace.pfw.gz", "custom.idx")
```

#### Methods

##### `read_range_bytes(start_bytes: int, end_bytes: int) -> str`

Read a range of bytes from the compressed file.

**Parameters:**
- `start_bytes`: Starting byte position (inclusive)
- `end_bytes`: Ending byte position (exclusive)

**Returns:** String containing the decompressed data

**Example:**
```python
# Read bytes 1000-2000
data = reader.read_range_bytes(1000, 2000)
```

##### `read_range_megabytes(start_mb: float, end_mb: float) -> str`

Read a range of megabytes from the compressed file.

**Parameters:**
- `start_mb`: Starting position in megabytes
- `end_mb`: Ending position in megabytes

**Returns:** String containing the decompressed data

**Example:**
```python
# Read from 0.5MB to 1.5MB
data = reader.read_range_megabytes(0.5, 1.5)
```

##### `open_database() -> None`

Manually open the index database. Called automatically by constructor.

##### `close_database() -> None`

Manually close the index database. Called automatically by context manager.

#### Properties

##### `gzip_path: str`

Path to the gzip file (read-only).

##### `index_path: str`

Path to the index file (read-only).

##### `is_open: bool`

Whether the database is currently open (read-only).

## Usage Patterns

### Context Manager (Recommended)

```python
with DFTracerReader("trace.pfw.gz") as reader:
    data = reader.read_range_bytes(0, 1024)
    # Database automatically closed when exiting context
```

### Manual Resource Management

```python
reader = DFTracerReader("trace.pfw.gz")
try:
    data = reader.read_range_bytes(0, 1024)
finally:
    reader.close_database()
```

### Reading JSON Records

The reader is line-aware and ensures complete JSON records:

```python
with DFTracerReader("trace.pfw.gz") as reader:
    data = reader.read_range_megabytes(0, 1)
    lines = data.strip().split('\\n')
    
    # Skip the opening '[' if present
    for line in lines[1:]:
        if line.strip() and line != ']':
            # Remove trailing comma if present
            json_line = line.rstrip(',')
            # Parse JSON record
            import json
            record = json.loads(json_line)
            print(record)
```

## Error Handling

The binding raises `RuntimeError` for various error conditions:

```python
try:
    with DFTracerReader("nonexistent.pfw.gz") as reader:
        data = reader.read_range_bytes(0, 1024)
except RuntimeError as e:
    print(f"Error: {e}")
```

Common errors:
- Index file not found
- Corrupted index file
- Invalid byte ranges
- I/O errors

## Performance Notes

- The reader uses SQLite indexing for efficient random access
- Reading complete JSON lines may extend the requested range slightly
- Megabyte ranges are converted to byte ranges internally
- The underlying C++ implementation provides high performance

## Index File Requirements

The reader requires an index file created by the `dft_reader` command-line tool:

```bash
# Create index for trace file
dft_reader index trace.pfw.gz

# This creates trace.pfw.gz.idx
```

## Example: Processing Large Files

```python
def process_trace_chunks(filename, chunk_size_mb=10):
    \"\"\"Process a trace file in chunks\"\"\"
    with DFTracerReader(filename) as reader:
        offset = 0
        while True:
            try:
                data = reader.read_range_megabytes(offset, offset + chunk_size_mb)
                if not data:
                    break
                    
                # Process chunk
                lines = data.strip().split('\\n')
                for line in lines[1:]:  # Skip opening '['
                    if line.strip() and line != ']':
                        yield line.rstrip(',')
                        
                offset += chunk_size_mb
            except RuntimeError:
                break  # End of file or error

# Usage
for json_line in process_trace_chunks("large_trace.pfw.gz"):
    record = json.loads(json_line)
    # Process record
```
