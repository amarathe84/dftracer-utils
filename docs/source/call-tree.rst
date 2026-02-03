Call Tree Utility
=================

The call tree utility provides MPI-parallel capabilities for building hierarchical call trees from DFTracer trace files. It analyzes trace files to reconstruct the calling relationships between functions, creating a tree structure that represents the flow of traced applications.

Overview
--------

The Call Tree utility is designed to perform the following tasks:

- Parse plain text or gzipped DFTracer trace files (.pfw, .pfw.gz) and extract function call information
- Build hierarchical call trees showing parent-child relationships between function calls
- Support distributed processing using MPI for handling large-scale trace datasets
- Serialize call trees in multiple formats (binary, JSON/Chrome Tracing) using DFtracer serialization.
- Provide statistical analysis of call patterns and execution timings

Key Features
------------

**MPI-Parallel Processing**
  The utility uses MPI to distribute work across multiple processes, enabling efficient processing of large trace datasets. PIDs (process IDs) from traces are distributed among MPI ranks for parallel graph construction.

**Multi-Format Output**
  Call trees can be saved in:
  
  - Binary format for efficient storage and fast loading
  - JSON format compatible with Chrome Tracing for visualization
  - Text format for human-readable inspection

**Hierarchical Analysis**
  Reconstructs complete call stacks with:
  
  - Parent-child relationships between function calls
  - Call sequences and temporal ordering
  - Per-level and per-category statistics
  - Depth-first traversal capabilities

Output Formats
--------------

Binary Format
~~~~~~~~~~~~~

Efficient binary serialization format for saving and loading call trees. This format preserves all call tree information including:

- Process graphs with unique process/thread keys
- Node hierarchy and relationships
- Timing information (start time, duration)
- Function names, categories, and arguments

JSON Format (Chrome Tracing)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The output follows the Chrome Tracing format specification, allowing visualization of:

- Timeline of function calls
- Nested function relationships
- Duration and timing information
- Custom metadata and arguments

Text Format
~~~~~~~~~~~

Human-readable text representation showing:

- Hierarchical structure with indentation
- Function names and categories
- Timing information at each level
- Statistical summaries

Performance Considerations
--------------------------

**Index Files**
  The utility creates index files for compressed traces to enable efficient random access. These are cached and reused across runs.

**Checkpoint Size**
  Larger checkpoint sizes reduce index file size but may increase memory usage during processing. Default is 32 MB.

**Thread Count**
  Each MPI rank can use multiple threads for parallel processing within the rank. Balance thread count with available cores.

**PID Distribution**
  PIDs are distributed across MPI ranks. More ranks enable processing more PIDs in parallel, but increase communication overhead during the gather phase.

See Also
--------

- :doc:`cpp_api/index` - Full C++ API documentation
- :doc:`quickstart` - Quick start examples
- :doc:`cli` - Command-line tools

