# Call Graph Extraction Tool

## Overview
This tool extracts hierarchical function call graphs from DFTracer trace files. It organizes the data by process ID and shows the parent-child relationships between function calls with proper indentation.

## Usage
```bash
./build/bin/dftracer_call_graph <trace_file>
```

## Example
```bash
./build/bin/dftracer_call_graph trace/bert_v100-1.pfw
```

## Output Format
The tool outputs:
- Process count summary
- For each process ID:
  - Header with process ID and total calls
  - Hierarchical call tree with:
    - Function name and category in brackets
    - Level number (indentation shows hierarchy)
    - Duration in microseconds
    - Timestamp

## Features
- **Process Organization**: Uses process ID as top-level key
- **Hierarchy Levels**: Shows parent-child relationships with indentation
- **Complete Metrics**: Preserves duration, timestamps, and other trace data
- **Multi-Process Support**: Handles traces with multiple processes
- **Standalone Tool**: Can be used independently for analysis

## Implementation
- **Header**: `include/dftracer/utils/call_graph/call_graph.h`
- **Implementation**: `src/dftracer/utils/call_graph/call_graph.cpp`
- **CLI Tool**: `src/dftracer/utils/bin/dftracer_call_graph.cpp`

## Data Structures
- `FunctionCall`: Individual function call with metrics
- `ProcessCallGraph`: All calls for a single process
- `CallGraph`: Complete multi-process call graph

The tool builds parent-child relationships and displays them hierarchically, making it easy to understand the execution flow and call patterns in traced applications.