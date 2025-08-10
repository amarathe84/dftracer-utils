# Cython declaration file for dftracer reader
# This file declares the C API that we'll wrap

from libc.stdint cimport uint64_t
from libc.stddef cimport size_t

cdef extern from "sqlite3.h":
    ctypedef struct sqlite3:
        pass
    
    int sqlite3_open(const char *filename, sqlite3 **ppDb)
    int sqlite3_close(sqlite3 *db)
    const char *sqlite3_errmsg(sqlite3 *db)
    
    # SQLite constants
    int SQLITE_OK

cdef extern from "reader/reader.h":
    int read_data_range_bytes(
        sqlite3 *db, 
        const char *gz_path, 
        uint64_t start_bytes, 
        uint64_t end_bytes, 
        char **output, 
        size_t *output_size
    )

cdef extern from "reader/indexer.h":
    int init_schema(sqlite3 *db)
    int build_gzip_index(sqlite3 *db, int file_id, const char *gz_path, long long chunk_size)

