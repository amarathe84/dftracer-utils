# Cython implementation for dftracer reader
# distutils: language = c++
# distutils: sources = ../reader/reader.cpp ../reader/indexer.cpp

import os
# from libc.stdlib cimport malloc, free
# from libc.stdint cimport uint64_t
# from libc.stdlib cimport size_t

cimport reader

class ReaderError(Exception):
    """Exception raised for reader-related errors."""
    pass

class IndexerError(Exception):
    """Exception raised for indexer-related errors."""
    pass

cdef class GzipReader:
    """
    A class for reading gzipped files using an SQLite index.
    
    This class provides efficient random access to gzipped files by using
    a pre-built SQLite index that contains chunk information.
    """
    
    cdef reader.sqlite3 *db
    cdef str db_path
    cdef str gz_path
    
    def __cinit__(self, str db_path, str gz_path):
        """
        Initialize the GzipReader.
        
        Args:
            db_path: Path to the SQLite index database
            gz_path: Path to the gzipped file
        """
        self.db = NULL
        self.db_path = db_path
        self.gz_path = gz_path
        
        # Validate files exist
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database file not found: {db_path}")
        if not os.path.exists(gz_path):
            raise FileNotFoundError(f"Gzip file not found: {gz_path}")
    
    # def __dealloc__(self):
    #     """Clean up database connection."""
    #     if self.db != NULL:
    #         reader.sqlite3_close(self.db)
    
    # def open(self):
    #     """
    #     Open the SQLite database connection.
        
    #     Raises:
    #         ReaderError: If database cannot be opened
    #     """
    #     if self.db != NULL:
    #         return  # Already open
            
    #     cdef int result = reader.sqlite3_open(self.db_path.encode('utf-8'), &self.db)
    #     if result != reader.SQLITE_OK:
    #         error_msg = reader.sqlite3_errmsg(self.db).decode('utf-8')
    #         raise ReaderError(f"Failed to open database: {error_msg}")
    
    # def close(self):
    #     """Close the database connection."""
    #     if self.db != NULL:
    #         reader.sqlite3_close(self.db)
    #         self.db = NULL
    
    # def read_range_bytes(self, uint64_t start_bytes, uint64_t end_bytes):
    #     """
    #     Read a range of bytes from the gzipped file.
        
    #     Args:
    #         start_bytes: Starting byte position
    #         end_bytes: Ending byte position
            
    #     Returns:
    #         str: The extracted data as a string
            
    #     Raises:
    #         ReaderError: If reading fails
    #     """
    #     if self.db == NULL:
    #         raise ReaderError("Database not opened. Call open() first.")
        
    #     if start_bytes >= end_bytes:
    #         raise ValueError("start_bytes must be less than end_bytes")
        
    #     cdef char *output = NULL
    #     cdef size_t output_size = 0
        
    #     try:
    #         cdef int result = reader.read_data_range_bytes(
    #             self.db,
    #             self.gz_path.encode('utf-8'),
    #             start_bytes,
    #             end_bytes,
    #             &output,
    #             &output_size
    #         )
            
    #         if result != 0:
    #             raise ReaderError(f"Failed to read data range [{start_bytes}, {end_bytes}]")
            
    #         if output == NULL:
    #             return ""
            
    #         # Convert to Python string
    #         try:
    #             result_str = output[:output_size].decode('utf-8')
    #             return result_str
    #         except UnicodeDecodeError:
    #             # If UTF-8 decoding fails, return as bytes
    #             result_bytes = output[:output_size]
    #             return result_bytes
    #     finally:
    #         if output != NULL:
    #             free(output)
    
    # def read_range_mb(self, double start_mb, double end_mb):
    #     """
    #     Read a range in megabytes from the gzipped file.
        
    #     Args:
    #         start_mb: Starting position in megabytes
    #         end_mb: Ending position in megabytes
            
    #     Returns:
    #         str: The extracted data as a string
    #     """
    #     cdef uint64_t start_bytes = <uint64_t>(start_mb * 1024 * 1024)
    #     cdef uint64_t end_bytes = <uint64_t>(end_mb * 1024 * 1024)
    #     return self.read_range_bytes(start_bytes, end_bytes)
    
    # def __enter__(self):
    #     """Context manager entry."""
    #     self.open()
    #     return self
    
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     """Context manager exit."""
    #     self.close()

# def create_index(str gzfile_path, str db_path):
#     """
#     Create an SQLite index for a gzipped file.
    
#     Args:
#         gzfile_path: Path to the gzipped file to index
#         db_path: Path where to create the SQLite database
        
#     Raises:
#         IndexerError: If indexing fails
#         FileNotFoundError: If gzipped file doesn't exist
#     """
#     if not os.path.exists(gzfile_path):
#         raise FileNotFoundError(f"Gzip file not found: {gzfile_path}")
    
#     cdef int result = reader.index_gzip_file(
#         gzfile_path.encode('utf-8'),
#         db_path.encode('utf-8')
#     )
    
#     if result != 0:
#         raise IndexerError(f"Failed to create index for {gzfile_path}")

# # Convenience function for one-shot reading
# def read_gzip_range(str db_path, str gz_path, uint64_t start_bytes, uint64_t end_bytes):
#     """
#     Convenience function to read a byte range from a gzipped file.
    
#     Args:
#         db_path: Path to the SQLite index database
#         gz_path: Path to the gzipped file
#         start_bytes: Starting byte position
#         end_bytes: Ending byte position
        
#     Returns:
#         str: The extracted data as a string
#     """
#     with GzipReader(db_path, gz_path) as reader_obj:
#         return reader_obj.read_range_bytes(start_bytes, end_bytes)

# def read_gzip_range_mb(str db_path, str gz_path, double start_mb, double end_mb):
#     """
#     Convenience function to read a megabyte range from a gzipped file.
    
#     Args:
#         db_path: Path to the SQLite index database
#         gz_path: Path to the gzipped file
#         start_mb: Starting position in megabytes
#         end_mb: Ending position in megabytes
        
#     Returns:
#         str: The extracted data as a string
#     """
#     with GzipReader(db_path, gz_path) as reader_obj:
#         return reader_obj.read_range_mb(start_mb, end_mb)
