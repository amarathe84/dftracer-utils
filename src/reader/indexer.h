#ifndef __DFTRACER_UTILS_READER_INDEXER_H
#define __DFTRACER_UTILS_READER_INDEXER_H

#include <sqlite3.h>

#ifdef __cplusplus
extern "C" {
#endif

int init_schema(sqlite3 *db);
int build_gzip_index(sqlite3 *db, int file_id, const char *gz_path, long long chunk_size);

#ifdef __cplusplus
}
#endif

#endif // __DFTRACER_UTILS_READER_INDEXER_H
