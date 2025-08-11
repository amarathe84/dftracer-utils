#ifndef __DFTRACER_UTILS_READER_INDEXER_H
#define __DFTRACER_UTILS_READER_INDEXER_H

#include <sqlite3.h>

#ifdef __cplusplus
extern "C" {
#endif

int dft_indexer_init(sqlite3 *db);
int dft_indexer_build(sqlite3 *db, int file_id, const char *gz_path, long long chunk_size);

#ifdef __cplusplus
} // extern "C"

namespace dft {
namespace indexer {
    int init(sqlite3 *db);
    int build(sqlite3 *db, int file_id, const char *gz_path, long long chunk_size);
}
}
#endif

#endif // __DFTRACER_UTILS_READER_INDEXER_H
