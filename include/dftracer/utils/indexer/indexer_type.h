#ifndef DFTRACER_UTILS_INDEXER_TYPE_H
#define DFTRACER_UTILS_INDEXER_TYPE_H

namespace dftracer::utils {

/**
 * Enumeration of supported indexer types
 */
enum class IndexerType {
    GZIP,       // GZIP compressed files (.gz)
    TAR_GZ,     // TAR.GZ compressed archives (.tar.gz)
    UNKNOWN     // Unsupported or unrecognized format
};

/**
 * Convert IndexerType to string representation
 */
inline const char* to_string(IndexerType type) {
    switch (type) {
        case IndexerType::GZIP:
            return "GZIP";
        case IndexerType::TAR_GZ:
            return "TAR.GZ";
        case IndexerType::UNKNOWN:
        default:
            return "UNKNOWN";
    }
}

/**
 * Get the appropriate index file extension for an indexer type
 */
inline const char* get_index_extension(IndexerType type) {
    switch (type) {
        case IndexerType::GZIP:
            return ".idx";
        case IndexerType::TAR_GZ:
            return ".tar.idx";
        case IndexerType::UNKNOWN:
        default:
            return ".idx";
    }
}

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_INDEXER_TYPE_H