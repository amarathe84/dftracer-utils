#include <dftracer/utils/indexer/indexer_factory.h>
#include <dftracer/utils/indexer/gzip_indexer.h>
#include <dftracer/utils/indexer/tar/tar_indexer.h>
#include <dftracer/utils/indexer/common/format_detector.h>
#include <dftracer/utils/common/logging.h>

#include <filesystem>

namespace dftracer::utils {

std::unique_ptr<Indexer> IndexerFactory::create(
    const std::string &archive_path,
    const std::string &idx_path,
    std::uint64_t checkpoint_size,
    bool force) {
    
    gzip_common::ArchiveFormat format = detect_format(archive_path);
    std::string final_idx_path = idx_path.empty() ? generate_index_path(archive_path, format) : idx_path;
    
    switch (format) {
        case gzip_common::ArchiveFormat::GZIP:
            return std::make_unique<GzipIndexer>(archive_path, final_idx_path, checkpoint_size, force);
            
        case gzip_common::ArchiveFormat::TAR_GZ:
            return std::make_unique<tar_indexer::TarIndexer>(archive_path, final_idx_path, checkpoint_size, force);
            
        case gzip_common::ArchiveFormat::UNKNOWN:
        default:
            DFTRACER_UTILS_LOG_ERROR("Unsupported or unrecognized archive format for file: %s", 
                                      archive_path.c_str());
            return nullptr;
    }
}

gzip_common::ArchiveFormat IndexerFactory::detect_format(const std::string &archive_path) {
    return gzip_common::FormatDetector::detect_format(archive_path);
}

std::string IndexerFactory::generate_index_path(const std::string &archive_path, 
                                                 gzip_common::ArchiveFormat format) {
    // Auto-detect format if not specified
    if (format == gzip_common::ArchiveFormat::UNKNOWN) {
        format = detect_format(archive_path);
    }
    
    switch (format) {
        case gzip_common::ArchiveFormat::GZIP:
            return archive_path + ".idx";
            
        case gzip_common::ArchiveFormat::TAR_GZ:
            return archive_path + ".idx.tar";
            
        case gzip_common::ArchiveFormat::UNKNOWN:
        default:
            // Fallback to generic .idx extension
            DFTRACER_UTILS_LOG_WARN("Unknown format for %s, using generic .idx extension", 
                                     archive_path.c_str());
            return archive_path + ".idx";
    }
}

}  // namespace dftracer::utils
