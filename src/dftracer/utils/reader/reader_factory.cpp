#include <dftracer/utils/reader/reader_factory.h>
#include <dftracer/utils/reader/gzip_reader.h>
#include <dftracer/utils/reader/tar_reader.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/gzip_indexer.h>
#include <dftracer/utils/indexer/tar_indexer.h>

#include <stdexcept>

using dftracer::utils::gzip_common::ArchiveFormat;
using dftracer::utils::gzip_common::FormatDetector;

namespace dftracer::utils {

std::unique_ptr<Reader> ReaderFactory::create(
    const std::string &archive_path, 
    const std::string &idx_path,
    std::size_t index_ckpt_size) {
    
    ArchiveFormat format = detect_format(archive_path);
    
    DFTRACER_UTILS_LOG_DEBUG("ReaderFactory::create_reader - detected format: %d for file: %s",
                             static_cast<int>(format), archive_path.c_str());
    
    switch (format) {
        case ArchiveFormat::GZIP:
            return std::make_unique<GzipReader>(archive_path, idx_path, index_ckpt_size);
            
        case ArchiveFormat::TAR_GZ:
            return std::make_unique<TarReader>(archive_path, idx_path, index_ckpt_size);
            
        default:
            throw std::runtime_error("Unsupported archive format for file: " + archive_path);
    }
}

std::unique_ptr<Reader> ReaderFactory::create(Indexer *indexer) {
    if (!indexer) {
        throw std::invalid_argument("Indexer cannot be null");
    }
    
    if (indexer->get_indexer_type() == IndexerType::TAR_GZ) {
        return std::make_unique<TarReader>(static_cast<TarIndexer *>(indexer));
    }
    
    return std::make_unique<GzipReader>(indexer);
}


ArchiveFormat ReaderFactory::detect_format(const std::string &archive_path) {
    return FormatDetector::detect_format(archive_path);
}

bool ReaderFactory::is_format_supported(ArchiveFormat format) {
    switch (format) {
        case ArchiveFormat::GZIP:
        case ArchiveFormat::TAR_GZ:
            return true;
        default:
            return false;
    }
}

std::string ReaderFactory::get_index_extension(ArchiveFormat format) {
    switch (format) {
        case ArchiveFormat::GZIP:
            return ".idx";
        case ArchiveFormat::TAR_GZ:
            return ".tar.idx";
        default:
            return ".idx";
    }
}

} // namespace dftracer::utils
