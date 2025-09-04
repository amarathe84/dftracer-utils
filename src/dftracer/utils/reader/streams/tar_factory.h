#ifndef DFTRACER_UTILS_READER_STREAMS_TAR_FACTORY_H
#define DFTRACER_UTILS_READER_STREAMS_TAR_FACTORY_H

#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/indexer/tar_indexer.h>
#include <dftracer/utils/reader/streams/tar_byte_stream.h>
#include <dftracer/utils/reader/streams/tar_line_byte_stream.h>

#include <memory>
#include <string>

/**
 * Factory for creating TAR-aware stream objects that handle logical content mapping
 */
class TarStreamFactory {
   private:
    dftracer::utils::Indexer &indexer_;
    TarIndexer* tar_indexer_;

   public:
    explicit TarStreamFactory(dftracer::utils::Indexer &indexer) 
        : indexer_(indexer) {
        tar_indexer_ = dynamic_cast<TarIndexer*>(&indexer);
        if (!tar_indexer_) {
            throw std::runtime_error("TarStreamFactory requires a TarIndexer");
        }
    }

    std::unique_ptr<TarLineByteStream> create_line_stream(
        const std::string &tar_gz_path, size_t start_bytes, size_t end_bytes) {
        auto session = std::make_unique<TarLineByteStream>();
        session->initialize(tar_gz_path, start_bytes, end_bytes, indexer_);
        return session;
    }

    std::unique_ptr<TarByteStream> create_byte_stream(const std::string &tar_gz_path,
                                                      size_t start_bytes,
                                                      size_t end_bytes) {
        auto session = std::make_unique<TarByteStream>();
        session->initialize(tar_gz_path, start_bytes, end_bytes, indexer_);
        return session;
    }

    bool needs_new_line_stream(const TarLineByteStream *current,
                              const std::string &tar_gz_path, size_t start_bytes,
                              size_t end_bytes) const {
        return !current || !current->matches(tar_gz_path, start_bytes, end_bytes) ||
               current->is_finished();
    }

    bool needs_new_byte_stream(const TarByteStream *current,
                              const std::string &tar_gz_path, size_t start_bytes,
                              size_t end_bytes) const {
        return !current || !current->matches(tar_gz_path, start_bytes, end_bytes) ||
               current->is_finished();
    }
    
    // Check if the indexer is a TAR indexer
    static bool is_tar_indexer(dftracer::utils::Indexer &indexer) {
        return dynamic_cast<TarIndexer*>(&indexer) != nullptr;
    }
    
    // Get TAR-specific metadata
    std::size_t get_num_files() const {
        return tar_indexer_->get_num_files();
    }
    
    std::string get_archive_name() const {
        return tar_indexer_->get_archive_name();
    }
    
    std::vector<TarIndexer::TarFileInfo> list_files() const {
        return tar_indexer_->list_files();
    }
};

#endif  // DFTRACER_UTILS_READER_STREAMS_TAR_FACTORY_H
