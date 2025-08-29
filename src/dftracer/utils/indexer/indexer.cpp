#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/common/inflater.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/checkpoint.h>
#include <dftracer/utils/indexer/error.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/indexer/indexer_impl.h>
#include <dftracer/utils/utils/filesystem.h>
#include <sqlite3.h>
#include <zlib.h>

#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <vector>

namespace dftracer::utils {

Indexer::Indexer(const std::string &gz_path, const std::string &idx_path,
                 size_t ckpt_size, bool force_rebuild)
    : p_impl_(new IndexerImplementor(gz_path, idx_path, ckpt_size,
                                     force_rebuild)) {}

Indexer::~Indexer() = default;

Indexer::Indexer(Indexer &&other) noexcept : p_impl_(other.p_impl_.release()) {}

Indexer &Indexer::operator=(Indexer &&other) noexcept {
    if (this != &other) {
        p_impl_.reset(other.p_impl_.release());
    }
    return *this;
}

void Indexer::build() const { p_impl_->build(); }

bool Indexer::need_rebuild() const { return p_impl_->need_rebuild(); }

bool Indexer::exists() const { return p_impl_->exists(); }

const std::string &Indexer::get_gz_path() const { return p_impl_->gz_path; }

const std::string &Indexer::get_idx_path() const { return p_impl_->idx_path; }

std::size_t Indexer::get_checkpoint_size() const {
    return p_impl_->get_checkpoint_size();
}

uint64_t Indexer::get_max_bytes() const { return p_impl_->get_max_bytes(); }

uint64_t Indexer::get_num_lines() const { return p_impl_->get_num_lines(); }

int Indexer::find_file_id(const std::string &gz_path) const {
    return p_impl_->find_file_id(gz_path);
}

bool Indexer::find_checkpoint(size_t target_offset,
                              IndexCheckpoint &checkpoint) const {
    return p_impl_->find_checkpoint(target_offset, checkpoint);
}

std::vector<IndexCheckpoint> Indexer::get_checkpoints() const {
    return p_impl_->get_checkpoints();
}

}  // namespace dftracer::utils
