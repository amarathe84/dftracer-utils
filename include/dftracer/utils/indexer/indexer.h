#ifndef DFTRACER_UTILS_INDEXER_INDEXER_H
#define DFTRACER_UTILS_INDEXER_INDEXER_H

#include <dftracer/utils/indexer/checkpoint.h>

#ifdef __cplusplus
#include <dftracer/utils/common/constants.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace dftracer::utils {

struct IndexerImplementor;
class Indexer {
   public:
    static constexpr std::uint64_t DEFAULT_CHECKPOINT_SIZE =
        constants::indexer::DEFAULT_CHECKPOINT_SIZE;
    Indexer(const std::string &gz_path, const std::string &idx_path,
            std::uint64_t checkpoint_size = DEFAULT_CHECKPOINT_SIZE,
            bool force = false);
    ~Indexer();
    Indexer(const Indexer &) = delete;
    Indexer &operator=(const Indexer &) = delete;
    Indexer(Indexer &&other) noexcept;
    Indexer &operator=(Indexer &&other) noexcept;
    void build() const;
    bool need_rebuild() const;
    bool exists() const;

    // Metadata
    const std::string &get_idx_path() const;
    const std::string &get_gz_path() const;
    std::uint64_t get_checkpoint_size() const;
    std::uint64_t get_max_bytes() const;
    std::uint64_t get_num_lines() const;
    int get_file_id() const;

    // Lookup
    int find_file_id(const std::string &gz_path) const;
    bool find_checkpoint(size_t target_offset,
                         IndexCheckpoint &checkpoint) const;
    std::vector<IndexCheckpoint> get_checkpoints() const;
    std::vector<IndexCheckpoint> get_checkpoints_for_line_range(
        std::uint64_t start_line, std::uint64_t end_line) const;

   private:
    std::unique_ptr<IndexerImplementor> p_impl_;
};

}  // namespace dftracer::utils

extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

typedef void *dft_indexer_handle_t;
dft_indexer_handle_t dft_indexer_create(const char *gz_path,
                                        const char *idx_path,
                                        uint64_t checkpoint_size,
                                        int force_rebuild);
int dft_indexer_build(dft_indexer_handle_t indexer);
int dft_indexer_need_rebuild(dft_indexer_handle_t indexer);
int dft_indexer_exists(dft_indexer_handle_t indexer);
uint64_t dft_indexer_get_max_bytes(dft_indexer_handle_t indexer);
uint64_t dft_indexer_get_num_lines(dft_indexer_handle_t indexer);
int dft_indexer_find_file_id(dft_indexer_handle_t indexer, const char *gz_path);
int dft_indexer_find_checkpoint(dft_indexer_handle_t indexer,
                                size_t target_offset,
                                dft_indexer_checkpoint_t *checkpoint);
int dft_indexer_get_checkpoints(dft_indexer_handle_t indexer,
                                dft_indexer_checkpoint_t **checkpoints,
                                size_t *count);
void dft_indexer_destroy(dft_indexer_handle_t indexer);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // DFTRACER_UTILS_INDEXER_INDEXER_H
