#ifndef DFTRACER_UTILS_ANALYZERS_CONFIGURATION_MANAGER_H
#define DFTRACER_UTILS_ANALYZERS_CONFIGURATION_MANAGER_H

#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/common/constants.h>
#include <dftracer/utils/utils/filesystem.h>

#include <cstddef>
#include <cstdint>

namespace dftracer::utils::analyzers {

using namespace dftracer::utils::constants;
using namespace dftracer::utils::analyzers::constants;

class AnalyzerConfigManager {
   public:
    AnalyzerConfigManager(
        double time_granularity = DEFAULT_TIME_GRANULARITY,
        bool checkpoint = false, const std::string& checkpoint_dir = "",
        std::uint64_t checkpoint_size = indexer::DEFAULT_CHECKPOINT_SIZE,
        double time_resolution = DEFAULT_TIME_RESOLUTION)
        : time_granularity_(time_granularity),
          checkpoint_(checkpoint),
          checkpoint_dir_(checkpoint_dir),
          checkpoint_size_(checkpoint_size),
          time_resolution_(time_resolution) {
        if (checkpoint_) {
            if (checkpoint_dir_.empty()) {
                throw std::invalid_argument(
                    "Checkpointing is enabled but checkpoint_dir is empty.");
            }
            // Create checkpoint directory if it doesn't exist
            if (!fs::exists(checkpoint_dir_)) {
                fs::create_directories(checkpoint_dir_);
            }
        }
    }

    inline static AnalyzerConfigManager Default() {
        return AnalyzerConfigManager();
    }
    inline static AnalyzerConfigManager create(
        double time_granularity = DEFAULT_TIME_GRANULARITY,
        bool checkpoint = false, const std::string& checkpoint_dir = "",
        std::uint64_t checkpoint_size = indexer::DEFAULT_CHECKPOINT_SIZE,
        double time_resolution = DEFAULT_TIME_RESOLUTION) {
        return AnalyzerConfigManager(time_granularity, checkpoint,
                                     checkpoint_dir, checkpoint_size,
                                     time_resolution);
    }

    AnalyzerConfigManager(const AnalyzerConfigManager&) = default;
    AnalyzerConfigManager& operator=(const AnalyzerConfigManager&) = default;
    AnalyzerConfigManager(AnalyzerConfigManager&&) = default;
    AnalyzerConfigManager& operator=(AnalyzerConfigManager&&) = default;

    // Getter
    inline double time_granularity() const { return time_granularity_; }
    inline bool checkpoint() const { return checkpoint_; }
    inline const std::string& checkpoint_dir() const { return checkpoint_dir_; }
    inline std::size_t checkpoint_size() const { return checkpoint_size_; }
    inline double time_resolution() const { return time_resolution_; }

    // Setter
    inline AnalyzerConfigManager& set_time_granularity(
        double time_granularity) {
        time_granularity_ = time_granularity;
        return *this;
    }
    inline AnalyzerConfigManager& set_checkpoint(bool checkpoint) {
        checkpoint_ = checkpoint;
        return *this;
    }
    inline AnalyzerConfigManager& set_checkpoint_dir(
        const std::string& checkpoint_dir) {
        checkpoint_dir_ = checkpoint_dir;
        return *this;
    }
    inline AnalyzerConfigManager& set_checkpoint_size(
        std::size_t checkpoint_size) {
        checkpoint_size_ = checkpoint_size;
        return *this;
    }
    inline AnalyzerConfigManager& set_time_resolution(double time_resolution) {
        time_resolution_ = time_resolution;
        return *this;
    }

   private:
    double time_granularity_;
    bool checkpoint_;
    std::string checkpoint_dir_;
    std::size_t checkpoint_size_;
    double time_resolution_;
};

}  // namespace dftracer::utils::analyzers

#endif  // DFTRACER_UTILS_ANALYZERS_CONFIGURATION_MANAGER_H
