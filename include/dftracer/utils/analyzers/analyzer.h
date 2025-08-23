#ifndef __DFTRACER_UTILS_ANALYZERS_ANALYZER_H__
#define __DFTRACER_UTILS_ANALYZERS_ANALYZER_H__

#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/bag.h>
#include <dftracer/utils/utils/json.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace dftracer {
namespace utils {
namespace analyzers {

using namespace dftracer::utils::pipeline;

struct HighLevelMetrics {
  double time_sum = 0.0;
  uint64_t count_sum = 0;
  std::optional<uint64_t> size_sum;
  std::unordered_map<std::string, std::optional<uint32_t>> bin_sums;
  std::unordered_map<std::string, std::unordered_set<std::string>> unique_sets;
  std::unordered_map<std::string, std::string> group_values;

  template <class Archive>
  void serialize(Archive& ar) {
    ar(time_sum, count_sum, size_sum, bin_sums, unique_sets, group_values);
  }
};

struct AnalyzerResult {
  std::vector<HighLevelMetrics> _hlms;

  template <class Archive>
  void serialize(Archive& ar) {
    ar(_hlms);
  }
};

class Analyzer {
 public:
  Analyzer(double time_granularity = constants::DEFAULT_TIME_GRANULARITY,
           double time_resolution = constants::DEFAULT_TIME_RESOLUTION,
           size_t checkpoint_size =
               dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
           bool checkpoint = false, const std::string& checkpoint_dir = "");

  ~Analyzer() = default;

  template <typename Context>
  AnalyzerResult analyze_trace(
      Context& ctx, const std::vector<std::string>& traces,
      const std::vector<std::string>& view_types,
      const std::vector<std::string>& exclude_characteristics = {},
      const std::unordered_map<std::string, std::string>& extra_columns = {}
      // @TODO: add extra_columns_fn
  );

 protected:
  std::string get_checkpoint_name(const std::vector<std::string>& args) const;
  std::string get_checkpoint_path(const std::string& name) const;
  bool has_checkpoint(const std::string& name) const;

 private:
  double time_granularity_;
  double time_resolution_;
  size_t checkpoint_size_;
  std::string checkpoint_dir_;
  bool checkpoint_;
};

}  // namespace analyzers
}  // namespace utils
}  // namespace dftracer

#include <dftracer/utils/analyzers/analyzer_impl.h>

#endif  // __DFTRACER_UTILS_ANALYZERS_ANALYZER_H__
