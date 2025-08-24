#ifndef DFTRACER_UTILS_ANALYZERS_ANALYZER_H
#define DFTRACER_UTILS_ANALYZERS_ANALYZER_H

#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/bag.h>
#include <dftracer/utils/utils/json.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace dftracer::utils::analyzers {

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

class AnalyzerConfig {
 public:
  AnalyzerConfig(double time_granularity = constants::DEFAULT_TIME_GRANULARITY,
                 bool checkpoint = false,
                 const std::string& checkpoint_dir = "",
                 size_t checkpoint_size =
                     dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
                 double time_resolution = constants::DEFAULT_TIME_RESOLUTION);

  static AnalyzerConfig Default();
  static AnalyzerConfig create(
      double time_granularity = constants::DEFAULT_TIME_GRANULARITY,
      bool checkpoint = false, const std::string& checkpoint_dir = "",
      size_t checkpoint_size =
          dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
      double time_resolution = constants::DEFAULT_TIME_RESOLUTION);

  AnalyzerConfig(const AnalyzerConfig&) = default;
  AnalyzerConfig& operator=(const AnalyzerConfig&) = default;
  AnalyzerConfig(AnalyzerConfig&&) = default;
  AnalyzerConfig& operator=(AnalyzerConfig&&) = default;

  // Getter
  double time_granularity() const;
  bool checkpoint() const;
  const std::string& checkpoint_dir() const;
  size_t checkpoint_size() const;
  double time_resolution() const;

  // Setter
  AnalyzerConfig& set_time_granularity(double time_granularity);
  AnalyzerConfig& set_checkpoint(bool checkpoint);
  AnalyzerConfig& set_checkpoint_dir(const std::string& checkpoint_dir);
  AnalyzerConfig& set_checkpoint_size(size_t checkpoint_size);
  AnalyzerConfig& set_time_resolution(double time_resolution);

 private:
  double time_granularity_;
  bool checkpoint_;
  std::string checkpoint_dir_;
  size_t checkpoint_size_;
  double time_resolution_;
};

class Analyzer {
 public:
  Analyzer(double time_granularity = constants::DEFAULT_TIME_GRANULARITY,
           bool checkpoint = false, const std::string& checkpoint_dir = "",
           size_t checkpoint_size =
               dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
           double time_resolution = constants::DEFAULT_TIME_RESOLUTION);
  Analyzer(const AnalyzerConfig& config);

  Analyzer(const Analyzer&) = delete;
  Analyzer& operator=(const Analyzer&) = delete;
  Analyzer(Analyzer&&) = default;
  Analyzer& operator=(Analyzer&&) = default;
  ~Analyzer() = default;

  inline const AnalyzerConfig& config() const { return config_; }

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
  AnalyzerConfig config_;
};

}  // namespace dftracer::utils::analyzers

#include <dftracer/utils/analyzers/analyzer_impl.h>

#endif  // DFTRACER_UTILS_ANALYZERS_ANALYZER_H
