#ifndef __DFTRACER_UTILS_ANALYZERS_ANALYZER_H__
#define __DFTRACER_UTILS_ANALYZERS_ANALYZER_H__

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/pipeline/pipeline.h>

namespace dftracer {
namespace utils {
namespace analyzers {

using namespace dftracer::utils::pipeline;

struct TraceRecord {
  std::string cat;
  std::string io_cat;
  std::string acc_pat;
  std::string func_name;
  double time;
  uint64_t count;
  uint64_t size;
  uint64_t time_range;
  std::unordered_map<std::string, std::string> view_fields;
  std::unordered_map<std::string, uint32_t> bin_fields;


  template<class Archive>
  void serialize(Archive& ar)
  {
      ar(cat, io_cat, acc_pat, func_name, time, 
         count, size, time_range, view_fields, bin_fields);
  }
};

struct HighLevelMetrics {
  double time_sum = 0.0;
  uint64_t count_sum = 0;
  uint64_t size_sum = 0;
  std::unordered_map<std::string, uint32_t> bin_sums;
  std::unordered_map<std::string, std::unordered_set<std::string>> unique_sets;
  std::unordered_map<std::string, std::string> group_values;

  template<class Archive>
  void serialize(Archive& ar)
  {
      ar(time_sum, count_sum, size_sum, bin_sums, unique_sets, group_values);
  }
};

struct AnalyzerResult {
  HighLevelMetrics _hlms;

  template<class Archive>
  void serialize(Archive& ar)
  {
      ar(_hlms);
  }
};

class Analyzer {
public:
    Analyzer(
      double time_granularity = constants::DEFAULT_TIME_GRANULARITY,
      double time_resolution = constants::DEFAULT_TIME_RESOLUTION,
      size_t checkpoint_size =
          dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
      bool checkpoint = false,
      const std::string& checkpoint_dir = "");

    ~Analyzer() = default;

    template<typename Context>
    AnalyzerResult analyze_trace(
        Context& ctx,
        const std::vector<std::string>& traces,
        const std::vector<std::string>& view_types,
        const std::vector<std::string>& exclude_characteristics = {},
        const std::unordered_map<std::string, std::string>& extra_columns = {}
        // @TODO: add extra_columns_fn
    );

    Bag<HighLevelMetrics> compute_high_level_metrics(
        const std::vector<TraceRecord>& records,
        const std::vector<std::string>& view_types,
        const std::string& partition_size = "128MB",
        const std::string& checkpoint_name = ""
    );

    Bag<TraceRecord> read_trace(
        const std::string& trace_path,
        const std::unordered_map<std::string, std::string>& extra_columns = {}
    );

    Bag<TraceRecord> postread_trace(
        const std::vector<TraceRecord>& traces,
        const std::vector<std::string>& view_types
    );
    
    template<typename T, typename FallbackFunc>
    T restore_view(const std::string& checkpoint_name, FallbackFunc fallback,
      bool force = false, bool write_to_disk = true,
      bool read_from_disk = false, const std::vector<std::string>& view_types = {});
      
protected:
    std::string get_checkpoint_name(const std::vector<std::string>& args) const;
    std::string get_checkpoint_path(const std::string& name) const;
    bool has_checkpoint(const std::string& name) const;

    HighLevelMetrics _compute_high_level_metrics(
        const std::vector<TraceRecord>& traces,
        const std::vector<std::string>& view_types,
        const std::string& partition_size
    );

private:
  double time_granularity_;
  double time_resolution_;
  size_t checkpoint_size_;
  std::string checkpoint_dir_;
  bool checkpoint_;
};

} // namespace analyzers
} // namespace utils
} // namespace dftracer

#include <dftracer/utils/analyzers/analyzer_impl.h>

#endif // __DFTRACER_UTILS_ANALYZERS_ANALYZER_H__
