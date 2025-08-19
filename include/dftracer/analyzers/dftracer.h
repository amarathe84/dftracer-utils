#ifndef __DFTRACER_UTILS_ANALYZERS_DFTRACER_H
#define __DFTRACER_UTILS_ANALYZERS_DFTRACER_H

#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/utils/json.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dftracer {
namespace analyzers {

struct HLM_AGG {
  static constexpr const char* TIME = "time";
  static constexpr const char* COUNT = "count";
  static constexpr const char* SIZE = "size";
};

extern const std::vector<std::string> HLM_EXTRA_COLS;
extern const double DEFAULT_TIME_GRANULARITY;

extern const double KiB;
extern const double MiB;
extern const double GiB;
extern const std::vector<double> SIZE_BINS;
extern const std::vector<std::string> SIZE_BIN_SUFFIXES;

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
};

struct HighLevelMetrics {
  double time_sum = 0.0;
  uint64_t count_sum = 0;
  uint64_t size_sum = 0;
  std::unordered_map<std::string, uint32_t> bin_sums;
  std::unordered_map<std::string, std::unordered_set<std::string>> unique_sets;
  std::unordered_map<std::string, std::string> group_values;
};

extern const std::unordered_map<std::string, std::string> POSIX_IO_CAT_MAPPING;
extern const std::unordered_set<std::string> POSIX_METADATA_FUNCTIONS;
extern const std::unordered_set<std::string> IGNORED_FUNC_NAMES;
extern const std::vector<std::string> IGNORED_FUNC_PATTERNS;

// Utility functions
std::string derive_io_cat(const std::string& func_name);
bool should_ignore_event(const std::string& func_name,
                         const std::string& phase);
int get_size_bin_index(uint64_t size);
void set_size_bins(TraceRecord& record);
TraceRecord parse_trace_record(const dftracer::utils::json::JsonDocument& doc,
                               const std::vector<std::string>& view_types,
                               double time_granularity);
std::string create_grouping_key(const TraceRecord& record,
                                const std::vector<std::string>& view_types);
void ensure_index_exists(const std::string& gz_path, size_t checkpoint_size,
                         bool force_rebuild, int mpi_rank = 0);
std::vector<TraceRecord> read_and_parse_traces(
    const std::string& gz_path, const std::vector<std::string>& view_types,
    size_t checkpoint_size, double time_granularity);
std::vector<HighLevelMetrics> replace_zeros_with_nan(
    std::vector<HighLevelMetrics> metrics);

// DFTracer Chrome tracing analyzer class
class DFTracerAnalyzer {
 public:
  DFTracerAnalyzer(
      double time_granularity = DEFAULT_TIME_GRANULARITY,
      double time_resolution = 1e6,
      size_t checkpoint_size =
          dftracer::utils::indexer::Indexer::DEFAULT_CHECKPOINT_SIZE,
      bool checkpoint = false,
      const std::string& checkpoint_dir = "");

  std::vector<TraceRecord> read_trace(
      const std::string& trace_path,
      const std::vector<std::string>& view_types);

  std::vector<TraceRecord> postread_trace(
      const std::vector<TraceRecord>& traces,
      const std::vector<std::string>& view_types);

  template <typename ExecutionContext>
  std::vector<HighLevelMetrics> compute_high_level_metrics(
      ExecutionContext& ctx, const std::vector<std::string>& trace_paths,
      const std::vector<std::string>& view_types);

  template <typename ExecutionContext>
  std::vector<HighLevelMetrics> analyze_trace(
      ExecutionContext& ctx, const std::vector<std::string>& trace_paths,
      const std::vector<std::string>& view_types);

  // Restore view from checkpoint or compute using fallback
  template <typename T, typename FallbackFunc>
  T restore_view(const std::string& checkpoint_name, FallbackFunc fallback,
                 bool force = false, bool write_to_disk = true,
                 bool read_from_disk = false, const std::vector<std::string>& view_types = {});

 private:
  double time_granularity_;
  double time_resolution_;
  size_t checkpoint_size_;
  std::string checkpoint_dir_;
  bool checkpoint_;

  std::vector<HighLevelMetrics> _compute_high_level_metrics(
      const std::vector<std::vector<TraceRecord>>& all_batches,
      const std::vector<std::string>& view_types);

  std::unordered_map<std::string, HighLevelMetrics> aggregate_hlm(
      const std::unordered_map<std::string, std::vector<TraceRecord>>& groups,
      const std::vector<std::string>& view_types);

  // Checkpoint helper functions
  std::string get_checkpoint_name(const std::vector<std::string>& args) const;
  std::string get_checkpoint_path(const std::string& name) const;
  bool has_checkpoint(const std::string& name) const;
  
  // Parquet serialization/deserialization helpers
  void store_view(const std::string& name, const std::vector<HighLevelMetrics>& view, const std::vector<std::string>& view_types);
  std::vector<HighLevelMetrics> load_view_from_parquet(const std::string& path);
};

}  // namespace analyzers
}  // namespace dftracer

// Include template implementations
#include <dftracer/analyzers/dftracer_impl.h>

#endif // __DFTRACER_UTILS_ANALYZERS_DFTRACER_H
