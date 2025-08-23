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

struct TraceRecord;
struct HighLevelMetrics;

struct TraceRecord {
  std::string cat;
  std::string io_cat;
  std::string acc_pat;
  std::string func_name;
  double duration;
  uint64_t count;
  uint64_t time_range;
  uint64_t time_start;
  uint64_t time_end;
  uint64_t epoch;
  uint64_t pid;
  uint64_t tid;
  std::string fhash;
  std::string hhash;
  uint64_t image_id;
  uint8_t event_type;  // 0=regular, 1=file_hash, 2=host_hash, 3=string_hash,
                       // 4=other_metadata
  std::optional<uint64_t> size;
  std::optional<uint64_t> offset;
  std::unordered_map<std::string, std::string> view_fields;
  std::unordered_map<std::string, std::optional<uint32_t>> bin_fields;

  template <class Archive>
  void serialize(Archive& ar) {
    ar(cat, io_cat, acc_pat, func_name, duration, count, size, time_range,
       time_start, time_end, epoch, pid, tid, fhash, hhash, image_id, offset,
       event_type, view_fields, bin_fields);
  }
};

struct HashEntry {
  std::string name;
  std::string hash;
  uint64_t pid;
  uint64_t tid;
  std::string hhash;

  template <class Archive>
  void serialize(Archive& ar) {
    ar(name, hash, pid, tid, hhash);
  }
};

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

  template <typename T, typename FallbackFunc>
  T restore_view(const std::string& checkpoint_name, FallbackFunc fallback,
                 bool force = false, bool write_to_disk = true,
                 bool read_from_disk = false,
                 const std::vector<std::string>& view_types = {});

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
