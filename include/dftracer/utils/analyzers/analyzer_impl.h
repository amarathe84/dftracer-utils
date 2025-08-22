#ifndef __DFTRACER_UTILS_ANALYZERS_ANALYZER_IMPL_H__
#define __DFTRACER_UTILS_ANALYZERS_ANALYZER_IMPL_H__

#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/json.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <map>

namespace dftracer {
namespace utils {
namespace analyzers {
using namespace dftracer::utils::json;
using namespace dftracer::utils::pipeline;

namespace helpers {

struct WorkInfo {
  std::string path;
  size_t start;
  size_t end;

  template <typename Archive>
  void serialize(Archive& ar) {
    ar(path, start, end);
  }
};

struct FileMetadata {
  std::string path;
  size_t size;

  template <typename Archive>
  void serialize(Archive& ar) {
    ar(path, size);
  }
};

namespace trace_reader {

// Pipeline stage 1: Get file metadata
inline auto get_traces_metadata(const std::vector<std::string>& traces) {
  return from_sequence(traces).map([](const std::string& path) {
    dftracer::utils::indexer::Indexer indexer(path, path + ".idx");
    indexer.build();
    auto max_bytes = indexer.get_max_bytes();
    spdlog::debug("Processing file: {} ({} bytes)", path, max_bytes);
    return FileMetadata{path, max_bytes};
  });
}

// Pipeline stage 2: Generate work chunks
inline auto generate_chunks(const std::vector<std::string>& traces,
                            size_t batch_size) {
  return get_traces_metadata(traces).flatmap(
      [batch_size](const FileMetadata& file_info) {
        std::vector<WorkInfo> work_items;
        size_t start = 0;
        size_t end = 0;

        while (start < file_info.size) {
          end = std::min(start + batch_size, file_info.size);
          work_items.push_back({file_info.path, start, end});
          start = end;
        }

        return work_items;
      });
}

// Pipeline stage 3: Read and parse JSON from chunks
inline auto load_traces(const std::vector<std::string>& traces,
                        size_t batch_size) {
  return generate_chunks(traces, batch_size)
      .repartition("32MB")  // Repartition for better load balancing
      .map_partitions([](const auto& partition) -> OwnedJsonDocuments {
        OwnedJsonDocuments results;
        results.reserve(partition.size() *
                        100);  // Estimate ~100 records per chunk

        spdlog::debug("Processing partition with {} work items on thread: {}",
                      partition.size(),
                      std::hash<std::thread::id>{}(std::this_thread::get_id()));

        for (const auto& work : partition) {
          try {
            dftracer::utils::reader::Reader reader(work.path,
                                                   work.path + ".idx");
            auto lines =
                reader.read_json_lines_bytes_owned(work.start, work.end);

            spdlog::trace("Read {} JSON documents from {}:{}-{}", lines.size(),
                          work.path, work.start, work.end);

            results.insert(results.end(),
                           std::make_move_iterator(lines.begin()),
                           std::make_move_iterator(lines.end()));
          } catch (const std::exception& e) {
            spdlog::error("Error reading {}: {}", work.path, e.what());
          }
        }

        spdlog::debug("Partition complete: {} total documents", results.size());
        return results;
      });
}

// Pipeline stage 4: Parse JSON to TraceRecords with filtering
inline auto parse_and_filter_traces(const std::vector<std::string>& traces,
                                    size_t batch_size,
                                    const std::vector<std::string>& view_types,
                                    double time_granularity) {
  return trace_reader::load_traces(traces, batch_size)
      .repartition("64MB")  // Repartition after JSON parsing for better memory
                            // distribution
      .map_partitions([view_types, time_granularity](
                          const auto& partition) -> std::vector<TraceRecord> {
        std::vector<TraceRecord> valid_records;
        valid_records.reserve(partition.size());

        size_t filtered_count = 0;

        for (const auto& doc : partition) {
          try {
            auto record = parse_trace_record(doc);

            // check if optional
            if (!record) {
              filtered_count++;
              continue;
            }

            // Filter out invalid/empty records
            if (!record->func_name.empty() && !record->cat.empty()) {
              valid_records.push_back(std::move(*record));
            } else {
              filtered_count++;
            }
          } catch (const std::exception& e) {
            spdlog::debug("Failed to parse trace record: {}", e.what());
            filtered_count++;
          }
        }

        if (filtered_count > 0) {
          spdlog::debug("Filtered out {} invalid records from partition",
                        filtered_count);
        }

        spdlog::trace("Parsed {} valid trace records", valid_records.size());
        return valid_records;
      });
}

// Hash resolution stage - separate by event type and collect hash mappings
template <typename BagType>
inline auto separate_events_and_hashes(BagType&& trace_records) {
  return std::forward<BagType>(trace_records)
      .map_partitions([](const std::vector<TraceRecord>& partition)
                          -> std::vector<TraceRecord> {
        std::vector<TraceRecord> result;
        result.reserve(partition.size());

        // Collect hash mappings (global across all records)
        std::unordered_map<std::string, std::string> file_hash_map;
        std::unordered_map<std::string, std::string> host_hash_map;

        // First pass: collect all hash mappings
        for (const auto& record : partition) {
          if (record.event_type == 1 && !record.fhash.empty()) {  // file hash
            file_hash_map[record.fhash] = record.func_name;
          } else if (record.event_type == 2 &&
                     !record.hhash.empty()) {  // host hash
            host_hash_map[record.hhash] = record.func_name;
          }
        }

        // Second pass: resolve hashes for regular events
        for (auto record : partition) {
          if (record.event_type == 0) {  // regular event only
            // Resolve file hash to file name
            if (!record.fhash.empty()) {
              auto it = file_hash_map.find(record.fhash);
              if (it != file_hash_map.end()) {
                record.view_fields["file_name"] = it->second;
              }
            }

            // Resolve host hash to hostname
            if (!record.hhash.empty()) {
              auto it = host_hash_map.find(record.hhash);
              if (it != host_hash_map.end()) {
                record.view_fields["host_name"] = it->second;
              }
            }

            // Set proc_name
            std::string host_name = record.view_fields["host_name"];
            if (host_name.empty()) host_name = "unknown";
            record.view_fields["proc_name"] = "app#" + host_name + "#" +
                                              std::to_string(record.pid) + "#" +
                                              std::to_string(record.tid);

            // Category enrichment based on file_name
            std::string file_name = record.view_fields["file_name"];
            if (!file_name.empty() &&
                (record.cat == "posix" || record.cat == "stdio")) {
              if (file_name.find("/checkpoint") != std::string::npos) {
                record.cat = record.cat + "_checkpoint";
              } else if (file_name.find("/data") != std::string::npos) {
                record.cat = record.cat + "_reader";
              } else if (file_name.find("/lustre") != std::string::npos) {
                record.cat = record.cat + "_lustre";
              } else if (file_name.find("/ssd") != std::string::npos) {
                record.cat = record.cat + "_ssd";
              }
            }

            // Filter ignored file patterns
            bool should_ignore_file = false;
            if (!file_name.empty()) {
              for (const auto& pattern : constants::IGNORED_FILE_PATTERNS) {
                if (file_name.find(pattern) != std::string::npos) {
                  should_ignore_file = true;
                  break;
                }
              }
            }

            if (!should_ignore_file) {
              result.push_back(record);
            }
          }
        }

        spdlog::debug("Processed {} regular events from {} total records",
                      result.size(), partition.size());

        return result;
      });
}
}  // namespace trace_reader

// Complete pipeline orchestration with DFTracer-specific processing
inline auto read_traces(const std::vector<std::string>& traces,
                        size_t batch_size,
                        const std::vector<std::string>& view_types,
                        double time_granularity) {
  spdlog::info("DFTracer loading {} trace files", traces.size());

  // Parse all events (including metadata)
  auto all_events = trace_reader::parse_and_filter_traces(
      traces, batch_size, view_types, time_granularity);

  // Separate events, resolve hashes, apply category enrichment and filtering
  auto normalized_events = trace_reader::separate_events_and_hashes(all_events);

  // Return normalized events
  return normalized_events;
}

// Timestamp normalization stage - find global minimum and normalize
template <typename Context, typename BagType>
inline auto normalize_timestamps_globally(Context& ctx, BagType&& trace_records,
                                          double time_resolution,
                                          double time_granularity) {
  // First pass: find global minimum timestamp
  auto global_min_timestamp =
      trace_records
          .map([](const TraceRecord& record) -> uint64_t {
            return record.time_start;
          })
          .reduce(ctx, [](uint64_t a, uint64_t b) -> uint64_t {
            return std::min(a, b);
          });

  // Second pass: normalize timestamps and recalculate time_range using global
  // minimum
  return trace_records.map(
      [global_min_timestamp, time_resolution,
       time_granularity](TraceRecord record) -> TraceRecord {
        // Normalize timestamps using global minimum
        record.time_start = record.time_start - global_min_timestamp;
        record.time_end =
            record.time_start + static_cast<uint64_t>(record.duration);
        // Scale duration by time_resolution
        record.duration = record.duration / time_resolution;
        record.time_range =
            record.time_start / static_cast<uint64_t>(time_granularity);
        return record;
      });
}

template <typename Context, typename BagType>
inline auto postread_trace(Context& ctx, BagType&& events,
                           const std::vector<std::string>& view_types,
                           double time_granularity) {
  struct EpochSpanEntry {
    uint64_t epoch_num;
    uint64_t start_time;
    uint64_t end_time;
    uint64_t duration;
  };

  // PHASE 1: epoch spans computation
  auto epoch_spans_bag =
      events
          .flatmap([](const TraceRecord& record) -> std::vector<TraceRecord> {
            if (constants::ai_dftracer::is_epoch_event(record.cat,
                                                       record.func_name)) {
              return {record};
            }
            return {};
          })
          .map([time_granularity](const TraceRecord& record) -> EpochSpanEntry {
            uint64_t start_time_range = record.time_range;
            uint64_t end_time_range =
                helpers::calc_time_range(record.time_end, time_granularity);
            uint64_t duration = end_time_range - start_time_range;
            return {record.epoch, start_time_range, end_time_range, duration};
          })
          .distributed_groupby(
              [](const EpochSpanEntry& entry) -> uint64_t {
                return entry.epoch_num;
              },
              [](uint64_t epoch_num, const std::vector<EpochSpanEntry>& entries)
                  -> std::pair<uint64_t, std::pair<uint64_t, uint64_t>> {
                auto max_entry = *std::max_element(
                    entries.begin(), entries.end(),
                    [](const EpochSpanEntry& a, const EpochSpanEntry& b) {
                      return a.duration < b.duration;
                    });
                return {epoch_num, {max_entry.start_time, max_entry.end_time}};
              });

  auto epoch_spans_vector = epoch_spans_bag.compute(ctx);
  std::map<uint64_t, std::pair<uint64_t, uint64_t>> epoch_spans(
      epoch_spans_vector.begin(), epoch_spans_vector.end());

  // PHASE 2: epoch event assignment
  return std::forward<BagType>(events).map_partitions(
      [epoch_spans, view_types](const std::vector<TraceRecord>& partition)
          -> std::vector<TraceRecord> {
        // check if view_types contain epoch, if not just return events
        if (std::find(view_types.begin(), view_types.end(), "epoch") ==
            view_types.end()) {
          spdlog::info(
              "No epoch view type detected, skipping epoch processing");
          return partition;
        }

        std::vector<TraceRecord> result;
        result.reserve(partition.size());

        for (auto record : partition) {
          uint64_t assigned_epoch = 0;
          for (const auto& [epoch_num, span] : epoch_spans) {
            auto [start, end] = span;
            if (record.time_range >= start && record.time_range <= end) {
              assigned_epoch = epoch_num;
              break;
            }
          }
          if (assigned_epoch != 0) {
            record.epoch = assigned_epoch;
            result.push_back(record);
          }
        }
        return result;
      });
}

template <typename BagType>
inline auto compute_high_level_metrics(
    BagType&& trace_records, const std::vector<std::string>& view_types,
    const std::string& partition_size = "128MB") {
  // Create unified groupby columns (view_types + HLM_EXTRA_COLS)
  std::unordered_set<std::string> hlm_groupby_set(view_types.begin(),
                                                  view_types.end());
  hlm_groupby_set.insert(constants::HLM_EXTRA_COLS.begin(),
                         constants::HLM_EXTRA_COLS.end());
  std::vector<std::string> hlm_groupby(hlm_groupby_set.begin(),
                                       hlm_groupby_set.end());

  // Get view_types_diff for unique_set aggregation
  std::vector<std::string> view_types_diff;
  for (const auto& vt : constants::VIEW_TYPES) {
    if (hlm_groupby_set.find(vt) == hlm_groupby_set.end()) {
      view_types_diff.push_back(vt);
    }
  }

  spdlog::info("HLM groupby columns: {}", hlm_groupby);
  spdlog::info("View types for unique_set: {}", view_types_diff);

  // Use distributed groupby for large datasets - this scales with data size
  return std::forward<BagType>(trace_records)
      .distributed_groupby(
          // Key function
          [hlm_groupby](const TraceRecord& record) -> std::string {
            std::ostringstream key_stream;
            bool first = true;

            for (const auto& col : hlm_groupby) {
              if (!first) key_stream << "|";
              first = false;

              if (col == "cat") {
                key_stream << record.cat;
              } else if (col == "io_cat") {
                key_stream << constants::get_io_cat(record.func_name);
              } else if (col == "acc_pat") {
                key_stream << record.acc_pat;
              } else if (col == "func_name") {
                key_stream << record.func_name;
              } else if (col == "time_range") {
                key_stream << record.time_range;
              } else if (col == "epoch") {
                key_stream << record.epoch;
              } else {
                auto it = record.view_fields.find(col);
                if (it != record.view_fields.end()) {
                  key_stream << it->second;
                } else {
                  key_stream << "";
                }
              }
            }

            std::string key = key_stream.str();
            static std::unordered_set<std::string> logged_keys;
            static size_t record_count = 0;
            record_count++;

            if (logged_keys.size() < 2000 &&
                logged_keys.find(key) == logged_keys.end()) {
              spdlog::debug(
                  "RAY DEBUG: Generated groupby key '{}' for record: cat={}, "
                  "io_cat={}, acc_pat={}, func_name={}, time_range={}, "
                  "epoch={}, proc_name={}",
                  key, record.cat, record.io_cat, record.acc_pat,
                  record.func_name, record.time_range, record.epoch,
                  record.view_fields.count("proc_name")
                      ? record.view_fields.at("proc_name")
                      : "MISSING");
              logged_keys.insert(key);
            }

            return key;
          },
          // Aggregation
          [hlm_groupby_set, view_types_diff](
              const std::string&,
              const std::vector<TraceRecord>& records) -> HighLevelMetrics {
            HighLevelMetrics hlm;

            // Apply HLM_AGG aggregations (sum for duration, count, size)
            for (const auto& record : records) {
              hlm.time_sum += record.duration;
              hlm.count_sum += record.count;

              // Handle optional size aggregation (like Python's NaN handling)
              if (record.size.has_value()) {
                if (!hlm.size_sum.has_value()) {
                  hlm.size_sum = 0;
                }
                hlm.size_sum = hlm.size_sum.value() + record.size.value();
              }

              // Sum bin fields (matches: hlm_agg.update({col: sum for col in
              // bin_cols}))
              for (const auto& [bin_field, value] : record.bin_fields) {
                if (value.has_value()) {
                  if (!hlm.bin_sums[bin_field].has_value()) {
                    hlm.bin_sums[bin_field] = 0;
                  }
                  hlm.bin_sums[bin_field] =
                      hlm.bin_sums[bin_field].value() + value.value();
                }
                // If value is nullopt, keep bin_sums[bin_field] as nullopt
                // (don't initialize)
              }
            }

            // Store group values (first() equivalent)
            if (!records.empty()) {
              const auto& first_record = records[0];
              hlm.group_values["cat"] = first_record.cat;
              hlm.group_values["io_cat"] = std::to_string(static_cast<uint64_t>(
                  constants::get_io_cat(first_record.func_name)));
              hlm.group_values["acc_pat"] = first_record.acc_pat;
              hlm.group_values["func_name"] = first_record.func_name;
              hlm.group_values["time_range"] =
                  std::to_string(first_record.time_range);
              hlm.group_values["epoch"] = std::to_string(first_record.epoch);

              for (const auto& [field, value] : first_record.view_fields) {
                hlm.group_values[field] = value;
              }
            }

            // Apply unique_set() for view_types_diff
            for (const auto& col : view_types_diff) {
              for (const auto& record : records) {
                auto it = record.view_fields.find(col);
                if (it != record.view_fields.end()) {
                  hlm.unique_sets[col].insert(it->second);
                }
              }
            }

            return hlm;
          },
          0  // Auto-determine number of partitions based on data size
          )
      // Repartition after aggregation for final processing
      .repartition(partition_size);
}
}  // namespace helpers

template <typename Context>
AnalyzerResult Analyzer::analyze_trace(
    Context& ctx, const std::vector<std::string>& traces,
    const std::vector<std::string>& view_types,
    const std::vector<std::string>& exclude_characteristics,
    const std::unordered_map<std::string, std::string>& extra_columns) {
  spdlog::info(
      "=== RAY DEBUG: Starting full pipeline analysis of {} trace files ===",
      traces.size());

  try {
    // Ensure proc_name is included in view_types
    std::vector<std::string> proc_view_types = view_types;
    if (std::find(proc_view_types.begin(), proc_view_types.end(),
                  constants::COL_PROC_NAME) == proc_view_types.end()) {
      proc_view_types.push_back(constants::COL_PROC_NAME);
    }
    std::sort(proc_view_types.begin(), proc_view_types.end());

    // Step 1: Get trace events
    auto events = helpers::read_traces(traces, checkpoint_size_,
                                       proc_view_types, time_granularity_);

    // Step 2: Apply global timestamp normalization
    auto normalized_events = helpers::normalize_timestamps_globally(
        ctx, events, constants::DEFAULT_TIME_RESOLUTION, time_granularity_);

    // Step 3: Post-process events
    auto post_processed_events = helpers::postread_trace(
        ctx, normalized_events, proc_view_types, time_granularity_);

    // Step 4: Compute high-level metrics on epoch-processed events
    auto hlm_pipeline = helpers::compute_high_level_metrics(
        post_processed_events, proc_view_types, "128MB");

    spdlog::info("Computing high-level metrics...");
    auto hlms = hlm_pipeline.flatmap([](const auto& hlms) { return hlms; })
                    .compute(ctx);

    spdlog::info("HLM computation complete: {} groups generated", hlms.size());

    // Log some statistics
    if (!hlms.empty()) {
      size_t total_count = 0;
      double total_time = 0.0;
      size_t total_size = 0;

      for (const auto& hlm : hlms) {
        total_count += hlm.count_sum;
        total_time += hlm.time_sum;
        if (hlm.size_sum.has_value()) {
          total_size += hlm.size_sum.value();
        }
      }

      spdlog::info("Analysis summary:");
      spdlog::info("  Total operations: {}", total_count);
      spdlog::info("  Total time: {:.2f}", total_time);
      spdlog::info("  Total size: {} bytes", total_size);
      spdlog::info("  Unique groups: {}", hlms.size());
      std::cout << helpers::hlms_to_csv(hlms);
    }

    return AnalyzerResult{std::move(hlms)};
  } catch (const std::exception& e) {
    spdlog::error("Pipeline execution failed: {}", e.what());
    throw;
  }
}

}  // namespace analyzers
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_ANALYZERS_ANALYZER_IMPL_H__
