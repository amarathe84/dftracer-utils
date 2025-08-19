#ifndef __DFTRACER_UTILS_ANALYZERS_DFTRACER_IMPL_H
#define __DFTRACER_UTILS_ANALYZERS_DFTRACER_IMPL_H

#include <dftracer/utils/pipeline/pipeline.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <ctime>
#include <fstream>
#include <sstream>
#include <type_traits>

namespace dftracer {
namespace analyzers {

template <typename ExecutionContext>
std::vector<HighLevelMetrics> DFTracerAnalyzer::compute_high_level_metrics(
    ExecutionContext& ctx, const std::vector<std::string>& trace_paths,
    const std::vector<std::string>& view_types) {
  using namespace dftracer::utils::pipeline;

  std::ostringstream view_types_stream;
  for (size_t i = 0; i < view_types.size(); ++i) {
    view_types_stream << view_types[i];
    if (i < view_types.size() - 1) view_types_stream << ", ";
  }
  spdlog::info(
      "Computing high-level metrics for {} trace files with view types: {}",
      trace_paths.size(), view_types_stream.str());

  // Generate checkpoint name similar to Python implementation
  std::vector<std::string> checkpoint_args = {"_hlm"};
  std::vector<std::string> sorted_view_types = view_types;
  std::sort(sorted_view_types.begin(), sorted_view_types.end());
  checkpoint_args.insert(checkpoint_args.end(), sorted_view_types.begin(), sorted_view_types.end());
  std::string checkpoint_name = get_checkpoint_name(checkpoint_args);

  return restore_view<std::vector<HighLevelMetrics>>(
    checkpoint_name,
    [this, &ctx, &trace_paths, &view_types]() -> std::vector<HighLevelMetrics> {
      // Build HLM groupby columns
      std::vector<std::string> hlm_groupby = view_types;
      hlm_groupby.insert(hlm_groupby.end(), HLM_EXTRA_COLS.begin(),
                         HLM_EXTRA_COLS.end());

      auto hlm_pipeline =
          make_pipeline<std::string>().map<std::vector<TraceRecord>>(
              [this, &view_types](const std::string& path) {
                auto traces = this->read_trace(path, view_types);
                return this->postread_trace(traces, view_types);
              });

      auto start = std::chrono::high_resolution_clock::now();
      auto all_batches = hlm_pipeline.run(ctx, trace_paths);
      auto elapsed = std::chrono::high_resolution_clock::now() - start;

      spdlog::info(
          "Pipeline execution completed in {}ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());

      return _compute_high_level_metrics(all_batches, view_types);
    },
    false, true, false, view_types
  );
}

template <typename ExecutionContext>
std::vector<HighLevelMetrics> DFTracerAnalyzer::analyze_trace(
    ExecutionContext& ctx, const std::vector<std::string>& trace_paths,
    const std::vector<std::string>& view_types) {
  spdlog::info("=== Starting DFTracer analysis ===");
  spdlog::info("Configuration:");
  spdlog::info("  Time granularity: {} µs", time_granularity_);
  spdlog::info("  Time resolution: {} µs", time_resolution_);
  spdlog::info("  Checkpoint size: {} MB", checkpoint_size_ / (1024 * 1024));

  for (const auto& trace_path : trace_paths) {
    ensure_index_exists(trace_path, checkpoint_size_, false, 0);
  }

  auto hlm_results = compute_high_level_metrics(ctx, trace_paths, view_types);

  spdlog::info("=== Analysis completed ===");
  return hlm_results;
}

template <typename T, typename FallbackFunc>
T DFTracerAnalyzer::restore_view(const std::string& checkpoint_name, 
                                FallbackFunc fallback, bool force, 
                                bool write_to_disk, bool read_from_disk, const std::vector<std::string>& view_types) {
  if (checkpoint_) {
    std::string view_path = get_checkpoint_path(checkpoint_name);
    if (force || !has_checkpoint(checkpoint_name)) {
      T view = fallback();
      if (!write_to_disk) {
        return view;
      }
      if constexpr (std::is_same_v<T, std::vector<HighLevelMetrics>>) {
        store_view(checkpoint_name, view, view_types);
      }
      if (!read_from_disk) {
        return view;
      }
      // If read_from_disk is true, read back from the stored checkpoint
    }
    // Read from checkpoint
    if constexpr (std::is_same_v<T, std::vector<HighLevelMetrics>>) {
      return load_view_from_parquet(view_path);
    } else {
      // For other types, fallback to computation
      return fallback();
    }
  }
  return fallback();
}

}  // namespace analyzers
}  // namespace dftracer

#endif // __DFTRACER_UTILS_ANALYZERS_DFTRACER_IMPL_H
