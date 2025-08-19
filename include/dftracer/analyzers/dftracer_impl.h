#ifndef __DFTRACER_UTILS_ANALYZERS_DFTRACER_IMPL_H
#define __DFTRACER_UTILS_ANALYZERS_DFTRACER_IMPL_H

#include <dftracer/utils/pipeline/pipeline.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <sstream>

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

}  // namespace analyzers
}  // namespace dftracer

#endif // __DFTRACER_UTILS_ANALYZERS_DFTRACER_IMPL_H
