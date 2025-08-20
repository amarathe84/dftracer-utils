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

  // Check if we need distributed checkpointing
  bool is_distributed = (ctx.get_size() > 1);
  
  return restore_view<std::vector<HighLevelMetrics>>(
    checkpoint_name,
    [this, &ctx, &trace_paths, &view_types, &checkpoint_name, is_distributed]() -> std::vector<HighLevelMetrics> {
      // Use byte-level work distribution
      WorkDistribution work_dist = distribute_work_by_bytes(
          trace_paths, checkpoint_size_, ctx.get_size(), ctx.get_rank());
      
      if (work_dist.file_work.empty()) {
        spdlog::info("Rank {} has no work assigned", ctx.get_rank());
        return {};
      }
      
      spdlog::info("Rank {} processing {} work units", ctx.get_rank(), work_dist.file_work.size());

      // Debug: log each work unit for this rank
      for (size_t i = 0; i < work_dist.file_work.size(); ++i) {
        const auto& work = work_dist.file_work[i];
        spdlog::info("Rank {} work unit {}: path={}, bytes=[{}, {}), process_full_file={}", 
                     ctx.get_rank(), i, work.path, work.start_offset, work.end_offset, work.process_full_file);
      }

      // Build HLM groupby columns
      std::vector<std::string> hlm_groupby = view_types;
      hlm_groupby.insert(hlm_groupby.end(), HLM_EXTRA_COLS.begin(),
                         HLM_EXTRA_COLS.end());

      auto hlm_pipeline =
          make_pipeline<FileWorkInfo>().map<std::vector<TraceRecord>>(
              [this, &view_types](const FileWorkInfo& work_info) {
                spdlog::info("Pipeline processing work unit: path={}, bytes=[{}, {}), process_full_file={}", 
                             work_info.path, work_info.start_offset, work_info.end_offset, work_info.process_full_file);
                auto traces = this->read_trace(work_info, view_types);
                return this->postread_trace(traces, view_types);
              });

      auto start = std::chrono::high_resolution_clock::now();
      auto all_batches = hlm_pipeline.run(ctx, work_dist.file_work);
      auto elapsed = std::chrono::high_resolution_clock::now() - start;

      spdlog::info(
          "Rank {} pipeline execution completed in {}ms",
          ctx.get_rank(),
          std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());

      auto rank_metrics = _compute_high_level_metrics(all_batches, view_types);
      
      spdlog::info("Rank {} computed {} metrics, checkpoint_={}, ctx.get_size()={}", 
                   ctx.get_rank(), rank_metrics.size(), checkpoint_, ctx.get_size());
      
      // Store this rank's results in distributed checkpoint
      if (checkpoint_) {
        spdlog::info("Rank {} calling store_distributed_view with {} metrics", 
                     ctx.get_rank(), rank_metrics.size());
        store_distributed_view(checkpoint_name, rank_metrics, view_types, ctx.get_rank());
      }
      
      // Gather all results if we're in distributed mode
      if (ctx.get_size() > 1) {
        // In MPI mode, rank 0 loads and aggregates all distributed parts
        if (ctx.get_rank() == 0) {
          // Wait for all ranks to finish their work
#if DFTRACER_UTILS_MPI_ENABLE
          MPI_Barrier(MPI_COMM_WORLD);
#endif
          return load_distributed_view(checkpoint_name);
        } else {
          // Non-zero ranks return empty (only rank 0 has final result)
#if DFTRACER_UTILS_MPI_ENABLE
          MPI_Barrier(MPI_COMM_WORLD);
#endif
          return {};
        }
      }
      
      return rank_metrics;
    },
    false, false, false, view_types, is_distributed
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
                                bool write_to_disk, bool read_from_disk, const std::vector<std::string>& view_types,
                                bool is_distributed) {
  if (checkpoint_) {
    std::string view_path = get_checkpoint_path(checkpoint_name);
    if (force || !has_checkpoint(checkpoint_name)) {
      T view = fallback();
      if (!write_to_disk) {
        return view;
      }
      // Note: In distributed mode, the fallback function handles store_distributed_view,
      // so we don't call store_view here to avoid overwriting distributed checkpoints
      if (!read_from_disk) {
        return view;
      }
      // If read_from_disk is true, read back from the stored checkpoint
    }
    // Read from checkpoint
    if constexpr (std::is_same_v<T, std::vector<HighLevelMetrics>>) {
      if (is_distributed) {
        return load_distributed_view(checkpoint_name);
      } else {
        return load_view_from_parquet(view_path);
      }
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
