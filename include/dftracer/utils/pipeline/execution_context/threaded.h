#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_THREADED_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_THREADED_H

#include <dftracer/utils/pipeline/execution_context/execution_context.h>

#include <algorithm>
#include <cmath>
#include <future>
#include <mutex>
#include <numeric>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace execution_context {

class ThreadedContext : public ExecutionContext<ThreadedContext> {
 private:
  size_t num_threads_;

 public:
  explicit ThreadedContext(size_t num_threads = 0)
      : num_threads_(num_threads == 0 ? std::thread::hardware_concurrency()
                                      : num_threads) {
    if (num_threads_ == 0) {
      num_threads_ = 1;  // Fallback if hardware_concurrency() returns 0
    }
  }

  size_t get_num_threads() const { return num_threads_; }

  template <typename InT, typename OutT, typename MapFn>
  std::vector<OutT> map_impl(MapFn&& fn, const std::vector<InT>& input) const {
    if (input.empty()) return {};

    const size_t chunk_size = std::max(size_t(1), input.size() / num_threads_);
    std::vector<std::future<std::vector<OutT>>> futures;

    for (size_t i = 0; i < input.size(); i += chunk_size) {
      size_t end = std::min(i + chunk_size, input.size());

      futures.emplace_back(
          std::async(std::launch::async, [&fn, &input, i, end]() {
            std::vector<OutT> chunk_result;
            chunk_result.reserve(end - i);
            std::transform(input.begin() + i, input.begin() + end,
                           std::back_inserter(chunk_result), fn);
            return chunk_result;
          }));
    }

    // Collect results
    std::vector<OutT> result;
    result.reserve(input.size());
    for (auto& future : futures) {
      auto chunk_result = future.get();
      result.insert(result.end(), chunk_result.begin(), chunk_result.end());
    }

    return result;
  }

  template <typename InT, typename OutT, typename ReduceFn>
  OutT reduce_impl(ReduceFn&& fn, const std::vector<InT>& input) const {
    // For reduce, we just call the function directly since it operates on the
    // whole vector
    return fn(input);
  }

  template <typename InT, typename ChunkT, typename AggT, typename OutT,
            typename ChunkFn, typename AggFn, typename FinalizeFn>
  OutT aggregate_impl(ChunkFn&& chunk_fn, AggFn&& agg_fn,
                      FinalizeFn&& finalize_fn,
                      const std::vector<InT>& input) const {
    if (input.empty()) {
      // Handle empty input - create empty chunks and aggregate
      std::vector<ChunkT> empty_chunks;
      AggT agg_result = agg_fn(empty_chunks);
      return finalize_fn(agg_result);
    }

    const size_t chunk_size = std::max(size_t(1), input.size() / num_threads_);
    std::vector<std::future<ChunkT>> chunk_futures;

    // Chunk phase - parallel
    for (size_t i = 0; i < input.size(); i += chunk_size) {
      size_t end = std::min(i + chunk_size, input.size());

      chunk_futures.emplace_back(
          std::async(std::launch::async, [&chunk_fn, &input, i, end]() {
            std::vector<InT> chunk(input.begin() + i, input.begin() + end);
            return chunk_fn(chunk);
          }));
    }

    // Collect chunk results
    std::vector<ChunkT> chunks;
    chunks.reserve(chunk_futures.size());
    for (auto& future : chunk_futures) {
      chunks.push_back(future.get());
    }

    // Aggregate phase
    AggT agg_result = agg_fn(chunks);

    // Finalize phase
    return finalize_fn(agg_result);
  }

  template <typename InT, typename KeyT, typename OutT, typename KeyFn,
            typename AggFn>
  std::unordered_map<KeyT, OutT> groupby_aggregate_impl(
      KeyFn&& key_fn, AggFn&& agg_fn, const std::vector<InT>& input) const {
    if (input.empty()) {
      std::unordered_map<KeyT, std::vector<InT>> empty_groups;
      return agg_fn(empty_groups);
    }

    const size_t chunk_size = std::max(size_t(1), input.size() / num_threads_);
    std::vector<std::future<std::unordered_map<KeyT, std::vector<InT>>>>
        futures;

    // Parallel grouping by chunks
    for (size_t i = 0; i < input.size(); i += chunk_size) {
      size_t end = std::min(i + chunk_size, input.size());

      futures.emplace_back(
          std::async(std::launch::async, [&key_fn, &input, i, end]() {
            std::unordered_map<KeyT, std::vector<InT>> local_groups;
            for (size_t j = i; j < end; ++j) {
              KeyT key = key_fn(input[j]);
              local_groups[key].push_back(input[j]);
            }
            return local_groups;
          }));
    }

    // Merge results from all threads
    std::unordered_map<KeyT, std::vector<InT>> merged_groups;
    for (auto& future : futures) {
      auto local_groups = future.get();
      for (auto& [key, values] : local_groups) {
        auto& merged_values = merged_groups[key];
        merged_values.insert(merged_values.end(), values.begin(), values.end());
      }
    }

    // Aggregate each group
    return agg_fn(merged_groups);
  }
};

}  // namespace execution_context
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_THREADED_H