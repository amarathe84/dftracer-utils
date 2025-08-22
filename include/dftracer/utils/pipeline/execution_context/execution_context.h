#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_EXECUTION_CONTEXT_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_EXECUTION_CONTEXT_H

#include <vector>
#include <utility>

#include <dftracer/utils/pipeline/internal.h>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {

using namespace internal;

template <typename Derived>
class ExecutionContext {
 public:
  virtual ~ExecutionContext() = default;
  virtual ExecutionStrategy strategy() const = 0;

  virtual int rank() const {
      return 0;
  }

  virtual int size() const {
      return 1;
  }

  template <typename T, typename MapFunc>
  auto execute_map(const std::vector<T>& input, MapFunc&& func) const
      -> std::vector<map_result_t<MapFunc, T>> {
    return static_cast<const Derived*>(this)
        ->template execute_map_impl<T, MapFunc>(std::forward<MapFunc>(func),
                                                input);
  }

  template <typename T, typename MapPartitionsFunc>
  auto execute_map_partitions(const std::vector<T>& input,
                              MapPartitionsFunc&& func) const {
    return static_cast<const Derived*>(this)
        ->template execute_map_partitions_impl<T, MapPartitionsFunc>(
            std::forward<MapPartitionsFunc>(func), input);
  }

  template <typename T, typename MapPartitionsFunc>
  auto execute_repartitioned_map_partitions(
      const std::vector<std::vector<T>>& partitions,
      MapPartitionsFunc&& func) const {
    return static_cast<const Derived*>(this)
        ->template execute_repartitioned_map_partitions_impl<T,
                                                             MapPartitionsFunc>(
            partitions, std::forward<MapPartitionsFunc>(func));
  }

  template <typename T, typename ReduceFunc>
  auto execute_reduce(const std::vector<T>& input, ReduceFunc&& func) const
      -> std::vector<T> {
    return static_cast<const Derived*>(this)
        ->template execute_reduce_impl<T, ReduceFunc>(
            std::forward<ReduceFunc>(func), input);
  }

  template <typename T>
  auto execute_repartition(const std::vector<T>& input,
                           size_t num_partitions) const
      -> std::vector<std::vector<T>> {
    return static_cast<const Derived*>(this)
        ->template execute_repartition_impl<T>(input, num_partitions);
  }

  template <typename T>
  auto execute_repartition_by_bytes(const std::vector<T>& input,
                                    size_t target_bytes,
                                    bool estimate = true) const
      -> std::vector<std::vector<T>> {
    return static_cast<const Derived*>(this)
        ->template execute_repartition_by_bytes_impl<T>(input, target_bytes,
                                                        estimate);
  }

  template <typename T, typename HashFunc>
  auto execute_repartition_by_hash(const std::vector<T>& input,
                                   size_t num_partitions,
                                   HashFunc&& hash_func) const
      -> std::vector<std::vector<T>> {
    return static_cast<const Derived*>(this)
        ->template execute_repartition_by_hash_impl<T, HashFunc>(
            input, num_partitions, std::forward<HashFunc>(hash_func));
  }

  template <typename T, typename KeyFunc>
  auto execute_groupby(const std::vector<T>& input, KeyFunc&& key_func) const {
    return static_cast<const Derived*>(this)
        ->template execute_groupby_impl<T, KeyFunc>(
            input, std::forward<KeyFunc>(key_func));
  }

  template <typename T, typename KeyFunc, typename AggFunc>
  auto execute_distributed_groupby(const std::vector<T>& input,
                                   KeyFunc&& key_func, AggFunc&& agg_func,
                                   size_t num_partitions = 0) const {
    return static_cast<const Derived*>(this)
        ->template execute_distributed_groupby_impl<T, KeyFunc, AggFunc>(
            input, std::forward<KeyFunc>(key_func),
            std::forward<AggFunc>(agg_func), num_partitions);
  }
};
}
}
}
}

#endif // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_EXECUTION_CONTEXT_H
