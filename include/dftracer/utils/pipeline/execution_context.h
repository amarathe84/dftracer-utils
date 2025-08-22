#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_H

#include <vector>
#include <utility>

#include <dftracer/utils/pipeline/pipeline.h>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {
enum class ExecutionStrategy { Sequential, Threaded, MPI };

template <typename Derived>
class ExecutionContext {
 public:
  virtual ~ExecutionContext() = default;
  virtual ExecutionStrategy strategy() const = 0;

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

class SequentialContext : public ExecutionContext<SequentialContext> {
 public:
  ExecutionStrategy strategy() const override {
    return ExecutionStrategy::Sequential;
  }

  template <typename T, typename MapFunc>
  auto execute_map_impl(MapFunc&& func, const std::vector<T>& input) const
      -> std::vector<map_result_t<MapFunc, T>> {
    std::vector<map_result_t<MapFunc, T>> result;
    result.reserve(input.size());

    for (const auto& item : input) {
      result.push_back(func(item));
    }
    return result;
  }

  template <typename T, typename MapPartitionsFunc>
  auto execute_map_partitions_impl(MapPartitionsFunc&& func,
                                   const std::vector<T>& input) const {
    using partition_result_t = decltype(func(std::vector<T>{}));
    using element_t = typename partition_result_t::value_type;

    size_t partition_size = std::max(input.size() / 4, size_t(1));
    std::vector<element_t> final_result;

    for (size_t i = 0; i < input.size(); i += partition_size) {
      size_t end = std::min(i + partition_size, input.size());
      std::vector<T> partition(
          input.begin() + static_cast<std::ptrdiff_t>(i),
          input.begin() + static_cast<std::ptrdiff_t>(end));

      auto partition_result = func(partition);
      final_result.insert(final_result.end(), partition_result.begin(),
                          partition_result.end());
    }

    return final_result;
  }

  template <typename T, typename MapPartitionsFunc>
  auto execute_repartitioned_map_partitions_impl(
      const std::vector<std::vector<T>>& partitions,
      MapPartitionsFunc&& func) const {
    using partition_result_t = decltype(func(std::declval<std::vector<T>>()));
    using element_t = typename partition_result_t::value_type;

    std::vector<element_t> final_result;

    for (const auto& partition : partitions) {
      auto partition_result = func(partition);
      final_result.insert(final_result.end(), partition_result.begin(),
                          partition_result.end());
    }

    return final_result;
  }

  template <typename T, typename ReduceFunc>
  auto execute_reduce_impl(ReduceFunc&& func, const std::vector<T>& input) const
      -> std::vector<T> {
    if (input.empty()) return {};

    T result = input[0];
    for (size_t i = 1; i < input.size(); ++i) {
      result = func(result, input[i]);
    }
    return {result};
  }

  template <typename T>
  auto execute_repartition_impl(const std::vector<T>& input,
                                size_t num_partitions) const
      -> std::vector<std::vector<T>> {
    if (num_partitions == 0 || input.empty()) {
      return {};
    }

    std::vector<std::vector<T>> partitions(num_partitions);
    size_t partition_size =
        (input.size() + num_partitions - 1) / num_partitions;

    for (size_t i = 0; i < input.size(); ++i) {
      size_t partition_idx = i / partition_size;
      if (partition_idx >= num_partitions) partition_idx = num_partitions - 1;
      partitions[partition_idx].push_back(input[i]);
    }
    return partitions;
  }

  template <typename T>
  auto execute_repartition_by_bytes_impl(const std::vector<T>& input,
                                         size_t target_bytes,
                                         bool estimate = true) const
      -> std::vector<std::vector<T>> {
    if (target_bytes == 0) {
      throw std::invalid_argument("Target bytes cannot be zero");
    }

    if (input.empty()) {
      return {};
    }

    if (estimate) {
      size_t estimated_element_size = estimate_element_size(input);
      if (estimated_element_size == 0) {
        estimated_element_size = 1;
      }

      size_t elements_per_partition =
          std::max(target_bytes / estimated_element_size, size_t(1));
      size_t num_partitions =
          (input.size() + elements_per_partition - 1) / elements_per_partition;

      return execute_repartition_impl(input, num_partitions);
    } else {
      std::vector<std::vector<T>> partitions;
      std::vector<T> current_partition;
      size_t current_bytes = 0;

      for (const auto& item : input) {
        size_t item_size = get_actual_size(item);

        if (current_bytes + item_size > target_bytes &&
            !current_partition.empty()) {
          partitions.push_back(std::move(current_partition));
          current_partition.clear();
          current_bytes = 0;
        }

        current_partition.push_back(item);
        current_bytes += item_size;
      }

      if (!current_partition.empty()) {
        partitions.push_back(std::move(current_partition));
      }

      return partitions;
    }
  }

  template <typename T, typename HashFunc>
  auto execute_repartition_by_hash_impl(const std::vector<T>& input,
                                        size_t num_partitions,
                                        HashFunc&& hash_func) const
      -> std::vector<std::vector<T>> {
    if (num_partitions == 0 || input.empty()) {
      return {};
    }

    std::vector<std::vector<T>> partitions(num_partitions);

    for (const auto& item : input) {
      size_t hash_value = hash_func(item);
      size_t partition_idx = hash_value % num_partitions;
      partitions[partition_idx].push_back(item);
    }
    return partitions;
  }

  template <typename T, typename KeyFunc>
  auto execute_groupby_impl(const std::vector<T>& input,
                            KeyFunc&& key_func) const {
    using key_type = decltype(key_func(std::declval<T>()));
    std::unordered_map<key_type, std::vector<T>> groups;

    for (const auto& item : input) {
      auto key = key_func(item);
      groups[key].push_back(item);
    }

    return groups;
  }

  template <typename T, typename KeyFunc, typename AggFunc>
  auto execute_distributed_groupby_impl(const std::vector<T>& input,
                                        KeyFunc&& key_func, AggFunc&& agg_func,
                                        size_t num_partitions) const {
    if (num_partitions == 0) {
      num_partitions = std::max(size_t(1), input.size() / 1000);
    }

    using key_type = decltype(key_func(std::declval<T>()));
    using agg_result_type = decltype(agg_func(key_type{}, std::vector<T>{}));

    std::vector<std::vector<T>> hash_partitions(num_partitions);
    std::hash<key_type> hasher;

    for (const auto& item : input) {
      auto key = key_func(item);
      size_t hash_value = hasher(key);
      size_t partition_idx = hash_value % num_partitions;
      hash_partitions[partition_idx].push_back(item);
    }

    std::vector<agg_result_type> partition_results;

    for (const auto& partition : hash_partitions) {
      std::unordered_map<key_type, std::vector<T>> local_groups;

      for (const auto& item : partition) {
        auto key = key_func(item);
        local_groups[key].push_back(item);
      }

      for (const auto& [key, group] : local_groups) {
        auto agg_result = agg_func(key, group);
        partition_results.push_back(std::move(agg_result));
      }
    }

    return partition_results;
  }

 private:
  template <typename T, typename = void>
  struct has_size_method : std::false_type {};

  template <typename T>
  struct has_size_method<T, std::void_t<decltype(std::declval<T>().size())>>
      : std::true_type {};

  template <typename T>
  typename std::enable_if_t<std::is_arithmetic_v<T>, size_t>
  estimate_element_size_impl(const std::vector<T>& input) const {
    return sizeof(T);
  }

  template <typename T>
  typename std::enable_if_t<
      has_size_method<T>::value && !std::is_arithmetic_v<T>, size_t>
  estimate_element_size_impl(const std::vector<T>& input) const {
    if (input.empty()) return 32;
    size_t total_size = 0;
    size_t sample_size = std::min(input.size(), size_t(100));
    for (size_t i = 0; i < sample_size; ++i) {
      total_size += input[i].size();
    }
    return total_size / sample_size;
  }

  template <typename T>
  typename std::enable_if_t<
      !std::is_arithmetic_v<T> && !has_size_method<T>::value, size_t>
  estimate_element_size_impl(const std::vector<T>&) const {
    return sizeof(T);
  }

  template <typename T>
  size_t estimate_element_size(const std::vector<T>& input) const {
    if (input.empty()) return sizeof(T);

    return estimate_element_size_impl(input);
  }

  template <typename T>
  typename std::enable_if_t<std::is_arithmetic_v<T>, size_t>
  get_actual_size_impl(const T& item) const {
    return sizeof(T);
  }

  template <typename T>
  typename std::enable_if_t<
      has_size_method<T>::value && !std::is_arithmetic_v<T>, size_t>
  get_actual_size_impl(const T& item) const {
    return item.size();
  }

  template <typename T>
  typename std::enable_if_t<
      !std::is_arithmetic_v<T> && !has_size_method<T>::value, size_t>
  get_actual_size_impl(const T&) const {
    return sizeof(T);
  }

  template <typename T>
  size_t get_actual_size(const T& item) const {
    return get_actual_size_impl(item);
  }
};

class ThreadedContext : public ExecutionContext<ThreadedContext> {
 public:
  ThreadedContext(size_t num_threads = std::thread::hardware_concurrency())
      : num_threads_(num_threads) {}

  ExecutionStrategy strategy() const override {
    return ExecutionStrategy::Threaded;
  }
  size_t num_threads() const { return num_threads_; }

  template <typename T, typename MapFunc>
  auto execute_map_impl(MapFunc&& func, const std::vector<T>& input) const
      -> std::vector<map_result_t<MapFunc, T>> {
    std::vector<map_result_t<MapFunc, T>> result(input.size());

    size_t chunk_size = (input.size() + num_threads_ - 1) / num_threads_;
    std::vector<std::future<void>> futures;

    for (size_t t = 0; t < num_threads_; ++t) {
      size_t start = t * chunk_size;
      size_t end = std::min(start + chunk_size, input.size());

      if (start < end) {
        futures.emplace_back(std::async(std::launch::async,
                                        [&input, &result, &func, start, end]() {
                                          for (size_t i = start; i < end; ++i) {
                                            result[i] = func(input[i]);
                                          }
                                        }));
      }
    }

    for (auto& future : futures) {
      future.wait();
    }

    return result;
  }

  template <typename T, typename MapPartitionsFunc>
  auto execute_map_partitions_impl(MapPartitionsFunc&& func,
                                   const std::vector<T>& input) const {
    using partition_result_t = decltype(func(std::vector<T>{}));
    using element_t = typename partition_result_t::value_type;

    size_t partition_size = (input.size() + num_threads_ - 1) / num_threads_;
    std::vector<std::future<partition_result_t>> futures;

    for (size_t t = 0; t < num_threads_; ++t) {
      size_t start = t * partition_size;
      size_t end = std::min(start + partition_size, input.size());

      if (start < end) {
        futures.emplace_back(std::async(std::launch::async, [&input, &func,
                                                             start, end]() {
          std::vector<T> partition(input.begin() +  static_cast<std::ptrdiff_t>(start), input.begin() +  static_cast<std::ptrdiff_t>(end));
          return func(partition);
        }));
      }
    }

    std::vector<element_t> final_result;
    for (auto& future : futures) {
      auto partition_result = future.get();
      final_result.insert(final_result.end(), partition_result.begin(),
                          partition_result.end());
    }

    return final_result;
  }

  template <typename T, typename MapPartitionsFunc>
  auto execute_repartitioned_map_partitions_impl(
      const std::vector<std::vector<T>>& partitions,
      MapPartitionsFunc&& func) const {
    using partition_result_t = decltype(func(std::declval<std::vector<T>>()));
    using element_t = typename partition_result_t::value_type;

    std::vector<std::future<partition_result_t>> futures;

    for (const auto& partition : partitions) {
      futures.emplace_back(
          std::async(std::launch::async,
                     [&partition, &func]() { return func(partition); }));
    }

    std::vector<element_t> final_result;
    for (auto& future : futures) {
      auto partition_result = future.get();
      final_result.insert(final_result.end(), partition_result.begin(),
                          partition_result.end());
    }

    return final_result;
  }

  template <typename T, typename ReduceFunc>
  auto execute_reduce_impl(ReduceFunc&& func, const std::vector<T>& input) const
      -> std::vector<T> {
    if (input.empty()) return {};

    size_t chunk_size = (input.size() + num_threads_ - 1) / num_threads_;
    std::vector<std::future<T>> futures;

    for (size_t t = 0; t < num_threads_; ++t) {
      size_t start = t * chunk_size;
      size_t end = std::min(start + chunk_size, input.size());

      if (start < end) {
        futures.emplace_back(
            std::async(std::launch::async, [&input, &func, start, end]() -> T {
              T local_result = input[start];
              for (size_t i = start + 1; i < end; ++i) {
                local_result = func(local_result, input[i]);
              }
              return local_result;
            }));
      }
    }

    T result = futures[0].get();
    for (size_t i = 1; i < futures.size(); ++i) {
      result = func(result, futures[i].get());
    }

    return {result};
  }

  template <typename T>
  auto execute_repartition_impl(const std::vector<T>& input,
                                size_t num_partitions) const
      -> std::vector<std::vector<T>> {
    if (num_partitions == 0 || input.empty()) {
      return {};
    }

    std::vector<std::vector<T>> partitions(num_partitions);

    for (size_t i = 0; i < input.size(); ++i) {
      partitions[i % num_partitions].push_back(input[i]);
    }
    return partitions;
  }

  template <typename T>
  auto execute_repartition_by_bytes_impl(const std::vector<T>& input,
                                         size_t target_bytes,
                                         bool estimate = true) const
      -> std::vector<std::vector<T>> {
    SequentialContext seq_ctx;
    return seq_ctx.execute_repartition_by_bytes_impl(input, target_bytes,
                                                     estimate);
  }

  template <typename T, typename HashFunc>
  auto execute_repartition_by_hash_impl(const std::vector<T>& input,
                                        size_t num_partitions,
                                        HashFunc&& hash_func) const
      -> std::vector<std::vector<T>> {
    if (num_partitions == 0 || input.empty()) {
      return {};
    }

    std::vector<std::vector<T>> partitions(num_partitions);
    std::vector<std::mutex> partition_mutexes(num_partitions);
    size_t chunk_size = (input.size() + num_threads_ - 1) / num_threads_;
    std::vector<std::future<void>> futures;

    for (size_t t = 0; t < num_threads_; ++t) {
      size_t start = t * chunk_size;
      size_t end = std::min(start + chunk_size, input.size());

      if (start < end) {
        futures.emplace_back(std::async(
            std::launch::async, [&input, &partitions, &partition_mutexes,
                                 &hash_func, num_partitions, start, end]() {
              std::vector<std::vector<T>> local_partitions(num_partitions);

              for (size_t i = start; i < end; ++i) {
                size_t hash_value = hash_func(input[i]);
                size_t partition_idx = hash_value % num_partitions;
                local_partitions[partition_idx].push_back(input[i]);
              }

              for (size_t p = 0; p < num_partitions; ++p) {
                if (!local_partitions[p].empty()) {
                  std::lock_guard<std::mutex> lock(partition_mutexes[p]);
                  auto& global_partition = partitions[p];
                  global_partition.insert(global_partition.end(),
                                          local_partitions[p].begin(),
                                          local_partitions[p].end());
                }
              }
            }));
      }
    }

    for (auto& future : futures) {
      future.wait();
    }

    return partitions;
  }

  template <typename T, typename KeyFunc>
  auto execute_groupby_impl(const std::vector<T>& input,
                            KeyFunc&& key_func) const {
    using key_type = decltype(key_func(std::declval<T>()));

    size_t chunk_size = (input.size() + num_threads_ - 1) / num_threads_;
    std::vector<std::future<std::unordered_map<key_type, std::vector<T>>>>
        futures;

    for (size_t t = 0; t < num_threads_; ++t) {
      size_t start = t * chunk_size;
      size_t end = std::min(start + chunk_size, input.size());

      if (start < end) {
        futures.emplace_back(
            std::async(std::launch::async, [&input, &key_func, start, end]() {
              std::unordered_map<key_type, std::vector<T>> local_groups;
              for (size_t i = start; i < end; ++i) {
                auto key = key_func(input[i]);
                local_groups[key].push_back(input[i]);
              }
              return local_groups;
            }));
      }
    }

    std::unordered_map<key_type, std::vector<T>> final_groups;
    for (auto& future : futures) {
      auto local_groups = future.get();
      for (auto& kv : local_groups) {
        auto& key = kv.first;
        auto& values = kv.second;
        auto& target = final_groups[key];
        target.insert(target.end(), values.begin(), values.end());
      }
    }

    return final_groups;
  }

  template <typename T, typename KeyFunc, typename AggFunc>
  auto execute_distributed_groupby_impl(const std::vector<T>& input,
                                        KeyFunc&& key_func, AggFunc&& agg_func,
                                        size_t num_partitions) const {
    if (num_partitions == 0) {
        num_partitions = std::max(num_threads_, input.size() / 1000);
    }

    using key_type = decltype(key_func(std::declval<T>()));
    using agg_result_type = decltype(agg_func(key_type{}, std::vector<T>{}));

    std::vector<std::vector<T>> hash_partitions(num_partitions);
    std::vector<std::mutex> partition_mutexes(num_partitions);

    size_t chunk_size = (input.size() + num_threads_ - 1) / num_threads_;
    std::vector<std::future<void>> futures;

    for (size_t t = 0; t < num_threads_; ++t) {
        size_t start = t * chunk_size;
        size_t end = std::min(start + chunk_size, input.size());

        if (start < end) {
            futures.emplace_back(std::async(
                std::launch::async,
                [&input, &hash_partitions, &partition_mutexes,
                 key_func,  // Capture by value
                 num_partitions, start, end]() {
                    std::hash<key_type> local_hasher;  // Create local instance
                    std::vector<std::vector<T>> local_partitions(num_partitions);

                    for (size_t i = start; i < end; ++i) {
                        auto key = key_func(input[i]);
                        size_t hash_value = local_hasher(key);
                        size_t partition_idx = hash_value % num_partitions;
                        local_partitions[partition_idx].push_back(input[i]);
                    }

                    for (size_t p = 0; p < num_partitions; ++p) {
                        if (!local_partitions[p].empty()) {
                            std::lock_guard<std::mutex> lock(partition_mutexes[p]);
                            auto& global_partition = hash_partitions[p];
                            global_partition.insert(global_partition.end(),
                                                   local_partitions[p].begin(),
                                                   local_partitions[p].end());
                        }
                    }
                }));
        }
    }

    for (auto& future : futures) {
      future.wait();
    }

    std::vector<std::future<std::vector<agg_result_type>>> partition_futures;

    for (const auto& partition : hash_partitions) {
      partition_futures.emplace_back(std::async(
          std::launch::async,
          [&partition, &key_func, &agg_func]() -> std::vector<agg_result_type> {
            std::unordered_map<key_type, std::vector<T>> local_groups;

            for (const auto& item : partition) {
              auto key = key_func(item);
              local_groups[key].push_back(item);
            }

            std::vector<agg_result_type> partition_results;
            for (const auto& [key, group] : local_groups) {
              auto agg_result = agg_func(key, group);
              partition_results.push_back(std::move(agg_result));
            }

            return partition_results;
          }));
    }

    std::vector<agg_result_type> final_results;
    for (auto& future : partition_futures) {
      auto partition_results = future.get();
      final_results.insert(final_results.end(), partition_results.begin(),
                           partition_results.end());
    }

    return final_results;
  }

 private:
  size_t num_threads_;
};
}
}
}
}

#endif // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_H
