#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_MPI_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_MPI_H

#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/execution_context/sequential.h>
#include <dftracer/utils/pipeline/internal.h>
#include <mpi.h>
#include <spdlog/spdlog.h>

#include <cereal/archives/binary.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/unordered_map.hpp>
#include <cereal/types/utility.hpp>
#include <cereal/types/vector.hpp>
#include <numeric>
#include <sstream>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {

using namespace internal;

class MPIContext : public ExecutionContext<MPIContext> {
 public:
  MPIContext(MPI_Comm comm = MPI_COMM_WORLD) : comm_(comm) {
    MPI_Comm_rank(comm_, &rank_);
    MPI_Comm_size(comm_, &size_);
  }

  ExecutionStrategy strategy() const override { return ExecutionStrategy::MPI; }

  virtual size_t rank() const override { return static_cast<size_t>(rank_); }
  virtual size_t size() const override { return static_cast<size_t>(size_); }
  MPI_Comm comm() const { return comm_; }

  template <typename T>
  std::vector<T> collect(const std::vector<T>& local_data) const {
    return gather_all_data(local_data);
  }

  template <typename T, typename MapFunc>
  auto execute_map_impl(MapFunc&& func, const std::vector<T>& input) const
      -> std::vector<map_result_t<MapFunc, T>> {
    spdlog::debug("Rank {} map: {} items → processing locally", rank_,
                  input.size());

    std::vector<map_result_t<MapFunc, T>> local_result;
    local_result.reserve(input.size());

    for (const auto& item : input) {
      local_result.push_back(func(item));
    }

    spdlog::debug("Rank {} map: {} items → {} results (staying distributed)",
                  rank_, input.size(), local_result.size());
    return local_result;
  }

  template <typename T, typename MapPartitionsFunc>
  auto execute_map_partitions_impl(MapPartitionsFunc&& func,
                                   const std::vector<T>& input) const {
    spdlog::debug(
        "Rank {} map_partitions: {} items → processing as single partition",
        rank_, input.size());

    auto local_result = func(input);

    spdlog::debug(
        "Rank {} map_partitions: partition → {} results (staying distributed)",
        rank_, local_result.size());
    return local_result;
  }

  template <typename T, typename MapPartitionsFunc>
  auto execute_repartitioned_map_partitions_impl(
      const std::vector<std::vector<T>>& partitions,
      MapPartitionsFunc&& func) const {
    using partition_result_t = decltype(func(std::declval<std::vector<T>>()));
    using element_t = typename partition_result_t::value_type;

    std::vector<element_t> final_result;
    size_t partitions_per_process = (partitions.size() + size_ - 1) / size_;
    size_t start_idx = rank_ * partitions_per_process;
    size_t end_idx =
        std::min(start_idx + partitions_per_process, partitions.size());

    spdlog::debug("Rank {} processing partitions {}-{} of {}", rank_, start_idx,
                  end_idx, partitions.size());

    for (size_t i = start_idx; i < end_idx; ++i) {
      auto partition_result = func(partitions[i]);
      final_result.insert(final_result.end(), partition_result.begin(),
                          partition_result.end());
    }

    spdlog::debug(
        "Rank {} repartitioned_map_partitions: {} results (staying "
        "distributed)",
        rank_, final_result.size());
    return final_result;
  }

  template <typename T, typename ReduceFunc>
  auto execute_reduce_impl(ReduceFunc&& func, const std::vector<T>& input) const
      -> std::vector<T> {
    spdlog::debug("Rank {} reduce: {} distributed items → global reduction",
                  rank_, input.size());

    if (input.empty()) {
      spdlog::debug("Rank {} has empty input, participating in collective",
                    rank_);
      return participate_in_reduce_with_empty_data<T, ReduceFunc>(func);
    }

    T local_result = input[0];
    for (size_t i = 1; i < input.size(); ++i) {
      local_result = func(local_result, input[i]);
    }

    spdlog::debug("Rank {} completed local reduction", rank_);

    auto final_result = global_reduce(local_result, func);

    spdlog::debug("Rank {} reduce: global result replicated to all processes",
                  rank_);
    return {final_result};
  }

  template <typename T>
  auto execute_repartition_impl(const std::vector<T>& input,
                                size_t num_partitions) const
      -> std::vector<std::vector<T>> {
    if (num_partitions == 0 || input.empty()) {
      return {};
    }

    auto all_data = gather_all_data(input);

    if (rank_ == 0) {
      std::vector<std::vector<T>> partitions(num_partitions);
      size_t partition_size =
          (all_data.size() + num_partitions - 1) / num_partitions;

      for (size_t i = 0; i < all_data.size(); ++i) {
        size_t partition_idx = i / partition_size;
        if (partition_idx >= num_partitions) partition_idx = num_partitions - 1;
        partitions[partition_idx].push_back(all_data[i]);
      }

      broadcast_partitions(partitions);
      return partitions;
    } else {
      return receive_partitions<T>();
    }
  }

  template <typename T>
  auto execute_repartition_by_bytes_impl(const std::vector<T>& input,
                                         size_t target_bytes,
                                         bool estimate = true) const
      -> std::vector<std::vector<T>> {
    SequentialContext seq_ctx;
    auto all_data = gather_all_data(input);

    if (rank_ == 0) {
      auto partitions = seq_ctx.execute_repartition_by_bytes_impl(
          all_data, target_bytes, estimate);
      broadcast_partitions(partitions);
      return partitions;
    } else {
      return receive_partitions<T>();
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

    std::vector<std::vector<T>> local_partitions(num_partitions);

    for (const auto& item : input) {
      size_t hash_value = hash_func(item);
      size_t partition_idx = hash_value % num_partitions;
      local_partitions[partition_idx].push_back(item);
    }

    return exchange_hash_partitions(local_partitions);
  }

  template <typename T, typename KeyFunc>
  auto execute_groupby_impl(const std::vector<T>& input,
                            KeyFunc&& key_func) const {
    using key_type = decltype(key_func(std::declval<T>()));

    std::unordered_map<key_type, std::vector<T>> local_groups;
    for (const auto& item : input) {
      auto key = key_func(item);
      local_groups[key].push_back(item);
    }

    return exchange_groups(local_groups);
  }

  template <typename T, typename KeyFunc, typename AggFunc>
  auto execute_distributed_groupby_impl(const std::vector<T>& input,
                                        KeyFunc&& key_func, AggFunc&& agg_func,
                                        size_t num_partitions) const {
    if (num_partitions == 0) {
      num_partitions = std::max(size_t(size_), input.size() / 1000);
    }

    using key_type = decltype(key_func(std::declval<T>()));
    using agg_result_type = decltype(agg_func(key_type{}, std::vector<T>{}));

    spdlog::debug("Rank {} distributed_groupby: {} items → hash shuffle", rank_,
                  input.size());
    std::vector<std::vector<T>> hash_partitions(size_);
    std::hash<key_type> hasher;

    for (const auto& item : input) {
      auto key = key_func(item);
      size_t hash_value = hasher(key);
      size_t target_process = hash_value % size_;
      hash_partitions[target_process].push_back(item);
    }

    auto my_data = exchange_for_groupby(hash_partitions);

    spdlog::debug("Rank {} received {} items after shuffle", rank_,
                  my_data.size());

    std::unordered_map<key_type, std::vector<T>> local_groups;
    for (const auto& item : my_data) {
      auto key = key_func(item);
      local_groups[key].push_back(item);
    }

    std::vector<agg_result_type> local_results;
    for (const auto& [key, group] : local_groups) {
      auto agg_result = agg_func(key, group);
      local_results.push_back(std::move(agg_result));
    }

    spdlog::debug(
        "Rank {} distributed_groupby: {} groups (staying distributed)", rank_,
        local_results.size());
    return local_results;
  }

  template <typename T>
  std::vector<char> serialize(const T& data) const {
    std::ostringstream ss;
    {
      cereal::BinaryOutputArchive archive(ss);
      archive(data);
    }
    std::string str = ss.str();
    return std::vector<char>(str.begin(), str.end());
  }

  template <typename T>
  T deserialize(const std::vector<char>& data) const {
    std::string str(data.begin(), data.end());
    std::istringstream ss(str);
    cereal::BinaryInputArchive archive(ss);
    T result;
    archive(result);
    return result;
  }

 private:
  MPI_Comm comm_;
  int rank_;
  int size_;

  template <typename T, typename ReduceFunc>
  T global_reduce(const T& local_result, ReduceFunc&& func) const {
    auto serialized_local = serialize(local_result);
    size_t local_size = serialized_local.size();

    std::vector<size_t> all_sizes(size_);
    MPI_Allgather(&local_size, sizeof(size_t), MPI_BYTE, all_sizes.data(),
                  sizeof(size_t), MPI_BYTE, comm_);

    std::vector<int> displacements(size_);
    displacements[0] = 0;
    for (int i = 1; i < size_; ++i) {
      displacements[i] =
          displacements[i - 1] + static_cast<int>(all_sizes[i - 1]);
    }

    size_t total_size =
        std::accumulate(all_sizes.begin(), all_sizes.end(), 0UL);
    std::vector<char> all_serialized(total_size);
    std::vector<int> int_sizes(all_sizes.begin(), all_sizes.end());

    MPI_Allgatherv(serialized_local.data(), static_cast<int>(local_size),
                   MPI_BYTE, all_serialized.data(), int_sizes.data(),
                   displacements.data(), MPI_BYTE, comm_);

    std::vector<T> all_local_results;
    size_t offset = 0;
    for (int i = 0; i < size_; ++i) {
      if (all_sizes[i] > 0) {
        std::vector<char> proc_data(
            all_serialized.begin() + offset,
            all_serialized.begin() + offset + all_sizes[i]);
        T proc_result = deserialize<T>(proc_data);
        all_local_results.push_back(proc_result);
        offset += all_sizes[i];
      }
    }

    T final_result = all_local_results[0];
    for (size_t i = 1; i < all_local_results.size(); ++i) {
      final_result = func(final_result, all_local_results[i]);
    }

    return final_result;
  }

  template <typename T, typename ReduceFunc>
  std::vector<T> participate_in_reduce_with_empty_data(
      ReduceFunc&& func) const {
    size_t local_size = 0;
    std::vector<size_t> all_sizes(size_);
    MPI_Allgather(&local_size, sizeof(size_t), MPI_BYTE, all_sizes.data(),
                  sizeof(size_t), MPI_BYTE, comm_);

    bool any_has_data = std::any_of(all_sizes.begin(), all_sizes.end(),
                                    [](size_t s) { return s > 0; });

    if (!any_has_data) {
      return {};
    }

    std::vector<int> displacements(size_);
    displacements[0] = 0;
    for (int i = 1; i < size_; ++i) {
      displacements[i] =
          displacements[i - 1] + static_cast<int>(all_sizes[i - 1]);
    }

    size_t total_size =
        std::accumulate(all_sizes.begin(), all_sizes.end(), 0UL);
    std::vector<char> all_serialized(total_size);
    std::vector<int> int_sizes(all_sizes.begin(), all_sizes.end());

    std::vector<char> empty_data;
    MPI_Allgatherv(empty_data.data(), 0, MPI_BYTE, all_serialized.data(),
                   int_sizes.data(), displacements.data(), MPI_BYTE, comm_);

    std::vector<T> valid_results;
    size_t offset = 0;
    for (int i = 0; i < size_; ++i) {
      if (all_sizes[i] > 0) {
        std::vector<char> proc_data(
            all_serialized.begin() + offset,
            all_serialized.begin() + offset + all_sizes[i]);
        T proc_result = deserialize<T>(proc_data);
        valid_results.push_back(proc_result);
      }
      offset += all_sizes[i];
    }

    if (valid_results.empty()) {
      return {};
    }

    T final_result = valid_results[0];
    for (size_t i = 1; i < valid_results.size(); ++i) {
      final_result = func(final_result, valid_results[i]);
    }

    return {final_result};
  }

  template <typename T>
  std::vector<T> gather_all_data(const std::vector<T>& local_data) const {
    size_t local_size = local_data.size();
    std::vector<size_t> all_sizes(size_);
    MPI_Allgather(&local_size, sizeof(size_t), MPI_BYTE, all_sizes.data(),
                  sizeof(size_t), MPI_BYTE, comm_);

    auto local_serialized = serialize(local_data);
    size_t local_serialized_size = local_serialized.size();

    std::vector<size_t> serialized_sizes(size_);
    MPI_Allgather(&local_serialized_size, sizeof(size_t), MPI_BYTE,
                  serialized_sizes.data(), sizeof(size_t), MPI_BYTE, comm_);

    std::vector<int> displacements(size_);
    displacements[0] = 0;
    for (int i = 1; i < size_; ++i) {
      displacements[i] =
          displacements[i - 1] + static_cast<int>(serialized_sizes[i - 1]);
    }

    size_t total_serialized_size =
        std::accumulate(serialized_sizes.begin(), serialized_sizes.end(), 0UL);
    std::vector<char> all_serialized(total_serialized_size);
    std::vector<int> int_sizes(serialized_sizes.begin(),
                               serialized_sizes.end());

    MPI_Allgatherv(local_serialized.data(),
                   static_cast<int>(local_serialized_size), MPI_BYTE,
                   all_serialized.data(), int_sizes.data(),
                   displacements.data(), MPI_BYTE, comm_);

    std::vector<T> final_result;
    size_t offset = 0;
    for (int i = 0; i < size_; ++i) {
      if (serialized_sizes[i] > 0) {
        std::vector<char> proc_data(
            all_serialized.begin() + offset,
            all_serialized.begin() + offset + serialized_sizes[i]);
        auto proc_result = deserialize<std::vector<T>>(proc_data);
        final_result.insert(final_result.end(), proc_result.begin(),
                            proc_result.end());
        offset += serialized_sizes[i];
      }
    }

    return final_result;
  }

  template <typename T>
  void broadcast_partitions(
      const std::vector<std::vector<T>>& partitions) const {
    auto serialized = serialize(partitions);
    size_t size = serialized.size();
    MPI_Bcast(&size, sizeof(size_t), MPI_BYTE, 0, comm_);
    MPI_Bcast(serialized.data(), static_cast<int>(size), MPI_BYTE, 0, comm_);
  }

  template <typename T>
  std::vector<std::vector<T>> receive_partitions() const {
    size_t size;
    MPI_Bcast(&size, sizeof(size_t), MPI_BYTE, 0, comm_);

    std::vector<char> serialized(size);
    MPI_Bcast(serialized.data(), static_cast<int>(size), MPI_BYTE, 0, comm_);

    return deserialize<std::vector<std::vector<T>>>(serialized);
  }

  template <typename T>
  std::vector<std::vector<T>> exchange_hash_partitions(
      const std::vector<std::vector<T>>& local_partitions) const {
    std::vector<std::vector<T>> result_partitions(local_partitions.size());

    for (size_t partition_idx = 0; partition_idx < local_partitions.size();
         ++partition_idx) {
      auto gathered = gather_all_data(local_partitions[partition_idx]);
      if (rank_ == 0) {
        auto serialized = serialize(gathered);
        size_t size = serialized.size();
        MPI_Bcast(&size, sizeof(size_t), MPI_BYTE, 0, comm_);
        MPI_Bcast(serialized.data(), static_cast<int>(size), MPI_BYTE, 0,
                  comm_);
        result_partitions[partition_idx] = gathered;
      } else {
        size_t size;
        MPI_Bcast(&size, sizeof(size_t), MPI_BYTE, 0, comm_);
        std::vector<char> serialized(size);
        MPI_Bcast(serialized.data(), static_cast<int>(size), MPI_BYTE, 0,
                  comm_);
        result_partitions[partition_idx] =
            deserialize<std::vector<T>>(serialized);
      }
    }

    return result_partitions;
  }

  template <typename T, typename KeyType>
  std::unordered_map<KeyType, std::vector<T>> exchange_groups(
      const std::unordered_map<KeyType, std::vector<T>>& local_groups) const {
    auto serialized_groups = serialize(local_groups);
    auto all_serialized_groups =
        gather_all_data(std::vector<std::vector<char>>{serialized_groups});

    std::unordered_map<KeyType, std::vector<T>> final_groups;
    for (const auto& serialized : all_serialized_groups) {
      auto groups =
          deserialize<std::unordered_map<KeyType, std::vector<T>>>(serialized);
      for (const auto& [key, values] : groups) {
        auto& target = final_groups[key];
        target.insert(target.end(), values.begin(), values.end());
      }
    }

    return final_groups;
  }

  template <typename T>
  std::vector<T> exchange_for_groupby(
      const std::vector<std::vector<T>>& hash_partitions) const {
    spdlog::debug("Rank {} starting exchange_for_groupby", rank_);

    std::vector<T> my_data;

    std::vector<std::vector<char>> serialized_partitions(size_);
    std::vector<int> send_counts(size_);
    std::vector<int> send_displacements(size_);

    size_t total_send_size = 0;
    for (int i = 0; i < size_; ++i) {
      serialized_partitions[i] = serialize(hash_partitions[i]);
      send_counts[i] = static_cast<int>(serialized_partitions[i].size());
      send_displacements[i] = static_cast<int>(total_send_size);
      total_send_size += serialized_partitions[i].size();
    }

    std::vector<char> send_buffer(total_send_size);
    size_t offset = 0;
    for (int i = 0; i < size_; ++i) {
      std::copy(serialized_partitions[i].begin(),
                serialized_partitions[i].end(), send_buffer.begin() + offset);
      offset += serialized_partitions[i].size();
    }

    std::vector<int> recv_counts(size_);
    MPI_Alltoall(send_counts.data(), 1, MPI_INT, recv_counts.data(), 1, MPI_INT,
                 comm_);

    std::vector<int> recv_displacements(size_);
    recv_displacements[0] = 0;
    for (int i = 1; i < size_; ++i) {
      recv_displacements[i] = recv_displacements[i - 1] + recv_counts[i - 1];
    }

    size_t total_recv_size =
        std::accumulate(recv_counts.begin(), recv_counts.end(), 0);
    std::vector<char> recv_buffer(total_recv_size);

    MPI_Alltoallv(send_buffer.data(), send_counts.data(),
                  send_displacements.data(), MPI_BYTE, recv_buffer.data(),
                  recv_counts.data(), recv_displacements.data(), MPI_BYTE,
                  comm_);

    spdlog::debug("Rank {} completed MPI_Alltoallv", rank_);

    for (int i = 0; i < size_; ++i) {
      if (recv_counts[i] > 0) {
        std::vector<char> proc_data(
            recv_buffer.begin() + recv_displacements[i],
            recv_buffer.begin() + recv_displacements[i] + recv_counts[i]);
        auto received_partition = deserialize<std::vector<T>>(proc_data);
        my_data.insert(my_data.end(), received_partition.begin(),
                       received_partition.end());
      }
    }

    spdlog::debug("Rank {} completed exchange_for_groupby with {} items", rank_,
                  my_data.size());
    return my_data;
  }
};

}  // namespace context
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_MPI_H
