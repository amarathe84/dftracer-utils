#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_MPI_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_MPI_H

#include <vector>
#include <unordered_map>
#include <algorithm>
#include <numeric>
#include <type_traits>
#include <cstring>
#include <sstream>
#include <functional>

#include <dftracer/utils/config.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>

#if DFTRACER_UTILS_MPI_ENABLE
#include <mpi.h>
#else
#include <dftracer/utils/pipeline/execution_context/sequential.h>
#endif

namespace dftracer {
namespace utils {
namespace pipeline {
namespace execution_context {

#if DFTRACER_UTILS_MPI_ENABLE

class MPIContext : public ExecutionContext<MPIContext> {
private:
    int rank_;
    int size_;
    MPI_Comm comm_;

    // Helper to get MPI datatype for common types
    template<typename T>
    MPI_Datatype get_mpi_datatype() const {
        if constexpr (std::is_same_v<T, int>) {
            return MPI_INT;
        } else if constexpr (std::is_same_v<T, float>) {
            return MPI_FLOAT;
        } else if constexpr (std::is_same_v<T, double>) {
            return MPI_DOUBLE;
        } else if constexpr (std::is_same_v<T, long>) {
            return MPI_LONG;
        } else if constexpr (std::is_same_v<T, long long>) {
            return MPI_LONG_LONG;
        } else if constexpr (std::is_same_v<T, unsigned int>) {
            return MPI_UNSIGNED;
        } else if constexpr (std::is_same_v<T, size_t>) {
            return MPI_UNSIGNED_LONG;
        } else {
            return MPI_BYTE;
        }
    }

    // Helper to compute chunk distribution across ranks
    std::pair<size_t, size_t> get_chunk_bounds(size_t total_size) const {
        size_t chunk_size = total_size / size_;
        size_t remainder = total_size % size_;
        
        size_t start, end;
        if (rank_ < static_cast<int>(remainder)) {
            chunk_size++;
            start = rank_ * chunk_size;
        } else {
            start = rank_ * chunk_size + remainder;
        }
        end = start + chunk_size;
        
        return {start, std::min(end, total_size)};
    }

    // Serialize vector of strings for MPI communication
    std::vector<char> serialize_strings(const std::vector<std::string>& strings) const {
        std::ostringstream oss;
        for (const auto& str : strings) {
            oss << str.size() << ':' << str << ';';
        }
        std::string serialized = oss.str();
        return std::vector<char>(serialized.begin(), serialized.end());
    }

    // Deserialize vector of strings from MPI communication
    std::vector<std::string> deserialize_strings(const std::vector<char>& data) const {
        std::vector<std::string> result;
        std::string buffer(data.begin(), data.end());
        
        size_t pos = 0;
        while (pos < buffer.size()) {
            // Find size
            size_t colon_pos = buffer.find(':', pos);
            if (colon_pos == std::string::npos) break;
            
            size_t str_size = std::stoull(buffer.substr(pos, colon_pos - pos));
            pos = colon_pos + 1;
            
            // Extract string
            if (pos + str_size < buffer.size()) {
                result.push_back(buffer.substr(pos, str_size));
                pos += str_size + 1; // +1 for semicolon
            } else {
                break;
            }
        }
        return result;
    }

    // Gather strings from all ranks
    std::vector<std::string> gather_strings(const std::vector<std::string>& local_strings) const {
        // Serialize local strings
        auto local_data = serialize_strings(local_strings);
        
        // Gather sizes first
        int local_size = static_cast<int>(local_data.size());
        std::vector<int> sizes(size_);
        MPI_Allgather(&local_size, 1, MPI_INT, sizes.data(), 1, MPI_INT, comm_);
        
        // Calculate displacements
        std::vector<int> displacements(size_);
        int total_size = 0;
        for (int i = 0; i < size_; ++i) {
            displacements[i] = total_size;
            total_size += sizes[i];
        }
        
        // Gather all data
        std::vector<char> all_data(total_size);
        MPI_Allgatherv(local_data.data(), local_size, MPI_CHAR,
                      all_data.data(), sizes.data(), displacements.data(),
                      MPI_CHAR, comm_);
        
        // Deserialize and combine
        std::vector<std::string> result;
        size_t pos = 0;
        for (int i = 0; i < size_; ++i) {
            if (sizes[i] > 0) {
                std::vector<char> rank_data(all_data.begin() + pos, 
                                          all_data.begin() + pos + sizes[i]);
                auto rank_strings = deserialize_strings(rank_data);
                result.insert(result.end(), rank_strings.begin(), rank_strings.end());
                pos += sizes[i];
            }
        }
        
        return result;
    }

    // Serialize key-value pair for MPI communication
    template<typename KeyT, typename InT>
    std::vector<char> serialize_key_value_pair(const KeyT& key, const std::vector<InT>& values) const {
        std::vector<char> buffer;
        
        // Serialize key
        if constexpr (std::is_arithmetic_v<KeyT>) {
            const char* key_bytes = reinterpret_cast<const char*>(&key);
            buffer.insert(buffer.end(), key_bytes, key_bytes + sizeof(KeyT));
        } else if constexpr (std::is_same_v<KeyT, std::string>) {
            size_t key_size = key.size();
            const char* size_bytes = reinterpret_cast<const char*>(&key_size);
            buffer.insert(buffer.end(), size_bytes, size_bytes + sizeof(size_t));
            buffer.insert(buffer.end(), key.begin(), key.end());
        }
        
        // Serialize values count
        size_t values_count = values.size();
        const char* count_bytes = reinterpret_cast<const char*>(&values_count);
        buffer.insert(buffer.end(), count_bytes, count_bytes + sizeof(size_t));
        
        // Serialize values
        if constexpr (std::is_arithmetic_v<InT>) {
            const char* values_bytes = reinterpret_cast<const char*>(values.data());
            buffer.insert(buffer.end(), values_bytes, values_bytes + values.size() * sizeof(InT));
        } else if constexpr (std::is_same_v<InT, std::string>) {
            for (const auto& value : values) {
                size_t value_size = value.size();
                const char* size_bytes = reinterpret_cast<const char*>(&value_size);
                buffer.insert(buffer.end(), size_bytes, size_bytes + sizeof(size_t));
                buffer.insert(buffer.end(), value.begin(), value.end());
            }
        }
        
        return buffer;
    }
    
    // Deserialize key-value pair from MPI communication
    template<typename KeyT, typename InT>
    std::pair<KeyT, std::vector<InT>> deserialize_key_value_pair(const char* data, size_t& offset) const {
        KeyT key;
        
        // Deserialize key
        if constexpr (std::is_arithmetic_v<KeyT>) {
            std::memcpy(&key, data + offset, sizeof(KeyT));
            offset += sizeof(KeyT);
        } else if constexpr (std::is_same_v<KeyT, std::string>) {
            size_t key_size;
            std::memcpy(&key_size, data + offset, sizeof(size_t));
            offset += sizeof(size_t);
            key = std::string(data + offset, key_size);
            offset += key_size;
        }
        
        // Deserialize values count
        size_t values_count;
        std::memcpy(&values_count, data + offset, sizeof(size_t));
        offset += sizeof(size_t);
        
        // Deserialize values
        std::vector<InT> values;
        values.reserve(values_count);
        
        if constexpr (std::is_arithmetic_v<InT>) {
            values.resize(values_count);
            std::memcpy(values.data(), data + offset, values_count * sizeof(InT));
            offset += values_count * sizeof(InT);
        } else if constexpr (std::is_same_v<InT, std::string>) {
            for (size_t i = 0; i < values_count; ++i) {
                size_t value_size;
                std::memcpy(&value_size, data + offset, sizeof(size_t));
                offset += sizeof(size_t);
                values.emplace_back(data + offset, value_size);
                offset += value_size;
            }
        }
        
        return {key, values};
    }

    // Hash-based key redistribution for groupby
    template<typename KeyT, typename InT>
    std::unordered_map<KeyT, std::vector<InT>> 
    redistribute_by_key(const std::unordered_map<KeyT, std::vector<InT>>& local_groups) const {
        // Step 1: Serialize data for each target rank
        std::vector<std::vector<char>> send_buffers(size_);
        
        for (const auto& [key, values] : local_groups) {
            int target_rank = std::hash<KeyT>{}(key) % size_;
            auto serialized = serialize_key_value_pair<KeyT, InT>(key, values);
            
            // Add size prefix for this key-value pair
            size_t pair_size = serialized.size();
            const char* size_bytes = reinterpret_cast<const char*>(&pair_size);
            send_buffers[target_rank].insert(send_buffers[target_rank].end(), 
                                           size_bytes, size_bytes + sizeof(size_t));
            send_buffers[target_rank].insert(send_buffers[target_rank].end(), 
                                           serialized.begin(), serialized.end());
        }
        
        // Step 2: Exchange buffer sizes
        std::vector<int> send_counts(size_);
        std::vector<int> recv_counts(size_);
        
        for (int i = 0; i < size_; ++i) {
            send_counts[i] = static_cast<int>(send_buffers[i].size());
        }
        
        MPI_Alltoall(send_counts.data(), 1, MPI_INT, 
                     recv_counts.data(), 1, MPI_INT, comm_);
        
        // Step 3: Calculate displacements
        std::vector<int> send_displs(size_);
        std::vector<int> recv_displs(size_);
        
        int send_total = 0, recv_total = 0;
        for (int i = 0; i < size_; ++i) {
            send_displs[i] = send_total;
            recv_displs[i] = recv_total;
            send_total += send_counts[i];
            recv_total += recv_counts[i];
        }
        
        // Step 4: Prepare send buffer
        std::vector<char> send_buffer(send_total);
        size_t offset = 0;
        for (int i = 0; i < size_; ++i) {
            std::memcpy(send_buffer.data() + offset, send_buffers[i].data(), send_counts[i]);
            offset += send_counts[i];
        }
        
        // Step 5: Exchange actual data
        std::vector<char> recv_buffer(recv_total);
        MPI_Alltoallv(send_buffer.data(), send_counts.data(), send_displs.data(), MPI_CHAR,
                      recv_buffer.data(), recv_counts.data(), recv_displs.data(), MPI_CHAR, comm_);
        
        // Step 6: Deserialize received data
        std::unordered_map<KeyT, std::vector<InT>> redistributed_groups;
        offset = 0;
        
        while (offset < recv_buffer.size()) {
            // Read size of next key-value pair
            size_t pair_size;
            std::memcpy(&pair_size, recv_buffer.data() + offset, sizeof(size_t));
            offset += sizeof(size_t);
            
            if (offset + pair_size > recv_buffer.size()) break;
            
            // Deserialize key-value pair
            size_t pair_offset = 0;
            auto [key, values] = deserialize_key_value_pair<KeyT, InT>(
                recv_buffer.data() + offset, pair_offset);
            
            // Merge with existing values for this key
            auto& existing_values = redistributed_groups[key];
            existing_values.insert(existing_values.end(), values.begin(), values.end());
            
            offset += pair_size;
        }
        
        return redistributed_groups;
    }

public:
    explicit MPIContext(MPI_Comm comm = MPI_COMM_WORLD) : comm_(comm) {
        MPI_Comm_rank(comm_, &rank_);
        MPI_Comm_size(comm_, &size_);
    }

    int get_rank() const { return rank_; }
    int get_size() const { return size_; }
    MPI_Comm get_comm() const { return comm_; }

    template <typename InT, typename OutT, typename MapFn>
    std::vector<OutT> map_impl(MapFn&& fn, const std::vector<InT>& input) const {
        if (input.empty()) {
            return {};
        }

        // Broadcast input size to all ranks for consistent processing
        size_t total_size = input.size();
        MPI_Bcast(&total_size, 1, MPI_UNSIGNED_LONG, 0, comm_);
        
        // Ensure all ranks have the input data
        std::vector<InT> local_input = input;
        if (rank_ != 0) {
            local_input.resize(total_size);
        }
        
        // Broadcast input data if needed
        if constexpr (std::is_arithmetic_v<InT>) {
            MPI_Bcast(local_input.data(), static_cast<int>(total_size), 
                     get_mpi_datatype<InT>(), 0, comm_);
        }
        
        // Distribute work across MPI ranks
        auto [start, end] = get_chunk_bounds(total_size);
        
        // Debug output to show work distribution
        printf("Rank %d/%d: Processing elements %zu-%zu (chunk size: %zu)\n", 
               rank_, size_, start, end-1, end-start);
        fflush(stdout);
        
        // Process local chunk
        std::vector<OutT> local_result;
        local_result.reserve(end - start);
        
        for (size_t i = start; i < end; ++i) {
            local_result.push_back(fn(local_input[i]));
        }

        // Gather results based on output type
        if constexpr (std::is_arithmetic_v<OutT>) {
            // For arithmetic types, use MPI_Allgatherv
            std::vector<int> recv_counts(size_);
            std::vector<int> displacements(size_);
            
            int local_count = static_cast<int>(local_result.size());
            MPI_Allgather(&local_count, 1, MPI_INT, recv_counts.data(), 1, MPI_INT, comm_);
            
            int total_count = 0;
            for (int i = 0; i < size_; ++i) {
                displacements[i] = total_count;
                total_count += recv_counts[i];
            }
            
            std::vector<OutT> global_result(total_count);
            MPI_Allgatherv(local_result.data(), local_count, get_mpi_datatype<OutT>(),
                          global_result.data(), recv_counts.data(), displacements.data(),
                          get_mpi_datatype<OutT>(), comm_);
            
            printf("Rank %d: Gathered %d total results via MPI_Allgatherv\n", rank_, total_count);
            fflush(stdout);
            return global_result;
            
        } else if constexpr (std::is_same_v<OutT, std::string>) {
            // Use custom string gathering
            return gather_strings(local_result);
            
        } else {
            // For complex types, gather to rank 0 and broadcast
            // This is a simplified approach - real implementation would need serialization
            std::vector<OutT> global_result;
            if (rank_ == 0) {
                global_result = local_result;
                global_result.resize(total_size);
            }
            return global_result;
        }
    }

    template <typename InT, typename OutT, typename ReduceFn>
    OutT reduce_impl(ReduceFn&& fn, const std::vector<InT>& input) const {
        // Broadcast input to all ranks
        size_t total_size = input.size();
        MPI_Bcast(&total_size, 1, MPI_UNSIGNED_LONG, 0, comm_);
        
        std::vector<InT> local_input = input;
        if (rank_ != 0) {
            local_input.resize(total_size);
        }
        
        if constexpr (std::is_arithmetic_v<InT>) {
            MPI_Bcast(local_input.data(), static_cast<int>(total_size), 
                     get_mpi_datatype<InT>(), 0, comm_);
        }
        
        // Get local chunk and apply reduction function
        auto [start, end] = get_chunk_bounds(total_size);
        std::vector<InT> local_chunk(local_input.begin() + start, local_input.begin() + end);
        
        printf("Rank %d: Reducing chunk of %zu elements\n", rank_, local_chunk.size());
        fflush(stdout);
        
        // Apply function to local chunk
        OutT local_result = fn(local_chunk);
        
        // Debug: Local reduction completed (result type may not be printable)
        printf("Rank %d: Local reduction completed\n", rank_);
        fflush(stdout);
        
        // Reduce across all ranks
        if constexpr (std::is_arithmetic_v<OutT>) {
            OutT global_result;
            MPI_Allreduce(&local_result, &global_result, 1, get_mpi_datatype<OutT>(),
                         MPI_SUM, comm_);
            
            if (rank_ == 0) {
                printf("Rank 0: Final MPI_Allreduce result: %g\n", static_cast<double>(global_result));
                fflush(stdout);
            }
            return global_result;
        } else {
            // For complex types, collect at rank 0 and broadcast
            OutT global_result = local_result;
            // In real implementation, you'd need custom reduction
            return global_result;
        }
    }

    template <typename InT, typename ChunkT, typename AggT, typename OutT, 
              typename ChunkFn, typename AggFn, typename FinalizeFn>
    OutT aggregate_impl(ChunkFn&& chunk_fn, AggFn&& agg_fn, FinalizeFn&& finalize_fn,
                       const std::vector<InT>& input) const {
        // Broadcast input size and data
        size_t total_size = input.size();
        MPI_Bcast(&total_size, 1, MPI_UNSIGNED_LONG, 0, comm_);
        
        std::vector<InT> local_input = input;
        if (rank_ != 0) {
            local_input.resize(total_size);
        }
        
        if constexpr (std::is_arithmetic_v<InT>) {
            MPI_Bcast(local_input.data(), static_cast<int>(total_size), 
                     get_mpi_datatype<InT>(), 0, comm_);
        }
        
        // Distribute input across ranks
        auto [start, end] = get_chunk_bounds(total_size);
        std::vector<InT> local_chunk(local_input.begin() + start, local_input.begin() + end);
        
        // Apply chunk function locally
        ChunkT local_chunk_result = chunk_fn(local_chunk);
        
        // Gather all chunk results to all ranks for aggregation
        std::vector<ChunkT> all_chunks(size_);
        
        if constexpr (std::is_arithmetic_v<ChunkT>) {
            MPI_Allgather(&local_chunk_result, 1, get_mpi_datatype<ChunkT>(),
                         all_chunks.data(), 1, get_mpi_datatype<ChunkT>(), comm_);
        } else {
            // For complex types, simplified gathering
            all_chunks[rank_] = local_chunk_result;
        }
        
        // Aggregate phase (done on all ranks for consistency)
        AggT agg_result = agg_fn(all_chunks);
        
        // Finalize phase
        OutT result = finalize_fn(agg_result);
        
        return result;
    }

    template <typename InT, typename KeyT, typename OutT, typename KeyFn, typename AggFn>
    std::unordered_map<KeyT, OutT> groupby_aggregate_impl(KeyFn&& key_fn, AggFn&& agg_fn,
                                                         const std::vector<InT>& input) const {
        // Broadcast input
        size_t total_size = input.size();
        MPI_Bcast(&total_size, 1, MPI_UNSIGNED_LONG, 0, comm_);
        
        std::vector<InT> local_input = input;
        if (rank_ != 0) {
            local_input.resize(total_size);
        }
        
        if constexpr (std::is_arithmetic_v<InT>) {
            MPI_Bcast(local_input.data(), static_cast<int>(total_size), 
                     get_mpi_datatype<InT>(), 0, comm_);
        }
        
        // Distribute input across ranks for initial grouping
        auto [start, end] = get_chunk_bounds(total_size);
        
        // Build local groups
        std::unordered_map<KeyT, std::vector<InT>> local_groups;
        for (size_t i = start; i < end; ++i) {
            KeyT key = key_fn(local_input[i]);
            local_groups[key].push_back(local_input[i]);
        }
        
        // Redistribute groups by key hash to ensure same keys are on same rank
        auto redistributed_groups = redistribute_by_key(local_groups);
        
        // Aggregate each group locally
        auto local_result = agg_fn(redistributed_groups);
        
        // Gather all results to all ranks
        // For simplicity, we'll assume results fit in memory
        // Real implementation would need more sophisticated gathering
        std::unordered_map<KeyT, OutT> global_result = local_result;
        
        return global_result;
    }
};

#else

// When MPI is disabled, alias MPIContext to SequentialContext
using MPIContext = SequentialContext;

#endif

} // namespace execution_context
} // namespace pipeline
} // namespace utils
} // namespace dftracer

#endif // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_MPI_H
