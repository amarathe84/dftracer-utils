#ifndef __DFTRACER_UTILS_PIPELINE_PIPELINE_H
#define __DFTRACER_UTILS_PIPELINE_PIPELINE_H

#include <type_traits>
#include <functional>
#include <vector>
#include <memory>
#include <future>
#include <algorithm>
#include <numeric>
#include <thread>
#include <unordered_map>
#include <mutex>
#include <string>
#include <stdexcept>
#include <cctype>

namespace dftracer {
namespace utils {
namespace pipeline {

// Forward declarations
template<typename T, typename Operation = void>
class Bag;

// SFINAE-based type checking
template<typename F, typename T, typename = void>
struct is_map_function : std::false_type {};

template<typename F, typename T>
struct is_map_function<F, T, std::void_t<decltype(std::declval<F>()(std::declval<T>()))>> : std::true_type {};

template<typename F, typename T>
using map_result_t = decltype(std::declval<F>()(std::declval<T>()));

// Execution strategies
enum class ExecutionStrategy {
    Sequential,
    Threaded,
    MPI
};

// Helper trait to check if Operation is void (leaf bag)
template<typename Operation>
struct is_leaf_bag : std::is_same<Operation, void> {};

// Helper trait to detect if we're working with partitioned data
template<typename T>
struct is_partitioned_data : std::false_type {};

template<typename T>
struct is_partitioned_data<std::vector<T>> : std::true_type {};

inline size_t parse_size_string(const std::string& size_str) {
    if (size_str.empty()) {
        throw std::invalid_argument("Empty size string");
    }
    
    size_t pos = 0;
    double value;
    
    try {
        value = std::stod(size_str, &pos);
    } catch (const std::exception& e) {
        throw std::invalid_argument("Invalid numeric value in size string: " + size_str);
    }
    
    if (value < 0) {
        throw std::invalid_argument("Size cannot be negative");
    }
    
    std::string unit = size_str.substr(pos);
    std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);
    
    // Remove whitespace
    unit.erase(std::remove_if(unit.begin(), unit.end(), ::isspace), unit.end());
    
    if (unit == "b" || unit.empty()) return static_cast<size_t>(value);
    if (unit == "kb") return static_cast<size_t>(value * 1024);
    if (unit == "mb") return static_cast<size_t>(value * 1024 * 1024);
    if (unit == "gb") return static_cast<size_t>(value * 1024 * 1024 * 1024);
    
    throw std::invalid_argument("Unknown size unit: " + unit);
}

// CRTP Base execution context with partition awareness
template<typename Derived>
class ExecutionContext {
public:
    virtual ~ExecutionContext() = default;
    virtual ExecutionStrategy strategy() const = 0;
    
    template<typename T, typename MapFunc>
    auto execute_map(const std::vector<T>& input, MapFunc&& func) const 
        -> std::vector<map_result_t<MapFunc, T>> {
        return static_cast<const Derived*>(this)->template execute_map_impl<T, MapFunc>(
            std::forward<MapFunc>(func), input);
    }
    
    template<typename T, typename MapPartitionsFunc>
    auto execute_map_partitions(const std::vector<T>& input, MapPartitionsFunc&& func) const {
        return static_cast<const Derived*>(this)->template execute_map_partitions_impl<T, MapPartitionsFunc>(
            std::forward<MapPartitionsFunc>(func), input);
    }
    
    // Partition-aware execution for repartitioned data
    template<typename T, typename MapPartitionsFunc>
    auto execute_repartitioned_map_partitions(const std::vector<std::vector<T>>& partitions, 
                                             MapPartitionsFunc&& func) const {
        return static_cast<const Derived*>(this)->template execute_repartitioned_map_partitions_impl<T, MapPartitionsFunc>(
            partitions, std::forward<MapPartitionsFunc>(func));
    }
    
    template<typename T, typename ReduceFunc>
    auto execute_reduce(const std::vector<T>& input, ReduceFunc&& func) const 
        -> std::vector<T> {
        return static_cast<const Derived*>(this)->template execute_reduce_impl<T, ReduceFunc>(
            std::forward<ReduceFunc>(func), input);
    }
    
    template<typename T>
    auto execute_repartition(const std::vector<T>& input, size_t num_partitions) const 
        -> std::vector<std::vector<T>> {
        return static_cast<const Derived*>(this)->template execute_repartition_impl<T>(input, num_partitions);
    }
    
    template<typename T>
    auto execute_repartition_by_bytes(const std::vector<T>& input, size_t target_bytes, bool estimate = true) const 
        -> std::vector<std::vector<T>> {
        return static_cast<const Derived*>(this)->template execute_repartition_by_bytes_impl<T>(input, target_bytes, estimate);
    }
    
    template<typename T, typename HashFunc>
    auto execute_repartition_by_hash(const std::vector<T>& input, size_t num_partitions, HashFunc&& hash_func) const 
        -> std::vector<std::vector<T>> {
        return static_cast<const Derived*>(this)->template execute_repartition_by_hash_impl<T, HashFunc>(
            input, num_partitions, std::forward<HashFunc>(hash_func));
    }
    
    template<typename T, typename KeyFunc>
    auto execute_groupby(const std::vector<T>& input, KeyFunc&& key_func) const {
        return static_cast<const Derived*>(this)->template execute_groupby_impl<T, KeyFunc>(
            input, std::forward<KeyFunc>(key_func));
    }
};

// Sequential execution context
class SequentialContext : public ExecutionContext<SequentialContext> {
public:
    ExecutionStrategy strategy() const override { return ExecutionStrategy::Sequential; }
    
    template<typename T, typename MapFunc>
    auto execute_map_impl(MapFunc&& func, const std::vector<T>& input) const 
        -> std::vector<map_result_t<MapFunc, T>> {
        std::vector<map_result_t<MapFunc, T>> result;
        result.reserve(input.size());
        
        for (const auto& item : input) {
            result.push_back(func(item));
        }
        return result;
    }
    
    template<typename T, typename MapPartitionsFunc>
    auto execute_map_partitions_impl(MapPartitionsFunc&& func, const std::vector<T>& input) const {
        using partition_result_t = decltype(func(std::vector<T>{}));
        using element_t = typename partition_result_t::value_type;
        
        size_t partition_size = std::max(input.size() / 4, size_t(1));
        std::vector<element_t> final_result;
        
        for (size_t i = 0; i < input.size(); i += partition_size) {
            size_t end = std::min(i + partition_size, input.size());
            std::vector<T> partition(input.begin() + i, input.begin() + end);
            
            auto partition_result = func(partition);
            final_result.insert(final_result.end(), 
                               partition_result.begin(), partition_result.end());
        }
        
        return final_result;
    }
    
    // Partition-aware execution - each partition processed independently
    template<typename T, typename MapPartitionsFunc>
    auto execute_repartitioned_map_partitions_impl(const std::vector<std::vector<T>>& partitions, 
                                                   MapPartitionsFunc&& func) const {
        using partition_result_t = decltype(func(std::declval<std::vector<T>>()));
        using element_t = typename partition_result_t::value_type;
        
        std::vector<element_t> final_result;
        
        // Process each partition sequentially but as independent units
        for (const auto& partition : partitions) {
            auto partition_result = func(partition);
            final_result.insert(final_result.end(), 
                               partition_result.begin(), partition_result.end());
        }
        
        return final_result;
    }
    
    template<typename T, typename ReduceFunc>
    auto execute_reduce_impl(ReduceFunc&& func, const std::vector<T>& input) const 
        -> std::vector<T> {
        if (input.empty()) return {};
        
        T result = input[0];
        for (size_t i = 1; i < input.size(); ++i) {
            result = func(result, input[i]);
        }
        return {result};
    }
    
    template<typename T>
    auto execute_repartition_impl(const std::vector<T>& input, size_t num_partitions) const 
        -> std::vector<std::vector<T>> {
        if (num_partitions == 0 || input.empty()) {
            return {};
        }
        
        std::vector<std::vector<T>> partitions(num_partitions);
        size_t partition_size = (input.size() + num_partitions - 1) / num_partitions;
        
        for (size_t i = 0; i < input.size(); ++i) {
            size_t partition_idx = i / partition_size;
            if (partition_idx >= num_partitions) partition_idx = num_partitions - 1;
            partitions[partition_idx].push_back(input[i]);
        }
        return partitions;
    }
    
    template<typename T>
auto execute_repartition_by_bytes_impl(const std::vector<T>& input, size_t target_bytes, bool estimate = true) const 
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
            estimated_element_size = 1; // Avoid division by zero
        }
        
        size_t elements_per_partition = std::max(target_bytes / estimated_element_size, size_t(1));
        size_t num_partitions = (input.size() + elements_per_partition - 1) / elements_per_partition;
        
        return execute_repartition_impl(input, num_partitions);
    } else {
        std::vector<std::vector<T>> partitions;
        std::vector<T> current_partition;
        size_t current_bytes = 0;
        
        for (const auto& item : input) {
            size_t item_size = get_actual_size(item);
            
            // Avoid infinite loop if single item is larger than target
            if (current_bytes + item_size > target_bytes && !current_partition.empty()) {
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

    
    template<typename T, typename HashFunc>
    auto execute_repartition_by_hash_impl(const std::vector<T>& input, size_t num_partitions, HashFunc&& hash_func) const 
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
    
    template<typename T, typename KeyFunc>
    auto execute_groupby_impl(const std::vector<T>& input, KeyFunc&& key_func) const {
        using key_type = decltype(key_func(std::declval<T>()));
        std::unordered_map<key_type, std::vector<T>> groups;
        
        for (const auto& item : input) {
            auto key = key_func(item);
            groups[key].push_back(item);
        }
        
        return groups;
    }

private:
    // Helper traits for size calculation
    template<typename T, typename = void>
    struct has_size_method : std::false_type {};
    
    template<typename T>
    struct has_size_method<T, std::void_t<decltype(std::declval<T>().size())>> : std::true_type {};
    
    template<typename T>
    typename std::enable_if_t<std::is_arithmetic_v<T>, size_t>
    estimate_element_size_impl(const std::vector<T>& input) const {
        return sizeof(T);
    }
    
    template<typename T>
    typename std::enable_if_t<has_size_method<T>::value && !std::is_arithmetic_v<T>, size_t>
    estimate_element_size_impl(const std::vector<T>& input) const {
        if (input.empty()) return 32;
        size_t total_size = 0;
        size_t sample_size = std::min(input.size(), size_t(100));
        for (size_t i = 0; i < sample_size; ++i) {
            total_size += input[i].size();
        }
        return total_size / sample_size;
    }
    
    template<typename T>
    typename std::enable_if_t<!std::is_arithmetic_v<T> && !has_size_method<T>::value, size_t>
    estimate_element_size_impl(const std::vector<T>& input) const {
        return sizeof(T);
    }

    template<typename T>
size_t estimate_element_size(const std::vector<T>& input) const {
    if (input.empty()) return sizeof(T); // Fallback for empty input
    
    return estimate_element_size_impl(input);
}
    
    template<typename T>
    typename std::enable_if_t<std::is_arithmetic_v<T>, size_t>
    get_actual_size_impl(const T& item) const {
        return sizeof(T);
    }
    
    template<typename T>
    typename std::enable_if_t<has_size_method<T>::value && !std::is_arithmetic_v<T>, size_t>
    get_actual_size_impl(const T& item) const {
        return item.size();
    }
    
    template<typename T>
    typename std::enable_if_t<!std::is_arithmetic_v<T> && !has_size_method<T>::value, size_t>
    get_actual_size_impl(const T& item) const {
        return sizeof(T);
    }
    
    template<typename T>
    size_t get_actual_size(const T& item) const {
        return get_actual_size_impl(item);
    }
};

// Threaded execution context
class ThreadedContext : public ExecutionContext<ThreadedContext> {
public:
    ThreadedContext(size_t num_threads = std::thread::hardware_concurrency()) 
        : num_threads_(num_threads) {}
    
    ExecutionStrategy strategy() const override { return ExecutionStrategy::Threaded; }
    size_t num_threads() const { return num_threads_; }
    
    template<typename T, typename MapFunc>
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
    
    template<typename T, typename MapPartitionsFunc>
    auto execute_map_partitions_impl(MapPartitionsFunc&& func, const std::vector<T>& input) const {
        using partition_result_t = decltype(func(std::vector<T>{}));
        using element_t = typename partition_result_t::value_type;
        
        size_t partition_size = (input.size() + num_threads_ - 1) / num_threads_;
        std::vector<std::future<partition_result_t>> futures;
        
        for (size_t t = 0; t < num_threads_; ++t) {
            size_t start = t * partition_size;
            size_t end = std::min(start + partition_size, input.size());
            
            if (start < end) {
                futures.emplace_back(std::async(std::launch::async,
                    [&input, &func, start, end]() {
                        std::vector<T> partition(input.begin() + start, input.begin() + end);
                        return func(partition);
                    }));
            }
        }
        
        std::vector<element_t> final_result;
        for (auto& future : futures) {
            auto partition_result = future.get();
            final_result.insert(final_result.end(), 
                               partition_result.begin(), partition_result.end());
        }
        
        return final_result;
    }
    
    // Partition-aware execution - each partition gets independent parallel processing
    template<typename T, typename MapPartitionsFunc>
    auto execute_repartitioned_map_partitions_impl(const std::vector<std::vector<T>>& partitions, 
                                                   MapPartitionsFunc&& func) const {
        using partition_result_t = decltype(func(std::declval<std::vector<T>>()));
        using element_t = typename partition_result_t::value_type;
        
        // Each partition becomes an independent parallel task - Dask-like behavior!
        std::vector<std::future<partition_result_t>> futures;
        
        for (const auto& partition : partitions) {
            futures.emplace_back(std::async(std::launch::async,
                [&partition, &func]() {
                    return func(partition);
                }));
        }
        
        // Collect results while preserving partition processing order
        std::vector<element_t> final_result;
        for (auto& future : futures) {
            auto partition_result = future.get();
            final_result.insert(final_result.end(), 
                               partition_result.begin(), partition_result.end());
        }
        
        return final_result;
    }
    
    template<typename T, typename ReduceFunc>
    auto execute_reduce_impl(ReduceFunc&& func, const std::vector<T>& input) const 
        -> std::vector<T> {
        if (input.empty()) return {};
        
        size_t chunk_size = (input.size() + num_threads_ - 1) / num_threads_;
        std::vector<std::future<T>> futures;
        
        for (size_t t = 0; t < num_threads_; ++t) {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, input.size());
            
            if (start < end) {
                futures.emplace_back(std::async(std::launch::async,
                    [&input, &func, start, end]() -> T {
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
    
    template<typename T>
    auto execute_repartition_impl(const std::vector<T>& input, size_t num_partitions) const 
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
    
    template<typename T>
    auto execute_repartition_by_bytes_impl(const std::vector<T>& input, size_t target_bytes, bool estimate = true) const 
        -> std::vector<std::vector<T>> {
        // For simplicity, delegate to sequential implementation
        SequentialContext seq_ctx;
        return seq_ctx.execute_repartition_by_bytes_impl(input, target_bytes, estimate);
    }
    
    template<typename T, typename HashFunc>
    auto execute_repartition_by_hash_impl(const std::vector<T>& input, size_t num_partitions, HashFunc&& hash_func) const 
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
                futures.emplace_back(std::async(std::launch::async,
                    [&input, &partitions, &partition_mutexes, &hash_func, num_partitions, start, end]() {
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
    
    template<typename T, typename KeyFunc>
    auto execute_groupby_impl(const std::vector<T>& input, KeyFunc&& key_func) const {
        using key_type = decltype(key_func(std::declval<T>()));
        
        size_t chunk_size = (input.size() + num_threads_ - 1) / num_threads_;
        std::vector<std::future<std::unordered_map<key_type, std::vector<T>>>> futures;
        
        for (size_t t = 0; t < num_threads_; ++t) {
            size_t start = t * chunk_size;
            size_t end = std::min(start + chunk_size, input.size());
            
            if (start < end) {
                futures.emplace_back(std::async(std::launch::async,
                    [&input, &key_func, start, end]() {
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

private:
    size_t num_threads_;
};

// Base class for bags without operations
template<typename T>
class BagBase {
protected:
    std::vector<T> data_;
    
public:
    explicit BagBase(std::vector<T> data) : data_(std::move(data)) {}
    BagBase(const BagBase& other) : data_(other.data_) {}
    BagBase(BagBase&& other) noexcept : data_(std::move(other.data_)) {}
    BagBase& operator=(const BagBase& other) {
        if (this != &other) {
            data_ = other.data_;
        }
        return *this;
    }
    BagBase& operator=(BagBase&& other) noexcept {
        if (this != &other) {
            data_ = std::move(other.data_);
        }
        return *this;
    }
};

// Specialization for leaf bags (Operation = void)
template<typename T>
class Bag<T, void> : public BagBase<T> {
public:
    using output_type = T;
    using BagBase<T>::data_;
    
    explicit Bag(std::vector<T> data) : BagBase<T>(std::move(data)) {}
    
    static Bag<T, void> from_sequence(std::vector<T> data) {
        return Bag<T, void>(std::move(data));
    }
    
    // Basic accessors
    size_t size() const { return data_.size(); }
    const std::vector<T>& data() const { return data_; }
    bool empty() const { return data_.empty(); }
    
    // Vector-like interface
    auto begin() { return data_.begin(); }
    auto end() { return data_.end(); }
    auto begin() const { return data_.begin(); }
    auto end() const { return data_.end(); }
    T& operator[](size_t index) { return data_[index]; }
    const T& operator[](size_t index) const { return data_[index]; }
    
    // Compute method
    template<typename Context>
    std::vector<T> compute(const Context& ctx) const {
        return data_;
    }
    
    // Lazy operations
    template<typename MapFunc>
    auto map(MapFunc func) const {
        using result_type = decltype(func(std::declval<T>()));
        
        auto new_operation = [*this, func](const auto& context) -> std::vector<result_type> {
            auto input_data = this->compute(context);
            return context.execute_map(input_data, func);
        };
        
        using operation_type = decltype(new_operation);
        return Bag<result_type, operation_type>({}, std::move(new_operation));
    }
    
    template<typename MapFunc>
    auto flatmap(MapFunc func) const {
        using result_container_type = decltype(func(std::declval<T>()));
        using result_element_type = typename result_container_type::value_type;
        
        auto new_operation = [*this, func](const auto& context) -> std::vector<result_element_type> {
            auto input_data = this->compute(context);
            
            std::vector<result_element_type> flattened_result;
            for (const auto& item : input_data) {
                auto item_results = func(item);
                flattened_result.insert(flattened_result.end(), 
                                      item_results.begin(), item_results.end());
            }
            return flattened_result;
        };
        
        using operation_type = decltype(new_operation);
        return Bag<result_element_type, operation_type>({}, std::move(new_operation));
    }
    
    template<typename MapPartitionsFunc>
    auto map_partitions(MapPartitionsFunc func) const {
        using partition_result_t = decltype(func(std::vector<T>{}));
        using element_t = typename partition_result_t::value_type;
        
        auto new_operation = [*this, func](const auto& context) -> std::vector<element_t> {
            auto input_data = this->compute(context);
            
            // Check if we're working with partitioned data (vector<vector<T>>)
            if constexpr (is_partitioned_data<T>::value) {
                // Use partition-aware execution for Dask-like behavior
                return context.execute_repartitioned_map_partitions(input_data, func);
            } else {
                // Use regular map_partitions for non-partitioned data
                return context.execute_map_partitions(input_data, func);
            }
        };
        
        using operation_type = decltype(new_operation);
        return Bag<element_t, operation_type>({}, std::move(new_operation));
    }
    
    template<typename ReduceFunc, typename Context>
    auto reduce(const Context& ctx, ReduceFunc func) const -> T {
        auto input_data = compute(ctx);
        auto result = ctx.execute_reduce(input_data, func);
        return result.empty() ? T{} : result[0];
    }
    
    auto repartition(size_t num_partitions) const {
        auto new_operation = [*this, num_partitions](const auto& context) -> std::vector<std::vector<T>> {
            auto input_data = this->compute(context);
            return context.execute_repartition(input_data, num_partitions);
        };
        
        using operation_type = decltype(new_operation);
        return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
    }
    
    auto repartition(const std::string& partition_size, bool estimate = true) const {
        size_t bytes = parse_size_string(partition_size);
        auto new_operation = [*this, bytes, estimate](const auto& context) -> std::vector<std::vector<T>> {
            auto input_data = this->compute(context);
            return context.execute_repartition_by_bytes(input_data, bytes, estimate);
        };
        
        using operation_type = decltype(new_operation);
        return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
    }
    
    template<typename HashFunc>
    auto repartition(size_t num_partitions, HashFunc hash_func) const {
        auto new_operation = [*this, num_partitions, hash_func](const auto& context) -> std::vector<std::vector<T>> {
            auto input_data = this->compute(context);
            return context.execute_repartition_by_hash(input_data, num_partitions, hash_func);
        };
        
        using operation_type = decltype(new_operation);
        return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
    }
    
    template<typename KeyFunc>
    auto groupby(KeyFunc key_func) const {
        auto new_operation = [*this, key_func](const auto& context) {
            auto input_data = this->compute(context);
            return context.execute_groupby(input_data, key_func);
        };
        
        using operation_type = decltype(new_operation);
        using key_type = decltype(key_func(std::declval<T>()));
        using result_type = std::unordered_map<key_type, std::vector<T>>;
        return Bag<result_type, operation_type>({}, std::move(new_operation));
    }
};

// General template for operation bags
template<typename T, typename Operation>
class Bag : public BagBase<T> {
public:
    using output_type = T;
    using BagBase<T>::data_;
    
private:
    Operation operation_;
    
public:
    // Operation bag constructor
    Bag(std::vector<T> data, Operation operation)
        : BagBase<T>(std::move(data)), operation_(std::move(operation)) {}
    
    // Copy constructor
    Bag(const Bag& other) : BagBase<T>(other), operation_(other.operation_) {}
    
    // Move constructor
    Bag(Bag&& other) noexcept 
        : BagBase<T>(std::move(other)), operation_(std::move(other.operation_)) {}
    
    // Assignment operators
    Bag& operator=(const Bag& other) {
        if (this != &other) {
            BagBase<T>::operator=(other);
            operation_ = other.operation_;
        }
        return *this;
    }
    
    Bag& operator=(Bag&& other) noexcept {
        if (this != &other) {
            BagBase<T>::operator=(std::move(other));
            operation_ = std::move(other.operation_);
        }
        return *this;
    }
    
    // Compute method - execute the operation chain
    template<typename Context>
    std::vector<T> compute(const Context& ctx) const {
        return operation_(ctx);
    }
    
    // Lazy operations
    template<typename MapFunc>
    auto map(MapFunc func) const {
        using result_type = decltype(func(std::declval<T>()));
        
        auto new_operation = [*this, func](const auto& context) -> std::vector<result_type> {
            auto input_data = this->compute(context);
            return context.execute_map(input_data, func);
        };
        
        using operation_type = decltype(new_operation);
        return Bag<result_type, operation_type>({}, std::move(new_operation));
    }
    
    template<typename MapFunc>
    auto flatmap(MapFunc func) const {
        using result_container_type = decltype(func(std::declval<T>()));
        using result_element_type = typename result_container_type::value_type;
        
        auto new_operation = [*this, func](const auto& context) -> std::vector<result_element_type> {
            auto input_data = this->compute(context);
            
            std::vector<result_element_type> flattened_result;
            for (const auto& item : input_data) {
                auto item_results = func(item);
                flattened_result.insert(flattened_result.end(), 
                                      item_results.begin(), item_results.end());
            }
            return flattened_result;
        };
        
        using operation_type = decltype(new_operation);
        return Bag<result_element_type, operation_type>({}, std::move(new_operation));
    }
    
    template<typename MapPartitionsFunc>
    auto map_partitions(MapPartitionsFunc func) const {
        using partition_result_t = decltype(func(std::vector<T>{}));
        using element_t = typename partition_result_t::value_type;
        
        auto new_operation = [*this, func](const auto& context) -> std::vector<element_t> {
            auto input_data = this->compute(context);
            
            // Check if we're working with partitioned data (vector<vector<T>>)
            if constexpr (is_partitioned_data<T>::value) {
                // Use partition-aware execution for Dask-like behavior
                return context.execute_repartitioned_map_partitions(input_data, func);
            } else {
                // Use regular map_partitions for non-partitioned data
                return context.execute_map_partitions(input_data, func);
            }
        };
        
        using operation_type = decltype(new_operation);
        return Bag<element_t, operation_type>({}, std::move(new_operation));
    }
    
    template<typename ReduceFunc, typename Context>
    auto reduce(const Context& ctx, ReduceFunc func) const -> T {
        auto input_data = compute(ctx);
        auto result = ctx.execute_reduce(input_data, func);
        return result.empty() ? T{} : result[0];
    }
    
    auto repartition(size_t num_partitions) const {
        auto new_operation = [*this, num_partitions](const auto& context) -> std::vector<std::vector<T>> {
            auto input_data = this->compute(context);
            return context.execute_repartition(input_data, num_partitions);
        };
        
        using operation_type = decltype(new_operation);
        return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
    }
    
    auto repartition(const std::string& partition_size, bool estimate = true) const {
        size_t bytes = parse_size_string(partition_size);
        auto new_operation = [*this, bytes, estimate](const auto& context) -> std::vector<std::vector<T>> {
            auto input_data = this->compute(context);
            return context.execute_repartition_by_bytes(input_data, bytes, estimate);
        };
        
        using operation_type = decltype(new_operation);
        return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
    }
    
    template<typename HashFunc>
    auto repartition(size_t num_partitions, HashFunc hash_func) const {
        auto new_operation = [*this, num_partitions, hash_func](const auto& context) -> std::vector<std::vector<T>> {
            auto input_data = this->compute(context);
            return context.execute_repartition_by_hash(input_data, num_partitions, hash_func);
        };
        
        using operation_type = decltype(new_operation);
        return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
    }
    
    template<typename KeyFunc>
    auto groupby(KeyFunc key_func) const {
        auto new_operation = [*this, key_func](const auto& context) {
            auto input_data = this->compute(context);
            return context.execute_groupby(input_data, key_func);
        };
        
        using operation_type = decltype(new_operation);
        using key_type = decltype(key_func(std::declval<T>()));
        using result_type = std::unordered_map<key_type, std::vector<T>>;
        return Bag<result_type, operation_type>({}, std::move(new_operation));
    }
};

// Helper function to create bags
template<typename T>
auto from_sequence(std::vector<T> data) -> Bag<T, void> {
    return Bag<T, void>(std::move(data));
}

} // namespace pipeline
} // namespace utils
} // namespace dftracer

// fmt::formatter specializations
#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ranges.h>

// Forward declaration for operation bags
template<typename T, typename Operation>
struct fmt::formatter<dftracer::utils::pipeline::Bag<T, Operation>> {
    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
        return ctx.begin();
    }
    
    template<typename FormatContext>
    auto format(const dftracer::utils::pipeline::Bag<T, Operation>& bag, FormatContext& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "OperationBag[{}](lazy)", typeid(T).name());
    }
};

// Specialization for leaf bags (void operation)
template<typename T>
struct fmt::formatter<dftracer::utils::pipeline::Bag<T, void>> {
    bool show_data = false;
    
    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
        auto it = ctx.begin(), end = ctx.end();
        if (it != end && *it == 'd') {
            show_data = true;
            ++it;
        }
        if (it != end && *it != '}') {
            throw format_error("invalid format");
        }
        return it;
    }
    
    template<typename FormatContext>
    auto format(const dftracer::utils::pipeline::Bag<T, void>& bag, FormatContext& ctx) -> decltype(ctx.out()) {
        if (show_data && bag.size() <= 10) {
            return fmt::format_to(ctx.out(), "Bag[{}]({} items): {}", 
                                 typeid(T).name(), bag.size(), bag.data());
        } else if (show_data && bag.size() > 10) {
            return fmt::format_to(ctx.out(), "Bag[{}]({} items): [{}, {}, ..., {}]", 
                                 typeid(T).name(), bag.size(), 
                                 bag[0], bag[1], bag[bag.size()-1]);
        } else {
            return fmt::format_to(ctx.out(), "Bag[{}]({} items)", 
                                 typeid(T).name(), bag.size());
        }
    }
};

// Utility function to format computed bags
template<typename T, typename Operation, typename Context>
auto format_computed_bag(const dftracer::utils::pipeline::Bag<T, Operation>& bag, 
                        const Context& ctx, bool show_data = false) -> std::string {
    auto computed_data = bag.compute(ctx);
    if (show_data && computed_data.size() <= 10) {
        return fmt::format("ComputedBag[{}]({} items): {}", 
                          typeid(T).name(), computed_data.size(), computed_data);
    } else if (show_data && computed_data.size() > 10) {
        return fmt::format("ComputedBag[{}]({} items): [{}, {}, ..., {}]", 
                          typeid(T).name(), computed_data.size(),
                          computed_data[0], computed_data[1], computed_data[computed_data.size()-1]);
    } else {
        return fmt::format("ComputedBag[{}]({} items)", 
                          typeid(T).name(), computed_data.size());
    }
}

#endif
