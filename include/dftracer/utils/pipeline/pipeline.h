#ifndef __DFTRACER_UTILS_PIPELINE_PIPELINE_H
#define __DFTRACER_UTILS_PIPELINE_PIPELINE_H

#include <algorithm>
#include <cctype>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <dftracer/utils/pipeline/internal.h>
#include <dftracer/utils/pipeline/execution_context/mpi.h>

namespace dftracer {
namespace utils {
namespace pipeline {

using namespace internal;

template <typename T, typename Operation = void>
class Bag;

template <typename T>
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

template <typename T>
class Bag<T, void> : public BagBase<T> {
 public:
  using output_type = T;
  using BagBase<T>::data_;

  explicit Bag(std::vector<T> data) : BagBase<T>(std::move(data)) {}

  static Bag<T, void> from_sequence(std::vector<T> data) {
    return Bag<T, void>(std::move(data));
  }

  template<typename Context>
  static Bag<T, void> from_sequence_distributed(Context& ctx, const std::vector<T>& data) {
    if constexpr (std::is_same_v<Context, context::MPIContext>) {
      std::vector<T> my_data;
      for (size_t i = ctx.rank(); i < data.size(); i += ctx.size()) {
        my_data.push_back(data[i]);
      }
      return from_sequence(my_data);
    } else {
      return from_sequence(data);
    }
  }

  size_t size() const { return data_.size(); }
  const std::vector<T>& data() const { return data_; }
  bool empty() const { return data_.empty(); }

  auto begin() { return data_.begin(); }
  auto end() { return data_.end(); }
  auto begin() const { return data_.begin(); }
  auto end() const { return data_.end(); }
  T& operator[](size_t index) { return data_[index]; }
  const T& operator[](size_t index) const { return data_[index]; }

  template <typename Context>
  std::vector<T> compute(const Context&) const {
    return data_;
  }

  template <typename MapFunc>
  auto map(MapFunc func) const {
    using result_type = decltype(func(std::declval<T>()));

    auto new_operation =
        [*this, func](const auto& context) -> std::vector<result_type> {
      auto input_data = this->compute(context);
      return context.execute_map(input_data, func);
    };

    using operation_type = decltype(new_operation);
    return Bag<result_type, operation_type>({}, std::move(new_operation));
  }

  template <typename MapFunc>
  auto flatmap(MapFunc func) const {
    using result_container_type = decltype(func(std::declval<T>()));
    using result_element_type = typename result_container_type::value_type;

    auto new_operation =
        [*this, func](const auto& context) -> std::vector<result_element_type> {
      auto input_data = this->compute(context);

      std::vector<result_element_type> flattened_result;
      for (const auto& item : input_data) {
        auto item_results = func(item);
        flattened_result.insert(flattened_result.end(), item_results.begin(),
                                item_results.end());
      }
      return flattened_result;
    };

    using operation_type = decltype(new_operation);
    return Bag<result_element_type, operation_type>({},
                                                    std::move(new_operation));
  }

  template <typename MapPartitionsFunc>
  auto map_partitions(MapPartitionsFunc func) const {
    using partition_result_t = decltype(func(std::vector<T>{}));
    using element_t = typename partition_result_t::value_type;

    auto new_operation = [*this,
                          func](const auto& context) -> std::vector<element_t> {
      auto input_data = this->compute(context);

      if constexpr (is_partitioned_data<T>::value) {
        return context.execute_repartitioned_map_partitions(input_data, func);
      } else {
        return context.execute_map_partitions(input_data, func);
      }
    };

    using operation_type = decltype(new_operation);
    return Bag<element_t, operation_type>({}, std::move(new_operation));
  }

  template <typename ReduceFunc, typename Context>
  auto reduce(const Context& ctx, ReduceFunc func) const -> T {
    auto input_data = compute(ctx);
    auto result = ctx.execute_reduce(input_data, func);
    return result.empty() ? T{} : result[0];
  }

  auto repartition(size_t num_partitions) const {
    auto new_operation =
        [*this,
         num_partitions](const auto& context) -> std::vector<std::vector<T>> {
      auto input_data = this->compute(context);
      return context.execute_repartition(input_data, num_partitions);
    };

    using operation_type = decltype(new_operation);
    return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
  }

  auto repartition(const std::string& partition_size,
                   bool estimate = true) const {
    size_t bytes = parse_size_string(partition_size);
    auto new_operation =
        [*this, bytes,
         estimate](const auto& context) -> std::vector<std::vector<T>> {
      auto input_data = this->compute(context);
      return context.execute_repartition_by_bytes(input_data, bytes, estimate);
    };

    using operation_type = decltype(new_operation);
    return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
  }

  template <typename HashFunc>
  auto repartition(size_t num_partitions, HashFunc hash_func) const {
    auto new_operation =
        [*this, num_partitions,
         hash_func](const auto& context) -> std::vector<std::vector<T>> {
      auto input_data = this->compute(context);
      return context.execute_repartition_by_hash(input_data, num_partitions,
                                                 hash_func);
    };

    using operation_type = decltype(new_operation);
    return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
  }

  template <typename KeyFunc>
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

  template <typename KeyFunc, typename AggFunc>
  auto distributed_groupby(KeyFunc key_func, AggFunc agg_func,
                           size_t num_partitions = 0) const {
    using agg_result_type = decltype(agg_func(std::vector<T>{}));

    auto new_operation =
        [*this, key_func, agg_func,
         num_partitions](const auto& context) -> std::vector<agg_result_type> {
      auto input_data = this->compute(context);
      return context.execute_distributed_groupby(input_data, key_func, agg_func,
                                                 num_partitions);
    };

    using operation_type = decltype(new_operation);
    return Bag<agg_result_type, operation_type>({}, std::move(new_operation));
  }

  template <typename Func>
  auto operator|(Func&& func) const -> decltype(func(*this)) {
    return func(*this);
  }

  template <typename Context>
  auto collect(const Context& ctx) const {
    auto local_results = compute(ctx);
  
    if constexpr (std::is_same_v<Context, context::MPIContext>) {
      return ctx.gather_results(local_results);
    } else {
      return local_results;
    }
  }

  auto collect() const {
    auto new_operation = [*this](const auto& context) -> std::vector<T> {
      auto local_data = this->compute(context);
      return context.collect(local_data);
    };
    
    using operation_type = decltype(new_operation);
    return Bag<T, operation_type>({}, std::move(new_operation));
  }
};

template <typename T, typename Operation>
class Bag : public BagBase<T> {
 public:
  using output_type = T;
  using BagBase<T>::data_;

 private:
  Operation operation_;

 public:
  Bag(std::vector<T> data, Operation operation)
      : BagBase<T>(std::move(data)), operation_(std::move(operation)) {}

  Bag(const Bag& other) : BagBase<T>(other), operation_(other.operation_) {}

  Bag(Bag&& other) noexcept
      : BagBase<T>(std::move(other)), operation_(std::move(other.operation_)) {}

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

  template <typename Context>
  std::vector<T> compute(const Context& ctx) const {
    return operation_(ctx);
  }

  template <typename MapFunc>
  auto map(MapFunc func) const {
    using result_type = decltype(func(std::declval<T>()));

    auto new_operation =
        [*this, func](const auto& context) -> std::vector<result_type> {
      auto input_data = this->compute(context);
      return context.execute_map(input_data, func);
    };

    using operation_type = decltype(new_operation);
    return Bag<result_type, operation_type>({}, std::move(new_operation));
  }

  template <typename MapFunc>
  auto flatmap(MapFunc func) const {
    using result_container_type = decltype(func(std::declval<T>()));
    using result_element_type = typename result_container_type::value_type;

    auto new_operation =
        [*this, func](const auto& context) -> std::vector<result_element_type> {
      auto input_data = this->compute(context);

      std::vector<result_element_type> flattened_result;
      for (const auto& item : input_data) {
        auto item_results = func(item);
        flattened_result.insert(flattened_result.end(), item_results.begin(),
                                item_results.end());
      }
      return flattened_result;
    };

    using operation_type = decltype(new_operation);
    return Bag<result_element_type, operation_type>({},
                                                    std::move(new_operation));
  }

  template <typename MapPartitionsFunc>
  auto map_partitions(MapPartitionsFunc func) const {
    using partition_result_t = decltype(func(std::vector<T>{}));
    using element_t = typename partition_result_t::value_type;

    auto new_operation = [*this,
                          func](const auto& context) -> std::vector<element_t> {
      auto input_data = this->compute(context);

      if constexpr (is_partitioned_data<T>::value) {
        return context.execute_repartitioned_map_partitions(input_data, func);
      } else {
        return context.execute_map_partitions(input_data, func);
      }
    };

    using operation_type = decltype(new_operation);
    return Bag<element_t, operation_type>({}, std::move(new_operation));
  }

  template <typename ReduceFunc, typename Context>
  auto reduce(const Context& ctx, ReduceFunc func) const -> T {
    auto input_data = compute(ctx);
    auto result = ctx.execute_reduce(input_data, func);
    return result.empty() ? T{} : result[0];
  }

  auto repartition(size_t num_partitions) const {
    auto new_operation =
        [*this,
         num_partitions](const auto& context) -> std::vector<std::vector<T>> {
      auto input_data = this->compute(context);
      return context.execute_repartition(input_data, num_partitions);
    };

    using operation_type = decltype(new_operation);
    return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
  }

  auto repartition(const std::string& partition_size,
                   bool estimate = true) const {
    size_t bytes = parse_size_string(partition_size);
    auto new_operation =
        [*this, bytes,
         estimate](const auto& context) -> std::vector<std::vector<T>> {
      auto input_data = this->compute(context);
      return context.execute_repartition_by_bytes(input_data, bytes, estimate);
    };

    using operation_type = decltype(new_operation);
    return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
  }

  template <typename HashFunc>
  auto repartition(size_t num_partitions, HashFunc hash_func) const {
    auto new_operation =
        [*this, num_partitions,
         hash_func](const auto& context) -> std::vector<std::vector<T>> {
      auto input_data = this->compute(context);
      return context.execute_repartition_by_hash(input_data, num_partitions,
                                                 hash_func);
    };

    using operation_type = decltype(new_operation);
    return Bag<std::vector<T>, operation_type>({}, std::move(new_operation));
  }

  template <typename KeyFunc>
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

  template <typename KeyFunc, typename AggFunc>
  auto distributed_groupby(KeyFunc key_func, AggFunc agg_func,
                           size_t num_partitions = 0) const {
    using key_result_type = decltype(key_func(std::declval<T>()));
    using agg_result_type =
        decltype(agg_func(key_result_type{}, std::vector<T>{}));

    auto new_operation =
        [*this, key_func, agg_func,
         num_partitions](const auto& context) -> std::vector<agg_result_type> {
      auto input_data = this->compute(context);
      return context.execute_distributed_groupby(input_data, key_func, agg_func,
                                                 num_partitions);
    };

    using operation_type = decltype(new_operation);
    return Bag<agg_result_type, operation_type>({}, std::move(new_operation));
  }

  template <typename Func>
  auto operator|(Func&& func) const -> decltype(func(*this)) {
    return func(*this);
  }

  template <typename Context>
  auto collect(const Context& ctx) const {
    auto local_results = compute(ctx);
  
    if constexpr (std::is_same_v<Context, context::MPIContext>) {
      return ctx.gather_results(local_results);
    } else {
      return local_results;
    }
  }

  auto collect() const {
    auto new_operation = [*this](const auto& context) -> std::vector<T> {
      auto local_data = this->compute(context);
      return context.collect(local_data);
    };
    
    using operation_type = decltype(new_operation);
    return Bag<T, operation_type>({}, std::move(new_operation));
  }
};

template <typename T>
auto from_sequence(std::vector<T> data) -> Bag<T, void> {
  return Bag<T, void>(std::move(data));
}

template<typename T, typename Context>
auto from_sequence_distributed(Context& ctx, const std::vector<T>& data) {
  if constexpr (std::is_same_v<Context, context::MPIContext>) {
    std::vector<T> my_data;
    for (size_t i = ctx.rank(); i < data.size(); i += ctx.size()) {
      my_data.push_back(data[i]);
    }
    return from_sequence(my_data);
  } else {
    return from_sequence(data);
  }
}
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#include <spdlog/fmt/fmt.h>
#include <spdlog/fmt/ranges.h>

template <typename T, typename Operation>
struct fmt::formatter<dftracer::utils::pipeline::Bag<T, Operation>> {
  constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const dftracer::utils::pipeline::Bag<T, Operation>& bag,
              FormatContext& ctx) -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "OperationBag[{}](lazy)",
                          typeid(T).name());
  }
};

template <typename T>
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

  template <typename FormatContext>
  auto format(const dftracer::utils::pipeline::Bag<T, void>& bag,
              FormatContext& ctx) -> decltype(ctx.out()) {
    if (show_data && bag.size() <= 10) {
      return fmt::format_to(ctx.out(), "Bag[{}]({} items): {}",
                            typeid(T).name(), bag.size(), bag.data());
    } else if (show_data && bag.size() > 10) {
      return fmt::format_to(ctx.out(), "Bag[{}]({} items): [{}, {}, ..., {}]",
                            typeid(T).name(), bag.size(), bag[0], bag[1],
                            bag[bag.size() - 1]);
    } else {
      return fmt::format_to(ctx.out(), "Bag[{}]({} items)", typeid(T).name(),
                            bag.size());
    }
  }
};

template <typename T, typename Operation, typename Context>
auto format_computed_bag(
    const dftracer::utils::pipeline::Bag<T, Operation>& bag, const Context& ctx,
    bool show_data = false) -> std::string {
  auto computed_data = bag.compute(ctx);
  if (show_data && computed_data.size() <= 10) {
    return fmt::format("ComputedBag[{}]({} items): {}", typeid(T).name(),
                       computed_data.size(), computed_data);
  } else if (show_data && computed_data.size() > 10) {
    return fmt::format("ComputedBag[{}]({} items): [{}, {}, ..., {}]",
                       typeid(T).name(), computed_data.size(), computed_data[0],
                       computed_data[1],
                       computed_data[computed_data.size() - 1]);
    } else {
    return fmt::format("ComputedBag[{}]({} items)", typeid(T).name(),
                       computed_data.size());
  }
}

#endif
