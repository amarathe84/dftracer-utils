#ifndef __DFTRACER_UTILS_PIPELINE_COLLECTION_H
#define __DFTRACER_UTILS_PIPELINE_COLLECTION_H

#include <dftracer/utils/pipeline/adapters/adapters.h>
#include <dftracer/utils/pipeline/engines/engines.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/execution_context/sequential.h>
#include <dftracer/utils/pipeline/operators/operators.h>

#include <cstddef>
#include <cstring>
#include <functional>
#include <type_traits>
#include <utility>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {

template <class T>
class Collection {
 public:
  using value_type = T;

  Collection() = default;
  explicit Collection(std::vector<T> data) : data_(std::move(data)) {}

  static Collection from_sequence(std::vector<T> data) {
    return Collection(std::move(data));
  }

  std::size_t size() const noexcept { return data_.size(); }
  bool empty() const noexcept { return data_.empty(); }

  const std::vector<T>& data() const noexcept { return data_; }
  std::vector<T>& data() noexcept { return data_; }

  const T& operator[](std::size_t i) const noexcept { return data_[i]; }
  T& operator[](std::size_t i) noexcept { return data_[i]; }

  auto begin() noexcept { return data_.begin(); }
  auto end() noexcept { return data_.end(); }
  auto begin() const noexcept { return data_.begin(); }
  auto end() const noexcept { return data_.end(); }

  template <class Func>
  auto map(Func fn) const
      -> Collection<std::decay_t<decltype(fn(std::declval<const T&>()))>> {
    context::SequentialContext seq;
    return map(fn, seq);
  }

  template <class Func>
  auto map(Func fn, context::ExecutionContext& ctx) const
      -> Collection<std::decay_t<decltype(fn(std::declval<const T&>()))>> {
    using U = std::decay_t<decltype(fn(std::declval<const T&>()))>;
    std::vector<U> out(data_.size());
    auto h = adapters::make_map_op<T, U>(fn);
    auto in_buf = engines::to_const_buffer(data_);
    auto out_buf = engines::to_mut_buffer(out);
    engines::run_map(ctx, h.op, in_buf, out_buf);
    return Collection<U>(std::move(out));
  }

  template <class Pred>
  auto filter(Pred pred) const -> Collection<T> {
    context::SequentialContext seq;
    return filter(pred, seq);
  }

  template <class Pred>
  auto filter(Pred pred, context::ExecutionContext& ctx) const
      -> Collection<T> {
    static_assert(std::is_trivially_copyable_v<T>,
                  "Collection::filter currently requires T to be trivially "
                  "copyable.");
    std::vector<T> out(data_.size());
    auto h = adapters::make_filter_op<T>(pred);
    auto in_buf = engines::to_const_buffer(data_);
    auto out_buf = engines::to_mut_buffer(out);
    const std::size_t kept = engines::run_filter(ctx, h.op, in_buf, out_buf);
    out.resize(kept);
    return Collection<T>(std::move(out));
  }

  template <class U, class Fn>
  auto flatmap(Fn fn) const -> Collection<U> {
    context::SequentialContext seq;
    return flatmap<U>(std::move(fn), seq);
  }

  template <class U, class Fn>
  auto flatmap(Fn fn, context::ExecutionContext& ctx) const -> Collection<U> {
    static_assert(std::is_trivially_copyable_v<U>,
                  "Collection::flatmap currently requires U to be trivially "
                  "copyable.");
    auto h = adapters::make_flatmap_op<T, U>(std::move(fn));
    engines::ConstBuffer in_buf{static_cast<const void*>(data_.data()),
                                data_.size(), sizeof(T), 0};
    std::vector<std::byte> out_bytes =
        engines::run_flatmap_alloc(ctx, h.op, in_buf);
    const std::size_t n = out_bytes.size() / sizeof(U);
    std::vector<U> out(n);
    if (n) std::memcpy(out.data(), out_bytes.data(), n * sizeof(U));
    return Collection<U>(std::move(out));
  }

  // ---- map_partitions ----
  // Apply a partition-aware function to each partition and concatenate results.
  // Fn signatures supported via adapters::make_map_partitions_op:
  //   - void(const PartitionInfo&, const T* data, size_t n, auto emit)
  //   - std::vector<U>(const PartitionInfo&, const T* data, size_t n)
  //   - std::initializer_list<U>(const PartitionInfo&, const T* data, size_t n)
  //   - std::pair<const U*, size_t>(const PartitionInfo&, const T* data, size_t
  //   n)
  template <class U, class Fn>
  auto map_partitions(Fn fn) const -> Collection<U> {
    context::SequentialContext seq;
    return map_partitions<U>(std::move(fn), seq);
  }

  template <class U, class Fn>
  auto map_partitions(Fn fn, context::ExecutionContext& ctx) const
      -> Collection<U> {
    static_assert(std::is_trivially_copyable_v<U>,
                  "Collection::map_partitions currently requires U to be "
                  "trivially copyable.");
    auto h = adapters::make_map_partitions_op<T, U>(std::move(fn));
    engines::ConstBuffer in_buf{static_cast<const void*>(data_.data()),
                                data_.size(), sizeof(T), 0};
    std::vector<std::byte> out_bytes =
        engines::run_map_partitions_alloc(ctx, h.op, in_buf);
    const std::size_t n = out_bytes.size() / sizeof(U);
    std::vector<U> out(n);
    if (n) std::memcpy(out.data(), out_bytes.data(), n * sizeof(U));
    return Collection<U>(std::move(out));
  }

 private:
  std::vector<T> data_;
};

}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_COLLECTION_H
