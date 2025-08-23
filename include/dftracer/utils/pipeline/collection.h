#ifndef __DFTRACER_UTILS_PIPELINE_COLLECTION_H
#define __DFTRACER_UTILS_PIPELINE_COLLECTION_H

#include <dftracer/utils/pipeline/adapters/adapters.h>
#include <dftracer/utils/pipeline/engines/engines.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/execution_context/sequential.h>
#include <dftracer/utils/pipeline/operators/operators.h>

#include <cstddef>
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

  template <class MapFunc>
  auto map(MapFunc fn) const
      -> Collection<std::decay_t<decltype(fn(std::declval<const T&>()))>> {
    context::SequentialContext seq;
    return map(fn, seq);
  }

  template <class MapFunc>
  auto map(MapFunc fn, context::ExecutionContext& ctx) const
      -> Collection<std::decay_t<decltype(fn(std::declval<const T&>()))>> {
    using U = std::decay_t<decltype(fn(std::declval<const T&>()))>;
    std::vector<U> out(data_.size());
    auto h = adapters::make_map_op<T, U>(fn);
    auto in_buf = engine::to_const_buffer(data_);
    auto out_buf = engine::to_mut_buffer(out);
    engine::run_map(ctx, h.op, in_buf, out_buf);
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
                  "copyable; add serde to support complex types.");
    std::vector<T> out(data_.size());
    auto h = adapters::make_filter_op<T>(pred);
    auto in_buf = engine::to_const_buffer(data_);
    auto out_buf = engine::to_mut_buffer(out);
    const std::size_t kept = engine::run_filter(ctx, h.op, in_buf, out_buf);
    out.resize(kept);
    return Collection<T>(std::move(out));
  }

 private:
  std::vector<T> data_;
};

}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_COLLECTION_H
