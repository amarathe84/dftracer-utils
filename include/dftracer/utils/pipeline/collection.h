#ifndef __DFTRACER_UTILS_PIPELINE_COLLECTION_H
#define __DFTRACER_UTILS_PIPELINE_COLLECTION_H

#include <dftracer/utils/pipeline/engine/filter_engine.h>
#include <dftracer/utils/pipeline/engine/map_engine.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/execution_context/sequential.h>
#include <dftracer/utils/pipeline/operators/filter_operator.h>
#include <dftracer/utils/pipeline/operators/map_operator.h>

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

    // Prepare operator with a stateful trampoline
    struct State {
      const void* fn_ptr;
    } state{static_cast<const void*>(&fn)};

    operators::MapOperator op(sizeof(T), sizeof(U));
    op.fn_with_state = +[](const void* i, void* o, void* st) {
      const auto* s = static_cast<const State*>(st);
      const auto* fp = static_cast<const MapFunc*>(s->fn_ptr);
      const T& ti = *static_cast<const T*>(i);
      U& to = *static_cast<U*>(o);
      to = (*fp)(ti);
    };
    op.state = static_cast<void*>(&state);

    // Buffer views
    engine::ConstBuffer in_buf{data_.data(), data_.size(), sizeof(T), 0};
    engine::MutBuffer out_buf{out.data(), out.size(), sizeof(U), 0};

    engine::run_map(ctx, op, in_buf, out_buf);
    return Collection<U>(std::move(out));
  }

  // ---- filter: keep elements where predicate(element) == true ----
  // Eager, no explicit context: use SequentialContext and delegate to engine
  template <class Pred>
  auto filter(Pred pred) const -> Collection<T> {
    context::SequentialContext seq;
    return filter(pred, seq);
  }

  // Context-aware filter using engine::run_filter.
  // Note: This path assumes T is trivially copyable since the engine compacts
  // using std::memcpy. Non-trivial types will be supported later via serde.
  template <class Pred>
  auto filter(Pred pred, context::ExecutionContext& ctx) const
      -> Collection<T> {
    static_assert(std::is_trivially_copyable_v<T>,
                  "Collection::filter currently requires T to be trivially "
                  "copyable; add serde to support complex types.");

    std::vector<T> out(data_.size());  // maximum possible size; will shrink

    // Wrap capturing predicate into a stateful trampoline
    struct State {
      const void* pred_ptr;
    } state{static_cast<const void*>(&pred)};

    operators::FilterOperator op(sizeof(T));
    op.pred_with_state = +[](const void* in, void* st) -> bool {
      const auto* s = static_cast<const State*>(st);
      const auto* fp = static_cast<const Pred*>(s->pred_ptr);
      const T& ti = *static_cast<const T*>(in);
      return (*fp)(ti);
    };
    op.state = static_cast<void*>(&state);

    engine::ConstBuffer in_buf{data_.data(), data_.size(), sizeof(T), 0};
    engine::MutBuffer out_buf{out.data(), out.size(), sizeof(T), 0};

    const std::size_t kept = engine::run_filter(ctx, op, in_buf, out_buf);
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
