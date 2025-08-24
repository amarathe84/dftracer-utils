#ifndef __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_H
#define __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_H

#include <dftracer/utils/pipeline/adapters/adapters.h>
#include <dftracer/utils/pipeline/engines/engines.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/lazy_collections/planner.h>
#include <dftracer/utils/pipeline/operators/operators.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace lazy_collections {
namespace detail {
// Helper for **bounded** unary operators where the number of outputs is
// guaranteed to be <= input count (e.g., map: N->N, filter: N-><=N).
// If an operator produces more than the preallocated capacity, we throw to
// avoid silent truncation. For unbounded ops (e.g., flatmap), use alloc path.
template <typename ProduceFn>
void run_unary_bounded(std::shared_ptr<std::vector<std::byte>>& cur_bytes,
                       std::size_t& cur_elem, std::size_t out_elem_size,
                       ProduceFn&& produce_fn) {
  if (!cur_bytes) throw std::logic_error("unary op has no input bytes");
  const std::size_t count = cur_elem ? (cur_bytes->size() / cur_elem) : 0;
  std::vector<std::byte> out_bytes(count * out_elem_size);  // max capacity
  engines::ConstBuffer in{cur_bytes->data(), count, cur_elem, 0};
  engines::MutBuffer out{out_bytes.data(), count, out_elem_size, 0};

  const std::size_t produced = produce_fn(in, out, count);
  const std::size_t max_cap =
      (out_elem_size ? out_bytes.size() / out_elem_size : 0);
  if (produced > max_cap) {
    throw std::logic_error(
        "run_unary_bounded: operator produced more than bounded capacity; use "
        "alloc path (e.g., flatmap)");
  }
  out_bytes.resize(produced * out_elem_size);
  cur_bytes = std::make_shared<std::vector<std::byte>>(std::move(out_bytes));
  cur_elem = out_elem_size;
}

// Helper for **unbounded** unary operators where outputs may exceed inputs,
// e.g., flatmap. This version delegates to an engine alloc path to ensure no
// truncation occurs.
template <typename ProduceAllocFn>
void run_unary_unbounded(std::shared_ptr<std::vector<std::byte>>& cur_bytes,
                         std::size_t& cur_elem, std::size_t out_elem_size,
                         ProduceAllocFn&& produce_alloc_fn) {
  if (!cur_bytes) throw std::logic_error("unary op has no input bytes");
  const std::size_t count = cur_elem ? (cur_bytes->size() / cur_elem) : 0;
  engines::ConstBuffer in{cur_bytes->data(), count, cur_elem, 0};

  std::vector<std::byte> out_bytes = produce_alloc_fn(in);
  cur_elem = out_elem_size;
  cur_bytes = std::make_shared<std::vector<std::byte>>(std::move(out_bytes));
}
}  // namespace detail
}  // namespace lazy_collections

using lazy_collections::detail::run_unary_bounded;
using lazy_collections::detail::run_unary_unbounded;
using namespace lazy_collections;
template <class T>
class LazyCollection {
 public:
  using value_type = T;

  // Allow LazyCollection<U> methods to access private members of
  // LazyCollection<T> so member functions that construct a LazyCollection<U>
  // can assign to `res.plan_` and `res.node_`.
  template <class>
  friend class LazyCollection;

  LazyCollection() : plan_(std::make_shared<Planner>()), node_{} {}

  static LazyCollection from_sequence(const std::vector<T>& local) {
    auto plan = std::make_shared<lazy_collections::Planner>();
    auto bytes = std::make_shared<std::vector<std::byte>>();
    bytes->resize(local.size() * sizeof(T));
    if (!local.empty()) {
      std::memcpy(bytes->data(), local.data(), bytes->size());
    }

    auto src = std::make_unique<operators::SourceOperator>(std::move(bytes));
    OutputLayout out{sizeof(T), true};

    LazyCollection c;
    c.plan_ = plan;
    c.node_ = plan->add_node(std::move(src), /*parents=*/{}, out);
    return c;
  }

  template <class U>
  LazyCollection<U> map(void (*fn)(const T& in, U& out)) const {
    auto h = adapters::make_map_op<T, U>(fn);
    auto mop = std::make_unique<operators::MapOperator>(h.op);
    OutputLayout out{sizeof(U), true};
    LazyCollection<U> res;
    res.plan_ = plan_;
    res.node_ = plan_->add_node(std::move(mop), {node_}, out, h.state);
    return res;
  }

  template <class Fn, class U = std::decay_t<decltype(std::declval<Fn&>()(
                          std::declval<const T&>()))>>
  LazyCollection<U> map(Fn fn) const {
    auto h = adapters::make_map_op<T, U>(std::move(fn));
    auto mop = std::make_unique<operators::MapOperator>(h.op);
    OutputLayout out{sizeof(U), true};
    LazyCollection<U> res;
    res.plan_ = plan_;
    res.node_ = plan_->add_node(std::move(mop), {node_}, out, h.state);
    return res;
  }

  LazyCollection<T> filter(bool (*pred)(const T&)) const {
    auto h = adapters::make_filter_op<T>(pred);
    auto fop = std::make_unique<operators::FilterOperator>(h.op);
    OutputLayout out{sizeof(T), true};
    LazyCollection<T> res;
    res.plan_ = plan_;
    res.node_ = plan_->add_node(std::move(fop), {node_}, out, h.state);
    return res;
  }

  template <class Pred>
  LazyCollection<T> filter(Pred pred) const {
    auto h = adapters::make_filter_op<T>(std::move(pred));
    auto fop = std::make_unique<operators::FilterOperator>(h.op);
    OutputLayout out{sizeof(T), true};
    LazyCollection<T> res;
    res.plan_ = plan_;
    res.node_ = plan_->add_node(std::move(fop), {node_}, out, h.state);
    return res;
  }

  template <class U, class Fn>
  LazyCollection<U> flatmap(Fn fn, double expansion_hint = -1.0) const {
    auto h = adapters::make_flatmap_op<T, U>(std::move(fn), expansion_hint);
    auto fop = std::make_unique<operators::FlatMapOperator>(h.op);
    OutputLayout out{sizeof(U), true};
    LazyCollection<U> res;
    res.plan_ = plan_;
    res.node_ = plan_->add_node(std::move(fop), {node_}, out, h.state);
    return res;
  }

  // map_partitions: apply a partition-aware Fn to each partition
  // Supported Fn forms (all partition-aware), wrapped by adapters:
  //   - void(const PartitionInfo&, const T* data, size_t n, auto emit)
  //   - std::vector<U>(const PartitionInfo&, const T* data, size_t n)
  //   - std::initializer_list<U>(const PartitionInfo&, const T* data, size_t n)
  //   - std::pair<const U*, size_t>(const PartitionInfo&, const T* data, size_t n)
  template <class U, class Fn>
  LazyCollection<U> map_partitions(Fn fn) const {
    auto h = adapters::make_map_partitions_op<T, U>(std::move(fn));
    auto mop = std::make_unique<operators::MapPartitionsOperator>(h.op);
    OutputLayout out{sizeof(U), true};
    LazyCollection<U> res;
    res.plan_ = plan_;
    res.node_ = plan_->add_node(std::move(mop), {node_}, out, h.state);
    return res;
  }

  std::vector<T> collect_local(context::ExecutionContext& ctx) const {
    std::vector<NodeId> chain;
    for (NodeId cur = node_;;) {
      chain.push_back(cur);
      const auto& n = plan_->node(cur);
      if (n.parents.empty()) break;
      cur = n.parents.front();
    }

    std::shared_ptr<std::vector<std::byte>> cur_bytes;
    std::size_t cur_elem = 0;

    for (std::size_t k = chain.size(); k-- > 0;) {
      const auto& node = plan_->node(chain[k]);
      const auto op = node.op->op();
      switch (op) {
        case operators::Op::SOURCE: {
          auto* src = static_cast<operators::SourceOperator*>(node.op.get());
          cur_bytes = src->bytes;
          cur_elem = node.out.elem_size;
          break;
        }
        case operators::Op::MAP: {
          auto* mop = static_cast<operators::MapOperator*>(node.op.get());
          run_unary_bounded(
              cur_bytes, cur_elem, node.out.elem_size,
              [&](const engines::ConstBuffer& in, engines::MutBuffer& out,
                  std::size_t /*count*/) -> std::size_t {
                engines::run_map(ctx, *mop, in, out);
                return in.count;  // map preserves cardinality
              });
          break;
        }
        case operators::Op::MAP_PARTITIONS: {
          auto* mpop = static_cast<operators::MapPartitionsOperator*>(node.op.get());
          run_unary_unbounded(
              cur_bytes, cur_elem, node.out.elem_size,
              [&](const engines::ConstBuffer& in) -> std::vector<std::byte> {
                return engines::run_map_partitions_alloc(ctx, *mpop, in);
              });
          break;
        }
        case operators::Op::FILTER: {
          auto* fop = static_cast<operators::FilterOperator*>(node.op.get());
          run_unary_bounded(
              cur_bytes, cur_elem, node.out.elem_size,
              [&](const engines::ConstBuffer& in, engines::MutBuffer& out,
                  std::size_t /*count*/) -> std::size_t {
                return engines::run_filter(ctx, *fop, in, out);
              });
          break;
        }
        case operators::Op::FLATMAP: {
          auto* fop = static_cast<operators::FlatMapOperator*>(node.op.get());
          run_unary_unbounded(
              cur_bytes, cur_elem, node.out.elem_size,
              [&](const engines::ConstBuffer& in) -> std::vector<std::byte> {
                return engines::run_flatmap_alloc(ctx, *fop, in);
              });
          break;
        }
        default:
          throw std::logic_error(
              "Unsupported operator in LazyCollection executor");
      }
    }

    std::vector<T> out;
    if (!cur_bytes) return out;
    const std::size_t n = cur_bytes->size() / sizeof(T);
    out.resize(n);
    if (n > 0) std::memcpy(out.data(), cur_bytes->data(), n * sizeof(T));
    return out;
  }

 private:
  std::shared_ptr<Planner> plan_ = std::make_shared<Planner>();
  NodeId node_{};
};

}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_H
