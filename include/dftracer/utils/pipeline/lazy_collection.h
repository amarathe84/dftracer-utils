#ifndef __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_H
#define __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_H

#include <dftracer/utils/pipeline/adapters/adapters.h>
#include <dftracer/utils/pipeline/engines/engines.h>
#include <dftracer/utils/pipeline/execution_context/execution_context.h>
#include <dftracer/utils/pipeline/lazy_collections/planner.h>
#include <dftracer/utils/pipeline/operators/operators.h>

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

using namespace lazy_collections;

template <class T>
class LazyCollection {
 public:
  using value_type = T;

  LazyCollection() : plan_(std::make_shared<Planner>()), node_{} {}

  static LazyCollection from_vector(const std::vector<T>& local) {
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
          if (!cur_bytes) throw std::logic_error("map has no input bytes");
          const std::size_t count =
              cur_elem ? (cur_bytes->size() / cur_elem) : 0;
          std::vector<std::byte> out_bytes(count * node.out.elem_size);
          engine::ConstBuffer in{cur_bytes->data(), count, cur_elem, 0};
          engine::MutBuffer out{out_bytes.data(), count, node.out.elem_size, 0};
          auto* mop = static_cast<operators::MapOperator*>(node.op.get());
          engine::run_map(ctx, *mop, in, out);
          cur_bytes =
              std::make_shared<std::vector<std::byte>>(std::move(out_bytes));
          cur_elem = node.out.elem_size;
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
