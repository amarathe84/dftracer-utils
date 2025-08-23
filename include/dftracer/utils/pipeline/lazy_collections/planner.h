#ifndef __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_PLANNER_H
#define __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_PLANNER_H

#include <dftracer/utils/pipeline/lazy_collections/planner_node.h>
#include <dftracer/utils/pipeline/operators/operator.h>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace lazy_collections {

struct Planner {
  std::vector<PlannerNode> nodes;
  std::vector<std::shared_ptr<void>> keep_alive;

  NodeId add_node(std::unique_ptr<operators::Operator> op,
                  std::vector<NodeId> parents, OutputLayout out,
                  std::shared_ptr<void> cookie = nullptr) {
    if (cookie) keep_alive.push_back(std::move(cookie));
    NodeId id{static_cast<std::uint32_t>(nodes.size())};
    PlannerNode n;
    n.parents = std::move(parents);
    n.op = std::move(op);
    n.out = out;
    nodes.push_back(std::move(n));
    return id;
  }

  const PlannerNode& node(NodeId id) const { return nodes.at(id.index); }
  PlannerNode& node(NodeId id) { return nodes.at(id.index); }
};

}  // namespace lazy_collections
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_PLANNER_H
