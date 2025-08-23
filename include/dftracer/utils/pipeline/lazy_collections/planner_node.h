#ifndef __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_PLANNER_NODE_H
#define __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_PLANNER_NODE_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace lazy_collections {
struct NodeId {
  std::uint32_t index = 0;
};

struct OutputLayout {
  std::size_t elem_size = 0;  // sizeof(T) at this node
  bool packed = true;
};

struct PlannerNode {
  std::vector<NodeId> parents;
  std::unique_ptr<operators::Operator> op;
  OutputLayout out;
};
}  // namespace lazy_collections
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_LAZY_COLLECTION_PLANNER_NODE_H
