#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_DISTINCT_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_DISTINCT_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

#include <algorithm>
#include <unordered_set>
#include <vector>

namespace dftracer::utils {

template <typename T>
class DistinctTask : public TypedTask<std::vector<T>, std::vector<T>> {
   public:
    explicit DistinctTask()
        : TypedTask<std::vector<T>, std::vector<T>>(TaskType::DISTINCT) {}

   protected:
    inline std::vector<T> apply(std::vector<T> input) override {
        if (!this->validate(input))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        std::unordered_set<T> seen;
        std::vector<T> result;
        result.reserve(input.size());  // Reserve space (upper bound)

        for (const auto& element : input) {
            if (seen.find(element) == seen.end()) {
                seen.insert(element);
                result.push_back(element);
            }
        }

        return result;
    }
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_DISTINCT_H