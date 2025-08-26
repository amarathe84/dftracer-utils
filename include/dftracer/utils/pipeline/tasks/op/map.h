#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_MAP_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_MAP_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

#include <functional>
#include <vector>

namespace dftracer::utils {

template <typename I, typename O, typename F = std::function<O(const I&)>>
class MapTask : public TypedTask<std::vector<I>, std::vector<O>> {
   private:
    F func;

   public:
    MapTask(F f)
        : TypedTask<std::vector<I>, std::vector<O>>(TaskType::MAP),
          func(std::move(f)) {}

   protected:
    inline std::vector<O> apply(std::vector<I> in) override {
        if (!this->validate(in))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        std::vector<O> result;
        result.reserve(in.size());

        for (const auto& element : in) {
            result.push_back(func(element));
        }

        return result;
    }
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_MAP_H
