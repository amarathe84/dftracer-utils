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
        if (!this->validate(in)) {
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");
        }

        std::vector<O> result;
        result.reserve(in.size());

        for (const auto& element : in) {
            result.push_back(func(element));
        }

        return result;
    }
};

namespace stream_ops {

template <typename F>
struct Map {
    F func;
    explicit Map(F f) : func(std::move(f)) {}
};

}  // namespace stream_ops

namespace ops {

template <typename F>
stream_ops::Map<F> map(F func) {
    return stream_ops::Map<F>(std::move(func));
}

}  // namespace ops

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_MAP_H
