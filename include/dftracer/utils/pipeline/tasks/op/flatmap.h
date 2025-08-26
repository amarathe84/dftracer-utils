#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_FLATMAP_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_FLATMAP_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

#include <functional>
#include <vector>

namespace dftracer::utils {

template <typename I, typename O,
          typename F = std::function<std::vector<O>(const I&)>>
class FlatMapTask : public TypedTask<std::vector<I>, std::vector<O>> {
   private:
    F func;

   public:
    FlatMapTask(F f)
        : TypedTask<std::vector<I>, std::vector<O>>(TaskType::FLATMAP),
          func(std::move(f)) {}

   protected:
    inline std::vector<O> apply(std::vector<I> in) override {
        if (!this->validate(in))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        std::vector<O> result;

        for (const auto& element : in) {
            auto sub_result = func(element);
            result.insert(result.end(), sub_result.begin(), sub_result.end());
        }

        return result;
    }
};

namespace stream_ops {

template <typename F>
struct FlatMap {
    F func;
    explicit FlatMap(F f) : func(std::move(f)) {}
};

}  // namespace stream_ops

namespace ops {

template <typename F>
stream_ops::FlatMap<F> flatmap(F func) {
    return stream_ops::FlatMap<F>(std::move(func));
}

}  // namespace ops

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_FLATMAP_H
