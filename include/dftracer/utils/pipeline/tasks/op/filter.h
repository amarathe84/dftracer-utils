#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_FILTER_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_FILTER_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

#include <functional>
#include <vector>

namespace dftracer::utils {

template <typename T, typename F = std::function<bool(const T&)>>
class FilterTask : public TypedTask<std::vector<T>, std::vector<T>> {
   private:
    F predicate;

   public:
    FilterTask(F pred)
        : TypedTask<std::vector<T>, std::vector<T>>(TaskType::FILTER),
          predicate(std::move(pred)) {}

   protected:
    inline std::vector<T> apply(std::vector<T> in) override {
        if (!this->validate(in))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        std::vector<T> result;

        for (const auto& element : in) {
            if (predicate(element)) {
                result.push_back(element);
            }
        }

        return result;
    }
};

namespace stream_ops {

template <typename F>
struct Filter {
    F predicate;
    explicit Filter(F pred) : predicate(std::move(pred)) {}
};

}  // namespace stream_ops

namespace ops {

template <typename F>
stream_ops::Filter<F> filter(F predicate) {
    return stream_ops::Filter<F>(std::move(predicate));
}

}  // namespace ops

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_FILTER_H
