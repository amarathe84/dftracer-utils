#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_TAKE_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_TAKE_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

#include <algorithm>
#include <vector>

namespace dftracer::utils {

template <typename T>
class TakeTask : public TypedTask<std::vector<T>, std::vector<T>> {
   private:
    size_t count_;

   public:
    explicit TakeTask(size_t count)
        : TypedTask<std::vector<T>, std::vector<T>>(TaskType::TAKE),
          count_(count) {}

   protected:
    inline std::vector<T> apply(std::vector<T> input) override {
        if (!this->validate(input))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        if (count_ >= input.size()) {
            return input;  // Return all elements if count is greater than size
        }

        std::vector<T> result;
        result.reserve(count_);

        auto end_it =
            input.begin() +
            static_cast<typename std::vector<T>::difference_type>(count_);
        std::copy(input.begin(), end_it, std::back_inserter(result));

        return result;
    }
};

// Alias for consistency with common naming conventions
template <typename T>
using LimitTask = TakeTask<T>;

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_TAKE_H