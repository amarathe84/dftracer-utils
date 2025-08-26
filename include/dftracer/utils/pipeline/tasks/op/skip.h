#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_SKIP_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_SKIP_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

#include <algorithm>
#include <vector>

namespace dftracer::utils {

template <typename T>
class SkipTask : public TypedTask<std::vector<T>, std::vector<T>> {
   private:
    size_t count_;

   public:
    explicit SkipTask(size_t count)
        : TypedTask<std::vector<T>, std::vector<T>>(TaskType::SKIP),
          count_(count) {}

   protected:
    inline std::vector<T> apply(std::vector<T> input) override {
        if (!this->validate(input))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        if (count_ >= input.size()) {
            return std::vector<T>{};  // Return empty vector if skip count
                                      // exceeds size
        }

        std::vector<T> result;
        result.reserve(input.size() - count_);

        auto start_it =
            input.begin() +
            static_cast<typename std::vector<T>::difference_type>(count_);
        std::copy(start_it, input.end(), std::back_inserter(result));

        return result;
    }
};

template <typename T>
using DropTask = SkipTask<T>;

namespace stream_ops {

struct Skip {
    size_t count;
};
struct Drop {
    size_t count;
};

}  // namespace stream_ops

namespace ops {

inline stream_ops::Skip skip(size_t count) { return stream_ops::Skip{count}; }

inline stream_ops::Drop drop(size_t count) { return stream_ops::Drop{count}; }

}  // namespace ops

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_SKIP_H
