#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_REDUCE_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_REDUCE_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <vector>

namespace dftracer::utils {

template <typename I, typename O,
          typename F = std::function<O(const O&, const I&)>>
class ReduceTask : public TypedTask<std::vector<I>, O> {
   private:
    F func;
    O initial_value;

   public:
    ReduceTask(F f, O init_val)
        : TypedTask<std::vector<I>, O>(TaskType::REDUCE),
          func(std::move(f)),
          initial_value(std::move(init_val)) {}

   protected:
    inline O apply(std::vector<I> in) override {
        if (!this->validate(in))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        O result = initial_value;

        for (const auto& element : in) {
            result = func(result, element);
        }

        return result;
    }
};

// Common reduce operations
template <typename T>
using SumTask = ReduceTask<T, T, std::plus<T>>;

template <typename T>
using ProductTask = ReduceTask<T, T, std::multiplies<T>>;

template <typename T>
using MaxTask = ReduceTask<T, T, std::function<T(const T&, const T&)>>;

template <typename T>
using MinTask = ReduceTask<T, T, std::function<T(const T&, const T&)>>;

namespace stream_ops {

struct Sum {};
struct Product {};
template <typename T>
struct Max {
    T initial;
};
template <typename T>
struct Min {
    T initial;
};

}  // namespace stream_ops
namespace ops {

inline stream_ops::Sum sum() { return stream_ops::Sum{}; }

inline stream_ops::Product product() { return stream_ops::Product{}; }

template <typename T>
stream_ops::Max<T> max(T initial = std::numeric_limits<T>::lowest()) {
    return stream_ops::Max<T>{initial};
}

template <typename T>
stream_ops::Min<T> min(T initial = std::numeric_limits<T>::max()) {
    return stream_ops::Min<T>{initial};
}

}  // namespace ops

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_REDUCE_H
