#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_SORT_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_SORT_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

#include <algorithm>
#include <functional>
#include <vector>

namespace dftracer::utils {

template <typename T, typename F = std::function<bool(const T&, const T&)>>
class SortTask : public TypedTask<std::vector<T>, std::vector<T>> {
   private:
    F comparator;

   public:
    SortTask(F comp)
        : TypedTask<std::vector<T>, std::vector<T>>(TaskType::SORT),
          comparator(std::move(comp)) {}

   protected:
    inline std::vector<T> apply(std::vector<T> in) override {
        if (!this->validate(in))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        // Sort in-place for efficiency
        std::sort(in.begin(), in.end(), comparator);
        return in;
    }
};

// Ascending sort
template <typename T>
using DefaultSortTask = SortTask<T, std::less<T>>;

namespace stream_ops {

template <typename F>
struct Sort {
    F comparator;
    explicit Sort(F comp) : comparator(std::move(comp)) {}
};

struct DefaultSort {};

}  // namespace stream_ops

namespace ops {

template <typename F>
stream_ops::Sort<F> sort(F comparator) {
    return stream_ops::Sort<F>(std::move(comparator));
}

inline stream_ops::DefaultSort sort() { return stream_ops::DefaultSort{}; }

}  // namespace ops

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_SORT_H
