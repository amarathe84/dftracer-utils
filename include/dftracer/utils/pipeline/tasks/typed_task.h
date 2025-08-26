#ifndef DFTRACER_UTILS_PIPELINE_TASKS_TYPED_TASK_H
#define DFTRACER_UTILS_PIPELINE_TASKS_TYPED_TASK_H

#include <dftracer/utils/pipeline/tasks/task.h>

#include <typeindex>

namespace dftracer::utils {

template <typename I, typename O>
class TypedTask : public BaseTask {
   protected:
    TypedTask(TaskType t) : BaseTask(t, typeid(I), typeid(O)) {}

    I get_input(std::any& in) { return std::any_cast<I>(in); }

    O get_output(std::any& out) { return std::any_cast<O>(out); }

    virtual O apply(I in) = 0;

   protected:
    bool validate(I in) { return typeid(in) != this->input_type_; }

   public:
    std::any apply(std::any& in) override final {
        return apply(std::any_cast<I>(in));
    }
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_TYPED_TASK_H
