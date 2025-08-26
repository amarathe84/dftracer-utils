#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_MAP_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_MAP_H

#include <dftracer/utils/pipeline/tasks/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

namespace dftracer::utils {

template <typename I, typename O, typename F = std::function<O(I&)>>
class MapTask : public Task<I, O> {
   private:
    F func;

   public:
    MapTask(F func)
        : Task<I, O>(TaskType::MAP, typeid(I), typeid(O)),
          func(std::move(func)) {}

   protected:
    inline O apply(I in) override {
        if (validate(in))
            throw TaskError(TaskError::TYPE_MISMATCH_ERROR,
                            "Input type validation failed");
        return func(in);
    }
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_MAP_H
