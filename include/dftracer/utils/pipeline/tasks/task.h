#ifndef DFTRACER_UTILS_PIPELINE_TASKS_TASK_H
#define DFTRACER_UTILS_PIPELINE_TASKS_TASK_H

#include <dftracer/utils/pipeline/tasks/task_type.h>

#include <any>
#include <cstdint>
#include <string>
#include <typeindex>

namespace dftracer::utils {

class Task {
   private:
    TaskType type_;
    std::type_index input_type_;
    std::type_index output_type_;

   protected:
    Task(TaskType t, std::type_index input_type, std::type_index output_type)
        : type_(t), input_type_(input_type), output_type_(output_type) {}

   public:
    virtual ~Task() = default;
    virtual std::any execute(std::any& in) = 0;

    TaskType get_type() const { return type_; }
    std::type_index get_input_type() const { return input_type_; }
    std::type_index get_output_type() const { return output_type_; }
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_TASK_H
