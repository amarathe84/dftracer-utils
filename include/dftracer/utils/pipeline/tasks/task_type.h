#ifndef DFTRACER_UTILS_PIPELINE_TASKS_TASK_TYPE_H
#define DFTRACER_UTILS_PIPELINE_TASKS_TASK_TYPE_H

#include <cstdint>
#include <string>

namespace dftracer::utils {

enum class TaskType : std::uint8_t {
    MAP,
    REDUCE,
    FILTER,
    FLATMAP,
    SORT,
    GROUPBY
};

std::string task_type_to_string(TaskType type);
TaskType string_to_task_type(const std::string &str);

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_TASK_TYPE_H
