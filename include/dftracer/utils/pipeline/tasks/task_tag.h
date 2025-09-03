#ifndef DFTRACER_UTILS_PIPELINE_TASKS_TASK_TAG_H
#define DFTRACER_UTILS_PIPELINE_TASKS_TASK_TAG_H

#include <dftracer/utils/common/typedefs.h>

#include <any>
#include <functional>
#include <memory>
#include <typeindex>

namespace dftracer::utils {

template <typename T>
struct Input {
    T value;
    explicit Input(T val) : value(std::move(val)) {}
};

struct DependsOn {
    TaskIndex id;
    explicit DependsOn(TaskIndex task_id) : id(task_id) {}
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_TASK_TAG_H
