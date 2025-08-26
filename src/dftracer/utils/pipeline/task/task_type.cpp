#include <dftracer/utils/pipeline/tasks/task_type.h>

#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace dftracer::utils {

std::string task_type_to_string(TaskType type) {
    switch (type) {
        case TaskType::MAP:
            return "map";
        case TaskType::REDUCE:
            return "reduce";
        case TaskType::FILTER:
            return "filter";
        default:
            return "unknown";
    }
}

TaskType string_to_task_type(const std::string &str) {
    std::string lower = str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    if (lower == "map") return TaskType::MAP;
    if (lower == "reduce") return TaskType::REDUCE;
    if (lower == "filter") return TaskType::FILTER;
    throw std::invalid_argument("Invalid task type");
}

}  // namespace dftracer::utils
