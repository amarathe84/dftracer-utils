#include <dftracer/utils/pipeline/executors/executor_type.h>

#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace dftracer::utils {

std::string executor_type_to_string(ExecutorType type) {
    switch (type) {
        case ExecutorType::SEQUENTIAL:
            return "sequential";
        case ExecutorType::THREAD:
            return "thread";
        case ExecutorType::MPI:
            return "mpi";
        default:
            return "unknown";
    }
}

ExecutorType string_to_executor_type(const std::string &str) {
    std::string lower = str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    if (lower == "sequential") return ExecutorType::SEQUENTIAL;
    if (lower == "thread") return ExecutorType::THREAD;
    if (lower == "mpi") return ExecutorType::MPI;
    throw std::invalid_argument("Unknown executor type: " + str);
}
}  // namespace dftracer::utils
