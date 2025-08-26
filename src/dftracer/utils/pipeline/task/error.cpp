#include <dftracer/utils/pipeline/tasks/error.h>

namespace dftracer::utils {

std::string TaskError::format_message(Type type, const std::string &message) {
    std::string prefix;
    switch (type) {
        case TYPE_MISMATCH_ERROR:
            prefix = "[TYPE_MISMATCH]";
            break;
        case VALIDATION_ERROR:
            prefix = "[VALIDATION]";
            break;
        case EXECUTION_ERROR:
            prefix = "[EXECUTION]";
            break;
        case INITIALIZATION_ERROR:
            prefix = "[INITIALIZATION]";
            break;
        case UNKNOWN_ERROR:
            prefix = "[UNKNOWN]";
            break;
    }
    return prefix + " " + message;
}

}  // namespace dftracer::utils