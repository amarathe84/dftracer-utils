#ifndef DFTRACER_UTILS_PIPELINE_TASKS_ERROR_H
#define DFTRACER_UTILS_PIPELINE_TASKS_ERROR_H

#include <stdexcept>
#include <string>

namespace dftracer::utils {

class TaskError : public std::runtime_error {
   public:
    enum Type {
        TYPE_MISMATCH_ERROR,
        VALIDATION_ERROR,
        EXECUTION_ERROR,
        INITIALIZATION_ERROR,
        UNKNOWN_ERROR,
    };

    TaskError(Type type, const std::string &message)
        : std::runtime_error(format_message(type, message)), type_(type) {}

    inline Type get_type() const { return type_; }

   private:
    Type type_;

    static std::string format_message(Type type, const std::string &message);
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_ERROR_H
