#ifndef DFTRACER_UTILS_PIPELINE_TASKS_TASK_RESULT_H
#define DFTRACER_UTILS_PIPELINE_TASKS_TASK_RESULT_H

#include <dftracer/utils/common/typedefs.h>

#include <functional>
#include <future>
#include <memory>

namespace dftracer::utils {

class TaskContext;
class ExecutorContext;

template <typename O>
class TaskResult {
   public:
    TaskIndex id;
    std::future<O> future;

    TaskResult(const TaskResult&) = delete;
    TaskResult& operator=(const TaskResult&) = delete;

    TaskResult(TaskResult&& other)
        : id(other.id),
          future(std::move(other.future)),
          context_(other.context_) {
        other.context_ = nullptr;
    }

    O get() { 
        auto result = future.get(); 
        if (context_) {
            context_->release_user_ref(id);
            context_ = nullptr;
        }
        return result;
    }

    TaskResult& operator=(TaskResult&& other) {
        if (this != &other) {
            if (context_) context_->release_user_ref(id);
            id = other.id;
            future = std::move(other.future);
            context_ = other.context_;
            other.context_ = nullptr;
        }
        return *this;
    }

    ~TaskResult() {
        if (context_) context_->release_user_ref(id);
    }

   private:
    ExecutorContext* context_;

    TaskResult(TaskIndex task_id, std::future<O> task_future,
               ExecutorContext* ctx)
        : id(task_id), future(std::move(task_future)), context_(ctx) {
        if (context_) context_->increment_user_ref(id);
    }

    TaskResult(TaskIndex task_id, std::future<O> task_future)
        : id(task_id), future(std::move(task_future)), context_(nullptr) {
    }

    friend class Pipeline;
    friend class TaskContext;
};
}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_TASK_RESULT_H
