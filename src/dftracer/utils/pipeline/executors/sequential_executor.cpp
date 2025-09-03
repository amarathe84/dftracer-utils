#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/scheduler/sequential_scheduler.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>

#include <any>
#include <unordered_map>

namespace dftracer::utils {

SequentialExecutor::SequentialExecutor() : Executor(ExecutorType::SEQUENTIAL) {}

std::any SequentialExecutor::execute(const Pipeline& pipeline, std::any input) {
    SequentialScheduler scheduler;
    return scheduler.execute(pipeline, input);
}

}  // namespace dftracer::utils
