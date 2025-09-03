#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>
#include <dftracer/utils/pipeline/executors/scheduler/sequential_scheduler.h>

#include <any>
#include <unordered_map>

namespace dftracer::utils {

SequentialExecutor::SequentialExecutor() : Executor(ExecutorType::SEQUENTIAL) {}

std::any SequentialExecutor::execute(const Pipeline& pipeline, std::any input) {
    // Use SequentialScheduler to handle both static and dynamic task execution
    SequentialScheduler scheduler;
    
    // Set this scheduler as the current scheduler for this thread
    set_current_scheduler(&scheduler);
    
    try {
        // Execute pipeline using the sequential scheduler
        std::any result = scheduler.execute_pipeline(pipeline, input);
        
        // Clean up scheduler reference
        set_current_scheduler(nullptr);
        
        return result;
    } catch (...) {
        // Clean up scheduler reference on exception
        set_current_scheduler(nullptr);
        throw;
    }
}

}  // namespace dftracer::utils
