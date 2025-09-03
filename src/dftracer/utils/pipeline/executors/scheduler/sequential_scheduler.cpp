#include <dftracer/utils/pipeline/executors/scheduler/sequential_scheduler.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/executor_context.h>
#include <dftracer/utils/common/logging.h>


namespace dftracer::utils {

SequentialScheduler::SequentialScheduler() : current_execution_context_(nullptr) {}


std::any SequentialScheduler::execute(const Pipeline& pipeline, std::any input) {
    if (pipeline.empty()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline is empty");
    }
    if (!pipeline.validate_types()) {
        throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                            "Pipeline type validation failed");
    }
    if (pipeline.has_cycles()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline contains cycles");
    }
    
    
    // Create ExecutorContext to manage runtime state
    ExecutorContext execution_context(&pipeline);
    current_execution_context_ = &execution_context;
    
    // Clear previous state
    task_outputs_.clear();
    while (!task_queue_.empty()) {
        task_queue_.pop();
    }
    
    // Execute pipeline tasks in topological order
    auto execution_order = pipeline.topological_sort();
    
    for (size_t i = 0; i < execution_order.size(); ++i) {
        TaskIndex task_id = execution_order[i];
        std::any task_input;
        auto* task = pipeline.get_task(task_id);

        // Determine input for this task
        if (pipeline.get_task_dependencies(task_id).empty()) {
            task_input = input;
        } else if (pipeline.get_task_dependencies(task_id).size() == 1) {
            TaskIndex dependency = pipeline.get_task_dependencies(task_id)[0];
            task_input = task_outputs_[dependency];
        } else {
            std::vector<std::any> combined_inputs;
            for (TaskIndex dependency : pipeline.get_task_dependencies(task_id)) {
                combined_inputs.push_back(task_outputs_[dependency]);
            }
            task_input = combined_inputs;
        }

        // Setup context for tasks that need it
        if (task->needs_context()) {
            TaskContext task_context(this, current_execution_context_, task_id);
            task->setup_context(&task_context);
        }
        
        task_outputs_[task_id] = task->execute(task_input);
        
        // Process any dynamically emitted tasks after each main task
        process_queued_tasks();
    }
    
    // Find the terminal tasks (those that no other tasks depend on)
    std::vector<TaskIndex> terminal_tasks;
    for (TaskIndex i = 0; i < static_cast<TaskIndex>(pipeline.size()); ++i) {
        if (execution_context.get_task_dependents(i).empty()) {
            terminal_tasks.push_back(i);
        }
    }
    
    // Clean up execution context reference
    current_execution_context_ = nullptr;
    
    // Return output of the last terminal task
    if (!terminal_tasks.empty()) {
        return task_outputs_[terminal_tasks.back()];
    }
    
    return input;
}

void SequentialScheduler::submit(TaskIndex task_id, std::any input,
                                std::function<void(std::any)> completion_callback) {
    // This method should be called with task pointer directly
    Task* task_ptr = nullptr;  // Will be passed via other submit method
    submit(task_id, task_ptr, std::move(input), std::move(completion_callback));
}

void SequentialScheduler::submit(TaskIndex task_id, Task* task_ptr, std::any input,
                                std::function<void(std::any)> completion_callback) {
    // For sequential execution, we just queue the task
    task_queue_.emplace(task_id, task_ptr, std::move(input), std::move(completion_callback));
    DFTRACER_UTILS_LOG_DEBUG("SequentialScheduler: Queued task %d", task_id);
}

void SequentialScheduler::process_queued_tasks() {
    while (!task_queue_.empty()) {
        TaskItem task = std::move(task_queue_.front());
        task_queue_.pop();
        
        try {
            std::any result;
            
            if (task.task_ptr) {
                // Setup context for tasks that need it
                if (task.task_ptr->needs_context()) {
                    TaskContext task_context(this, current_execution_context_, task.task_id);
                    task.task_ptr->setup_context(&task_context);
                }
                
                DFTRACER_UTILS_LOG_INFO("SequentialScheduler: Executing task %d, input type %s", 
                                       task.task_id, task.input.type().name());
                result = task.task_ptr->execute(task.input);
                DFTRACER_UTILS_LOG_DEBUG("SequentialScheduler: Executed task %d", task.task_id);
            } else {
                DFTRACER_UTILS_LOG_WARN("SequentialScheduler: No task pointer for task %d, using input as result", task.task_id);
                result = task.input;
            }
            
            // Store result for dependent tasks
            task_outputs_[task.task_id] = result;
            
            // Call the completion callback
            if (task.completion_callback) {
                task.completion_callback(result);
            }
            
        } catch (const std::exception& e) {
            DFTRACER_UTILS_LOG_ERROR("SequentialScheduler: Exception executing task %d: %s", task.task_id, e.what());
            
            // Still call callback to avoid hanging
            if (task.completion_callback) {
                task.completion_callback(std::any{});
            }
        }
    }
}

} // namespace dftracer::utils