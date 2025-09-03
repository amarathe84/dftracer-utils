#include <dftracer/utils/pipeline/executors/scheduler/sequential_scheduler.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/tasks/task_context.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/common/logging.h>

// Forward declaration from task_context.cpp
namespace dftracer::utils {
    void set_current_scheduler(SchedulerInterface* scheduler);
}

namespace dftracer::utils {

SequentialScheduler::SequentialScheduler() : current_pipeline_(nullptr) {}

void SequentialScheduler::set_pipeline(const Pipeline* pipeline) {
    current_pipeline_ = pipeline;
}

std::any SequentialScheduler::execute_pipeline(const Pipeline& pipeline, std::any input) {
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
    
    // Set current pipeline for context
    set_pipeline(&pipeline);
    
    // Set this scheduler as the current scheduler for this thread
    set_current_scheduler(this);
    
    // Clear previous state
    task_outputs_.clear();
    while (!task_queue_.empty()) {
        task_queue_.pop();
    }
    
    // Execute pipeline tasks in topological order
    auto execution_order = pipeline.topological_sort();
    Pipeline* mutable_pipeline = const_cast<Pipeline*>(&pipeline);
    
    // Pre-create contexts for all tasks
    std::vector<TaskContext> task_contexts;
    for (TaskIndex task_id : execution_order) {
        task_contexts.push_back(mutable_pipeline->create_context(task_id));
    }
    
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

        // Setup context and execute
        if (task->needs_context()) {
            task->setup_context(&task_contexts[i]);
        }
        
        task_outputs_[task_id] = task->execute(task_input);
        
        // Process any dynamically emitted tasks after each main task
        process_queued_tasks();
    }
    
    // Find the terminal tasks (those that no other tasks depend on)
    std::vector<TaskIndex> terminal_tasks;
    for (TaskIndex i = 0; i < static_cast<TaskIndex>(pipeline.size()); ++i) {
        if (pipeline.get_task_dependencies(i).empty()) {
            terminal_tasks.push_back(i);
        }
    }
    
    // Return output of the last terminal task
    if (!terminal_tasks.empty()) {
        return task_outputs_[terminal_tasks.back()];
    }
    
    return input;
}

void SequentialScheduler::submit(TaskIndex task_id, std::any input,
                                std::function<void(std::any)> completion_callback) {
    Task* task_ptr = current_pipeline_ ? current_pipeline_->get_task(task_id) : nullptr;
    submit(task_id, task_ptr, std::move(input), std::move(completion_callback));
}

void SequentialScheduler::submit(TaskIndex task_id, Task* task_ptr, std::any input,
                                std::function<void(std::any)> completion_callback) {
    // For sequential execution, we just queue the task
    task_queue_.emplace(task_id, task_ptr, std::move(input), std::move(completion_callback));
    DFTRACER_UTILS_LOG_DEBUG("SequentialScheduler: Queued task %d", task_id);
}

void SequentialScheduler::process_queued_tasks() {
    Pipeline* mutable_pipeline = const_cast<Pipeline*>(current_pipeline_);
    
    while (!task_queue_.empty()) {
        TaskItem task = std::move(task_queue_.front());
        task_queue_.pop();
        
        try {
            std::any result;
            
            if (task.task_ptr) {
                
                if (task.task_ptr->needs_context()) {
                    // Setup context for all tasks that need it
                    TaskContext task_context(mutable_pipeline, task.task_id);
                    task.task_ptr->setup_context(&task_context);
                    
                    DFTRACER_UTILS_LOG_INFO("SequentialScheduler: Executing task %d with context, input type %s", 
                                           task.task_id, task.input.type().name());
                    result = task.task_ptr->execute(task.input);
                } else {
                    // Tasks that don't need context
                    DFTRACER_UTILS_LOG_INFO("SequentialScheduler: Executing task %d without context, input type %s", 
                                           task.task_id, task.input.type().name());
                    result = task.task_ptr->execute(task.input);
                }
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