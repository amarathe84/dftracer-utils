#include <dftracer/utils/pipeline/thread_pipeline.h>
#include <chrono>

namespace dftracer::utils {

std::any ThreadPipeline::execute(std::any in) {
    if (!validate_types()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR, "Pipeline type validation failed");
    }
    
    if (has_cycles()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR, "Pipeline contains cycles");
    }
    
    return execute_parallel_internal(std::move(in));
}

std::any ThreadPipeline::execute_parallel_internal(std::any in) {
    auto execution_order = topological_sort();
    task_outputs_.clear();
    
    // Reset completion status
    for (TaskIndex i = 0; i < nodes_.size(); ++i) {
        task_completed_[i] = false;
    }
    
    std::vector<std::future<void>> futures;
    std::any original_input = in;
    
    // Execute tasks in parallel as dependencies are satisfied
    for (TaskIndex task_id : execution_order) {
        futures.push_back(std::async(std::launch::async, [this, task_id, &original_input]() {
            // Wait for dependencies
            for (TaskIndex dependency : dependents_[task_id]) {
                while (!task_completed_[dependency]) {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
            }
            
            // Execute task
            if (dependents_[task_id].empty()) {
                task_outputs_[task_id] = nodes_[task_id]->execute(original_input);
            } else {
                TaskIndex dependency = dependents_[task_id][0];
                task_outputs_[task_id] = nodes_[task_id]->execute(task_outputs_[dependency]);
            }
            
            task_completed_[task_id] = true;
        }));
    }
    
    // Wait for all tasks to complete
    for (auto& future : futures) {
        future.wait();
    }
    
    // Return output of the last task
    if (!execution_order.empty()) {
        return task_outputs_[execution_order.back()];
    }
    
    return original_input;
}

}  // namespace dftracer::utils