#include <dftracer/utils/pipeline/sequential_pipeline.h>

namespace dftracer::utils {

std::any SequentialPipeline::execute(std::any in) {
    if (!validate_types()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline type validation failed");
    }

    if (has_cycles()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline contains cycles");
    }

    return execute_sequential_internal(std::move(in));
}

std::any SequentialPipeline::execute_sequential_internal(std::any in) {
    auto execution_order = topological_sort();
    task_outputs_.clear();

    std::any current_input = std::move(in);

    for (TaskIndex task_id : execution_order) {
        // For tasks with no dependencies, use the original input
        if (dependents_[task_id].empty()) {
            task_outputs_[task_id] = nodes_[task_id]->execute(current_input);
        } else {
            // For tasks with dependencies, use output from the last dependency
            // TODO: More sophisticated input resolution for multiple
            // dependencies
            TaskIndex dependency = dependents_[task_id][0];
            task_outputs_[task_id] =
                nodes_[task_id]->execute(task_outputs_[dependency]);
        }
    }

    // Return output of the last task
    if (!execution_order.empty()) {
        return task_outputs_[execution_order.back()];
    }

    return current_input;
}

}  // namespace dftracer::utils