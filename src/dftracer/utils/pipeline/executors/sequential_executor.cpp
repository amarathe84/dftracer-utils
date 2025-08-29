#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>

#include <any>
#include <unordered_map>

namespace dftracer::utils {

SequentialExecutor::SequentialExecutor() : Executor(ExecutorType::SEQUENTIAL) {}

std::any SequentialExecutor::execute(const Pipeline& pipeline, std::any input) {
    // gather parameter is ignored in sequential executor (noop)
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

    auto execution_order = pipeline.topological_sort();
    std::unordered_map<TaskIndex, std::any> task_outputs;

    for (TaskIndex task_id : execution_order) {
        std::any task_input;

        // For tasks with no dependencies, use the original input
        if (pipeline.get_task_dependencies(task_id).empty()) {
            task_input = input;
        } else if (pipeline.get_task_dependencies(task_id).size() == 1) {
            // Single dependency - use its output directly
            TaskIndex dependency = pipeline.get_task_dependencies(task_id)[0];
            task_input = task_outputs[dependency];
        } else {
            // Multiple dependencies - combine into vector for CombineTask
            std::vector<std::any> combined_inputs;
            for (TaskIndex dependency :
                 pipeline.get_task_dependencies(task_id)) {
                combined_inputs.push_back(task_outputs[dependency]);
            }
            task_input = combined_inputs;
        }

        task_outputs[task_id] = pipeline.get_task(task_id)->execute(task_input);
    }

    // Return output of the last task
    if (!execution_order.empty()) {
        return task_outputs[execution_order.back()];
    }

    return input;
}

}  // namespace dftracer::utils
