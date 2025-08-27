#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/error.h>

#include <any>
#include <thread>
#include <future>
#include <vector>
#include <unordered_map>

namespace dftracer::utils {

ThreadExecutor::ThreadExecutor() : Executor(ExecutorType::THREAD) {}

std::any ThreadExecutor::execute(const Pipeline& pipeline, std::any input, bool gather) {
    // gather parameter is ignored in thread executor (noop)
    if (pipeline.empty()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR, "Pipeline is empty");
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
    std::unordered_map<TaskIndex, std::future<std::any>> task_futures;

    for (TaskIndex task_id : execution_order) {
        bool deps_ready = true;
        for (TaskIndex dep : pipeline.get_task_dependencies(task_id)) {
            if (task_outputs.find(dep) == task_outputs.end()) {
                deps_ready = false;
                break;
            }
        }

        if (deps_ready) {
            task_futures[task_id] = std::async(std::launch::async, [&pipeline, task_id, input, &task_outputs]() {
                std::any task_input;
                
                if (pipeline.get_task_dependencies(task_id).empty()) {
                    task_input = input;
                } else if (pipeline.get_task_dependencies(task_id).size() == 1) {
                    TaskIndex dependency = pipeline.get_task_dependencies(task_id)[0];
                    task_input = task_outputs[dependency];
                } else {
                    std::vector<std::any> combined_inputs;
                    for (TaskIndex dependency : pipeline.get_task_dependencies(task_id)) {
                        combined_inputs.push_back(task_outputs[dependency]);
                    }
                    task_input = combined_inputs;
                }
                
                return pipeline.get_task(task_id)->execute(task_input);
            });
        }
    }

    for (auto& [task_id, future] : task_futures) {
        task_outputs[task_id] = future.get();
    }

    if (!execution_order.empty()) {
        return task_outputs[execution_order.back()];
    }

    return input;
}

}  // namespace dftracer::utils
