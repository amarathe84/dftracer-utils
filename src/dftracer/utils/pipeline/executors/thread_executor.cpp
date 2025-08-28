#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/common/logging.h>

#include <any>
#include <future>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dftracer::utils {

ThreadExecutor::ThreadExecutor() : Executor(ExecutorType::THREAD), max_threads_(std::thread::hardware_concurrency()) {}

ThreadExecutor::ThreadExecutor(size_t max_threads) : Executor(ExecutorType::THREAD), max_threads_(max_threads > 0 ? max_threads : std::thread::hardware_concurrency()) {
    if (max_threads_ == 0) {
        max_threads_ = 2; // Fallback to 2 threads if hardware_concurrency is not well-defined
    }

    DFTRACER_UTILS_LOG_INFO("ThreadExecutor initialized with max_threads = %zu", max_threads_);
}

std::any ThreadExecutor::execute(const Pipeline& pipeline, std::any input,
                                 bool /* gather */) {
    // gather parameter is ignored in thread executor (noop)
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
    
    std::vector<std::vector<TaskIndex>> dependency_layers;
    std::unordered_map<TaskIndex, int> task_levels;
    
    for (TaskIndex task_id : execution_order) {
        int max_dep_level = -1;
        for (TaskIndex dep : pipeline.get_task_dependencies(task_id)) {
            max_dep_level = std::max(max_dep_level, task_levels[dep]);
        }
        task_levels[task_id] = max_dep_level + 1;
        
        if (dependency_layers.size() <= static_cast<size_t>(task_levels[task_id])) {
            dependency_layers.resize(task_levels[task_id] + 1);
        }
        dependency_layers[task_levels[task_id]].push_back(task_id);
    }
    
    // Execute tasks layer by layer with thread pool
    for (const auto& layer : dependency_layers) {
        // Process layer in batches
        for (size_t batch_start = 0; batch_start < layer.size(); batch_start += max_threads_) {
            size_t batch_end = std::min(batch_start + max_threads_, layer.size());
            
            // Launch tasks in current batch (up to max_threads_ tasks)
            std::unordered_map<TaskIndex, std::future<std::any>> batch_futures;
            for (size_t i = batch_start; i < batch_end; ++i) {
                TaskIndex task_id = layer[i];
                batch_futures[task_id] = std::async(
                    std::launch::async,
                    [&pipeline, task_id, input, &task_outputs]() {
                        std::any task_input;

                        if (pipeline.get_task_dependencies(task_id).empty()) {
                            task_input = input;
                        } else if (pipeline.get_task_dependencies(task_id).size() == 1) {
                            TaskIndex dependency = pipeline.get_task_dependencies(task_id)[0];
                            task_input = task_outputs.at(dependency);
                        } else {
                            std::vector<std::any> combined_inputs;
                            for (TaskIndex dependency : pipeline.get_task_dependencies(task_id)) {
                                combined_inputs.push_back(task_outputs.at(dependency));
                            }
                            task_input = combined_inputs;
                        }

                        return pipeline.get_task(task_id)->execute(task_input);
                    });
            }
            
            // Wait for current batch to complete before launching next batch
            for (auto& [task_id, future] : batch_futures) {
                task_outputs[task_id] = future.get();
            }
        }
    }

    if (!execution_order.empty()) {
        return task_outputs[execution_order.back()];
    }

    return input;
}

}  // namespace dftracer::utils
