#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/mpi_pipeline.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>

namespace dftracer::utils {

MPIPipeline::MPIPipeline() : mpi_(MPI::instance()) {
    if (is_master()) {
        DFTRACER_UTILS_LOG_INFO("Pipeline using %d processes", size());
    }
}

std::any MPIPipeline::execute(std::any in) {
    if (nodes_.empty()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline is empty");
    }

    if (!validate_types()) {
        throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                            "Pipeline type validation failed");
    }

    if (has_cycles()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "Pipeline contains cycles");
    }

    distribute_tasks();
    setup_dependency_tracking();

    // Each rank executes its assigned tasks with the same input
    // No broadcast needed - all ranks have the same input data
    execute_local_tasks(in);

    gather_results();

    // Return final result (only meaningful on master rank)
    if (is_master()) {
        return get_final_result();
    } else {
        return std::any{};  // Workers return empty result
    }
}

void MPIPipeline::distribute_tasks() {
    std::vector<TaskIndex> topo_order = topological_sort();
    int next_rank = 0;

    for (TaskIndex task_id : topo_order) {
        task_assignments_[task_id] = next_rank % size();

        if (task_assignments_[task_id] == rank()) {
            local_tasks_.insert(task_id);
        }

        next_rank++;
    }

    if (is_master()) {
        DFTRACER_UTILS_LOG_INFO("Distributed %d tasks across %d processes",
                                topo_order.size(), size());
        DFTRACER_UTILS_LOG_INFO("Rank %d assigned %d tasks", rank(),
                                local_tasks_.size());
    }
}

void MPIPipeline::execute_local_tasks(const std::any& input) {
    // Execute tasks in topological order, but only those assigned to this rank
    std::vector<TaskIndex> topo_order = topological_sort();

    for (TaskIndex task_id : topo_order) {
        if (local_tasks_.find(task_id) == local_tasks_.end()) {
            continue;  // Not assigned to this rank
        }

        // Wait for all dependencies to complete
        wait_for_dependencies(task_id);

        // Execute the task
        std::any task_input = input;  // For now, all tasks get the same input

        // If task has dependencies, we would need to use their outputs
        // For simplicity, this implementation assumes independent tasks

        auto start = std::chrono::high_resolution_clock::now();
        std::any result = nodes_[task_id]->execute(task_input);
        auto end = std::chrono::high_resolution_clock::now();

        // Store result locally
        task_outputs_[task_id] = result;
        task_completed_[task_id] = true;

        auto duration =
            std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        DFTRACER_UTILS_LOG_INFO("Rank %d completed task %d in %dms", rank(),
                                task_id, duration.count());

        // Send completion signals to dependent ranks
        send_completion_signal(task_id);

        // Serialize and store result for communication
        serialized_outputs_[task_id] = serialize_any(result);
    }
}

void MPIPipeline::gather_results() {
    // For each task, the rank that executed it sends results to master
    for (const auto& [task_id, assigned_rank] : task_assignments_) {
        if (assigned_rank == rank() && !is_master()) {
            // Worker sends result to master
            auto& serialized_result = serialized_outputs_[task_id];
            mpi_.send_vector(serialized_result, 0, static_cast<int>(task_id));

        } else if (is_master() && assigned_rank != 0) {
            // Master receives result from worker
            std::vector<uint8_t> serialized_result =
                mpi_.recv_vector(assigned_rank, static_cast<int>(task_id));

            // Deserialize and store result
            task_outputs_[task_id] = deserialize_any(serialized_result);
        }
    }

    // Synchronize all processes
    mpi_.barrier();
}

std::vector<uint8_t> MPIPipeline::serialize_any(const std::any& data) {
    std::ostringstream oss(std::ios::binary);

    try {
        if (data.type() == typeid(std::vector<double>)) {
            auto vec = std::any_cast<std::vector<double>>(data);
            size_t size = vec.size();
            oss.write(reinterpret_cast<const char*>(&size), sizeof(size));
            oss.write(reinterpret_cast<const char*>(vec.data()),
                      size * sizeof(double));
        } else if (data.type() == typeid(double)) {
            auto val = std::any_cast<double>(data);
            oss.write(reinterpret_cast<const char*>(&val), sizeof(val));
        } else if (data.type() == typeid(int)) {
            auto val = std::any_cast<int>(data);
            oss.write(reinterpret_cast<const char*>(&val), sizeof(val));
        } else if (data.type() == typeid(std::vector<int>)) {
            auto vec = std::any_cast<std::vector<int>>(data);
            size_t size = vec.size();
            oss.write(reinterpret_cast<const char*>(&size), sizeof(size));
            oss.write(reinterpret_cast<const char*>(vec.data()),
                      size * sizeof(int));
        } else {
            throw PipelineError(PipelineError::VALIDATION_ERROR,
                                "Unsupported type for MPI serialization");
        }
    } catch (const std::bad_any_cast& e) {
        throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                            "Failed to serialize data for MPI communication");
    }

    std::string str = oss.str();
    return std::vector<uint8_t>(str.begin(), str.end());
}

std::any MPIPipeline::deserialize_any(const std::vector<uint8_t>& data) {
    std::istringstream iss(std::string(data.begin(), data.end()),
                           std::ios::binary);

    // For this simple implementation, we need to detect the type
    // This is a simplified version - a real implementation would include type
    // information

    size_t size;
    iss.read(reinterpret_cast<char*>(&size), sizeof(size));

    // Check if there's more data after the size - indicates vector<double>
    size_t remaining_bytes = data.size() - sizeof(size_t);
    size_t expected_int_bytes = size * sizeof(int);
    size_t expected_double_bytes = size * sizeof(double);

    if (remaining_bytes == expected_double_bytes) {
        // This looks like vector<double>
        std::vector<double> vec(size);
        iss.read(reinterpret_cast<char*>(vec.data()), size * sizeof(double));
        return std::any(vec);
    } else if (remaining_bytes == expected_int_bytes) {
        // This looks like vector<int>
        std::vector<int> vec(size);
        iss.read(reinterpret_cast<char*>(vec.data()), size * sizeof(int));
        return std::any(vec);
    } else if (remaining_bytes == sizeof(double)) {
        // This looks like single double
        double val;
        iss.read(reinterpret_cast<char*>(&val), sizeof(double));
        return std::any(val);
    } else {
        // Default to vector<int> for backward compatibility
        std::vector<int> vec(size);
        iss.read(reinterpret_cast<char*>(vec.data()), size * sizeof(int));
        return std::any(vec);
    }
}

std::any MPIPipeline::get_final_result() {
    if (task_outputs_.empty()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR,
                            "No task outputs available");
    }

    std::vector<TaskIndex> topo_order = topological_sort();
    TaskIndex last_task = topo_order.back();

    auto it = task_outputs_.find(last_task);
    if (it != task_outputs_.end()) {
        return it->second;
    }

    throw PipelineError(PipelineError::VALIDATION_ERROR,
                        "Final task result not found");
}

bool MPIPipeline::can_execute_task(TaskIndex task_id) const {
    for (TaskIndex dep_id : dependents_[task_id]) {
        auto it = task_completed_.find(dep_id);
        if (it == task_completed_.end() || !it->second.load()) {
            return false;
        }
    }
    return true;
}

void MPIPipeline::setup_dependency_tracking() {
    // Build rank-to-rank dependency mappings
    for (TaskIndex task_id = 0; task_id < nodes_.size(); ++task_id) {
        int task_rank = task_assignments_[task_id];

        // Find which ranks this task depends on
        for (TaskIndex dep_task_id : dependents_[task_id]) {
            int dep_rank = task_assignments_[dep_task_id];
            if (dep_rank != task_rank) {
                dependency_ranks_[task_id].push_back(dep_rank);
                dependent_ranks_[dep_task_id].push_back(task_rank);
            }
        }

        // Initialize pending dependencies for this task
        for (int dep_rank : dependency_ranks_[task_id]) {
            pending_dependencies_[task_id].insert(dep_rank);
        }
    }

    if (is_master()) {
        DFTRACER_UTILS_LOG_INFO("Dependency tracking setup complete");
    }
}

void MPIPipeline::send_completion_signal(TaskIndex task_id) {
    // Send completion signals to all ranks that depend on this task
    for (int dependent_rank : dependent_ranks_[task_id]) {
        if (dependent_rank != rank()) {
            // Send a simple completion signal (task_id) to the dependent rank
            int signal = static_cast<int>(task_id);
            mpi_.send(&signal, 1, MPI_INT, dependent_rank, 9999);

            DFTRACER_UTILS_LOG_DEBUG(
                "Rank %d sent completion signal for task %d to rank %d", rank(),
                task_id, dependent_rank);
        }
    }
}

bool MPIPipeline::check_completion_signals(TaskIndex task_id) {
    // Check if we have any pending dependencies for this task
    auto it = pending_dependencies_.find(task_id);
    if (it == pending_dependencies_.end() || it->second.empty()) {
        return true;  // No pending dependencies
    }

    // Non-blocking check for completion signals
    int signal;
    MPI_Status status;

    // Keep checking for signals until no more are available
    while (true) {
        try {
            // Use probe to check if there's a message waiting
            int source = mpi_.probe_any_source(9999, &status);

            // Receive the signal
            mpi_.recv(&signal, 1, MPI_INT, source, 9999, &status);

            static_cast<void>(signal);  // Acknowledge signal received

            // Remove this rank from pending dependencies for the completed task
            auto dep_it = pending_dependencies_.find(task_id);
            if (dep_it != pending_dependencies_.end()) {
                dep_it->second.erase(source);

                DFTRACER_UTILS_LOG_DEBUG(
                    "Rank %d received completion signal from rank %d", rank(),
                    source);
            }

        } catch (const std::exception&) {
            // No more messages available
            break;
        }
    }

    // Check if all dependencies are now satisfied
    return pending_dependencies_[task_id].empty();
}

void MPIPipeline::receive_completion_signals(TaskIndex task_id) {
    // Wait for all pending dependencies to be resolved
    while (!pending_dependencies_[task_id].empty()) {
        int signal;
        MPI_Status status;

        // Blocking receive from any source
        int source = mpi_.probe_any_source(9999, &status);
        mpi_.recv(&signal, 1, MPI_INT, source, 9999, &status);

        static_cast<void>(signal);  // Acknowledge signal received

        // Remove this rank from pending dependencies
        pending_dependencies_[task_id].erase(source);

        DFTRACER_UTILS_LOG_DEBUG(
            "Rank %d received completion signal from rank %d", rank(), source);
    }
}

void MPIPipeline::wait_for_dependencies(TaskIndex task_id) {
    // Sophisticated targeted synchronization
    if (dependents_[task_id].empty()) {
        return;  // No dependencies, can execute immediately
    }

    // Check for local dependencies first (tasks on same rank)
    bool local_deps_ready = true;
    for (TaskIndex dep_task_id : dependents_[task_id]) {
        int dep_rank = task_assignments_[dep_task_id];
        if (dep_rank == rank()) {
            // Local dependency - check if completed
            auto it = task_completed_.find(dep_task_id);
            if (it == task_completed_.end() || !it->second.load()) {
                local_deps_ready = false;
                break;
            }
        }
    }

    if (!local_deps_ready) {
        DFTRACER_UTILS_LOG_ERROR(
            "Local dependencies not ready for task %d on rank %d", task_id,
            rank());
        return;
    }

    // Wait for remote dependencies using targeted synchronization
    if (!pending_dependencies_[task_id].empty()) {
        DFTRACER_UTILS_LOG_INFO(
            "Rank %d waiting for %d remote dependencies for task %d", rank(),
            pending_dependencies_[task_id].size(), task_id);

        receive_completion_signals(task_id);

        DFTRACER_UTILS_LOG_INFO(
            "Rank %d all dependencies satisfied for task %d", rank(), task_id);
    }
}

}  // namespace dftracer::utils
