#include <dftracer/utils/pipeline/mpi_pipeline.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/common/logging.h>

#include <iostream>
#include <sstream>
#include <algorithm>
#include <cstring>

namespace dftracer::utils {

MPIPipeline::MPIPipeline() : mpi_(MPI::instance()) {
    if (is_master()) {
      DFTRACER_UTILS_LOG_INFO("Pipeline using %d processes", get_size());
    }
}

std::any MPIPipeline::execute(std::any in) {
    if (nodes_.empty()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR, "Pipeline is empty");
    }
    
    if (!validate_types()) {
        throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR, "Pipeline type validation failed");
    }
    
    if (has_cycles()) {
        throw PipelineError(PipelineError::VALIDATION_ERROR, "Pipeline contains cycles");
    }
    
    distribute_tasks();
    
    // Broadcast input data to all processes
    std::vector<uint8_t> serialized_input = broadcast_input(in);
    std::any distributed_input = deserialize_any(serialized_input);
    
    // Each rank executes its assigned tasks
    execute_local_tasks(distributed_input);

    gather_results();
    
    // Return final result (only meaningful on master rank)
    if (is_master()) {
        return get_final_result();
    } else {
        return std::any{}; // Workers return empty result
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
        DFTRACER_UTILS_LOG_INFO("Distributed %d tasks across %d processes", topo_order.size(), size());
        DFTRACER_UTILS_LOG_INFO("Rank %d assigned %d tasks", rank(), local_tasks_.size());
    }
}

void MPIPipeline::execute_local_tasks(const std::any& input) {
    // Execute tasks in topological order, but only those assigned to this rank
    std::vector<TaskIndex> topo_order = topological_sort();
    
    for (TaskIndex task_id : topo_order) {
        if (local_tasks_.find(task_id) == local_tasks_.end()) {
            continue; // Not assigned to this rank
        }
        
        // Wait for all dependencies to complete
        wait_for_dependencies(task_id);
        
        // Execute the task
        std::any task_input = input; // For now, all tasks get the same input

        // If task has dependencies, we would need to use their outputs
        // For simplicity, this implementation assumes independent tasks
        
        auto start = std::chrono::high_resolution_clock::now();
        std::any result = nodes_[task_id]->execute(task_input);
        auto end = std::chrono::high_resolution_clock::now();
        
        // Store result locally
        task_outputs_[task_id] = result;
        task_completed_[task_id] = true;
        
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        DFTRACER_UTILS_LOG_INFO("Rank %d completed task %d in %dms", rank(), task_id, duration.count());

        // Serialize and store result for communication
        serialized_outputs_[task_id] = serialize_any(result);
    }
}

std::vector<uint8_t> MPIPipeline::broadcast_input(const std::any& input) {
    std::vector<uint8_t> serialized_data;
    
    if (is_master()) {
        // Master serializes the input
        serialized_data = serialize_any(input);
    }
    
    // Use MPI wrapper to broadcast data
    return mpi_.broadcast_vector(serialized_data, 0);
}

void MPIPipeline::gather_results() {
    // For each task, the rank that executed it sends results to master
    for (const auto& [task_id, assigned_rank] : task_assignments_) {
        if (assigned_rank == rank() && !is_master()) {
            // Worker sends result to master
            auto& serialized_result = serialized_outputs_[task_id];
            mpi_.send_vector(serialized_result, 0, task_id);
            
        } else if (is_master() && assigned_rank != 0) {
            // Master receives result from worker
            std::vector<uint8_t> serialized_result = mpi_.recv_vector(assigned_rank, task_id);
            
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
            oss.write(reinterpret_cast<const char*>(vec.data()), size * sizeof(double));
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
            oss.write(reinterpret_cast<const char*>(vec.data()), size * sizeof(int));
        } else {
            throw PipelineError(PipelineError::VALIDATION_ERROR, "Unsupported type for MPI serialization");
        }
    } catch (const std::bad_any_cast& e) {
        throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR, "Failed to serialize data for MPI communication");
    }
    
    std::string str = oss.str();
    return std::vector<uint8_t>(str.begin(), str.end());
}

std::any MPIPipeline::deserialize_any(const std::vector<uint8_t>& data) {
    std::istringstream iss(std::string(data.begin(), data.end()), std::ios::binary);
    
    // For this simple implementation, we need to detect the type
    // This is a simplified version - a real implementation would include type information
    
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
        throw PipelineError(PipelineError::VALIDATION_ERROR, "No task outputs available");
    }
    
    std::vector<TaskIndex> topo_order = topological_sort();
    TaskIndex last_task = topo_order.back();
    
    auto it = task_outputs_.find(last_task);
    if (it != task_outputs_.end()) {
        return it->second;
    }
    
    throw PipelineError(PipelineError::VALIDATION_ERROR, "Final task result not found");
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

void MPIPipeline::wait_for_dependencies(TaskIndex task_id) {
    // For this simple implementation, we use barriers for synchronization
    // In a more sophisticated implementation, you'd have targeted synchronization
    
    if (!dependents_[task_id].empty()) {
        // This task has dependencies, so we need to synchronize
        mpi_.barrier();
    }
}

}  // namespace dftracer::utils
