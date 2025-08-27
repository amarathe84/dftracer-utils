#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/mpi_executor.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>

#include <vector>
#include <cstring>

namespace dftracer::utils {

// Internal serializable buffer for MPI communication
class MPIBuffer {
public:
    // Serialize std::vector<double> to buffer
    static std::vector<uint8_t> serialize_double_vector(const std::vector<double>& vec) {
        size_t data_size = sizeof(size_t) + vec.size() * sizeof(double);
        std::vector<uint8_t> buffer(data_size);
        
        // Write vector size
        size_t vec_size = vec.size();
        std::memcpy(buffer.data(), &vec_size, sizeof(size_t));
        
        // Write vector data
        std::memcpy(buffer.data() + sizeof(size_t), vec.data(), vec.size() * sizeof(double));
        
        return buffer;
    }
    
    // Deserialize buffer to std::vector<double>
    static std::vector<double> deserialize_double_vector(const std::vector<uint8_t>& buffer) {
        if (buffer.size() < sizeof(size_t)) {
            throw PipelineError(PipelineError::VALIDATION_ERROR, "Invalid buffer size");
        }
        
        // Read vector size
        size_t vec_size;
        std::memcpy(&vec_size, buffer.data(), sizeof(size_t));
        
        // Read vector data
        std::vector<double> vec(vec_size);
        std::memcpy(vec.data(), buffer.data() + sizeof(size_t), vec_size * sizeof(double));
        
        return vec;
    }
    
    // Serialize std::vector<int> to buffer
    static std::vector<uint8_t> serialize_int_vector(const std::vector<int>& vec) {
        size_t data_size = sizeof(size_t) + vec.size() * sizeof(int);
        std::vector<uint8_t> buffer(data_size);
        
        // Write vector size
        size_t vec_size = vec.size();
        std::memcpy(buffer.data(), &vec_size, sizeof(size_t));
        
        // Write vector data
        std::memcpy(buffer.data() + sizeof(size_t), vec.data(), vec.size() * sizeof(int));
        
        return buffer;
    }
    
    // Deserialize buffer to std::vector<int>
    static std::vector<int> deserialize_int_vector(const std::vector<uint8_t>& buffer) {
        if (buffer.size() < sizeof(size_t)) {
            throw PipelineError(PipelineError::VALIDATION_ERROR, "Invalid buffer size");
        }
        
        // Read vector size
        size_t vec_size;
        std::memcpy(&vec_size, buffer.data(), sizeof(size_t));
        
        // Read vector data
        std::vector<int> vec(vec_size);
        std::memcpy(vec.data(), buffer.data() + sizeof(size_t), vec_size * sizeof(int));
        
        return vec;
    }
};

MPIExecutor::MPIExecutor() : Executor(ExecutorType::MPI), mpi_(MPIContext::instance()) {
    if (is_master()) {
        DFTRACER_UTILS_LOG_INFO("Pipeline using %d processes", size());
    }
}

std::any MPIExecutor::execute(const Pipeline& pipeline, std::any input, bool gather) {
    // Each rank processes its assigned chunk using sequential execution
    SequentialExecutor sequential_executor;
    std::any local_result = sequential_executor.execute(pipeline, input);
    
    if (gather) {
        return gather_results(local_result);
    } else {
        // No gathering - just return local result
        return local_result;
    }
}

std::any MPIExecutor::gather_results(const std::any& local_result) {
    try {
        // Try to gather as vector<double>
        if (local_result.type() == typeid(std::vector<double>)) {
            auto local_vec = std::any_cast<std::vector<double>>(local_result);
            auto serialized = MPIBuffer::serialize_double_vector(local_vec);
            
            if (is_master()) {
                // Master gathers all results
                std::vector<std::vector<double>> all_results;
                all_results.push_back(local_vec); // Add master's result
                
                // Receive from all other ranks
                for (int rank = 1; rank < size(); ++rank) {
                    int buffer_size;
                    mpi_.recv(&buffer_size, 1, MPI_INT, rank, 0);
                    
                    std::vector<uint8_t> buffer(buffer_size);
                    mpi_.recv(buffer.data(), buffer_size, MPI_BYTE, rank, 1);
                    
                    auto rank_result = MPIBuffer::deserialize_double_vector(buffer);
                    all_results.push_back(rank_result);
                }
                
                // Combine all results into single vector
                std::vector<double> combined_result;
                for (const auto& result : all_results) {
                    combined_result.insert(combined_result.end(), result.begin(), result.end());
                }
                
                return std::any(combined_result);
            } else {
                // Workers send their results to master
                int buffer_size = static_cast<int>(serialized.size());
                mpi_.send(&buffer_size, 1, MPI_INT, 0, 0);
                mpi_.send(serialized.data(), buffer_size, MPI_BYTE, 0, 1);
                
                // Workers return their local result
                return local_result;
            }
        }
        
        // Try to gather as vector<int>
        if (local_result.type() == typeid(std::vector<int>)) {
            auto local_vec = std::any_cast<std::vector<int>>(local_result);
            auto serialized = MPIBuffer::serialize_int_vector(local_vec);
            
            if (is_master()) {
                // Master gathers all results
                std::vector<std::vector<int>> all_results;
                all_results.push_back(local_vec); // Add master's result
                
                // Receive from all other ranks
                for (int rank = 1; rank < size(); ++rank) {
                    int buffer_size;
                    mpi_.recv(&buffer_size, 1, MPI_INT, rank, 0);
                    
                    std::vector<uint8_t> buffer(buffer_size);
                    mpi_.recv(buffer.data(), buffer_size, MPI_BYTE, rank, 1);
                    
                    auto rank_result = MPIBuffer::deserialize_int_vector(buffer);
                    all_results.push_back(rank_result);
                }
                
                // Combine all results into single vector
                std::vector<int> combined_result;
                for (const auto& result : all_results) {
                    combined_result.insert(combined_result.end(), result.begin(), result.end());
                }
                
                return std::any(combined_result);
            } else {
                // Workers send their results to master
                int buffer_size = static_cast<int>(serialized.size());
                mpi_.send(&buffer_size, 1, MPI_INT, 0, 0);
                mpi_.send(serialized.data(), buffer_size, MPI_BYTE, 0, 1);
                
                // Workers return their local result
                return local_result;
            }
        }
        
        // Unsupported type - just return local result
        DFTRACER_UTILS_LOG_INFO("Unsupported type for gathering: %s", local_result.type().name());
        return local_result;
        
    } catch (const std::bad_any_cast& e) {
        DFTRACER_UTILS_LOG_INFO("Failed to cast result for gathering");
        return local_result;
    }
}

}  // namespace dftracer::utils