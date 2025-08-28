#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/executors/mpi_executor.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/mpi_helpers.h>
#include <dftracer/utils/pipeline/executors/sequential_executor.h>

#include <cstring>
#include <functional>
#include <typeindex>
#include <vector>

namespace dftracer::utils {

namespace {

// A map from type_index to a function that can gather that type.
using Gatherer =
    std::function<std::any(MPIContext&, int, bool, const std::any&)>;
std::unordered_map<std::type_index, Gatherer> gatherers;

// Generic function to gather vectors
template <typename T>
std::any gather_vector_results(MPIContext& mpi, int mpi_size, bool is_master,
                               const std::any& local_result) {
    auto local_vec = std::any_cast<std::vector<T>>(local_result);

    if (is_master) {
        std::vector<std::vector<T>> all_results;
        all_results.reserve(mpi_size);
        all_results.push_back(local_vec);

        for (int rank = 1; rank < mpi_size; ++rank) {
            std::vector<T> rank_result;
            mpi_recv(rank_result, rank, 0, mpi.comm());
            all_results.push_back(std::move(rank_result));
        }

        std::vector<T> combined_result;
        for (const auto& result : all_results) {
            combined_result.insert(combined_result.end(), result.begin(),
                                   result.end());
        }
        return std::any(combined_result);
    } else {
        mpi_send(local_vec, 0, 0, mpi.comm());
        return local_result;
    }
}

// Generic function to gather maps
template <typename K, typename V>
std::any gather_map_results(MPIContext& mpi, int mpi_size, bool is_master,
                            const std::any& local_result) {
    auto local_map = std::any_cast<std::unordered_map<K, V>>(local_result);

    if (is_master) {
        std::vector<std::unordered_map<K, V>> all_results;
        all_results.reserve(mpi_size);
        all_results.push_back(local_map);

        for (int rank = 1; rank < mpi_size; ++rank) {
            std::unordered_map<K, V> rank_result;
            mpi_recv(rank_result, rank, 0, mpi.comm());
            all_results.push_back(std::move(rank_result));
        }

        std::unordered_map<K, V> combined_result;
        for (const auto& result : all_results) {
            combined_result.insert(result.begin(), result.end());
        }
        return std::any(combined_result);
    } else {
        mpi_send(local_map, 0, 0, mpi.comm());
        return local_result;
    }
}

// Helper to register a type for gathering
template <typename T>
void register_vector_gatherer() {
    gatherers[typeid(std::vector<T>)] = gather_vector_results<T>;
}

template <typename K, typename V>
void register_map_gatherer() {
    gatherers[typeid(std::unordered_map<K, V>)] = gather_map_results<K, V>;
}

// Call this once to initialize the gatherers map
void initialize_gatherers() {
    register_vector_gatherer<double>();
    register_vector_gatherer<int>();
    register_vector_gatherer<float>();
    register_vector_gatherer<char>();
    register_vector_gatherer<std::int64_t>();
    register_vector_gatherer<std::uint64_t>();
    register_vector_gatherer<std::size_t>();
    register_map_gatherer<std::string, int>();
    register_map_gatherer<std::string, double>();
}

}  // namespace

MPIExecutor::MPIExecutor()
    : Executor(ExecutorType::MPI), mpi_(MPIContext::instance()) {
    if (is_master()) {
        DFTRACER_UTILS_LOG_INFO("Pipeline using %d processes", size());
    }
    initialize_gatherers();
    register_common_any_serializers();
}

std::any MPIExecutor::execute(const Pipeline& pipeline, std::any input,
                              bool gather) {
    // Each rank processes its assigned chunk using sequential execution
    SequentialExecutor sequential_executor;
    std::any local_result = sequential_executor.execute(pipeline, input);

    DFTRACER_UTILS_LOG_DEBUG("Local result ready");

    if (!gather) {
        return local_result;
    }

    // If gathering is needed, workers send their results to the master.
    // The master will proceed to gather them.
    if (!is_master()) {
        DFTRACER_UTILS_LOG_DEBUG("Sending local result to master");
        mpi_send_any(local_result, 0, 0, mpi_.comm());
        // Workers' job is done, they can return their local result.
        // The master's final result is the one that matters.
        return local_result;
    }

    // Master's gathering logic starts here.
    return gather_results(local_result);
}

std::any MPIExecutor::gather_results(const std::any& local_result) {
    // This function is now only executed by the master.
    try {
        const auto& type = local_result.type();
        auto it = gatherers.find(type);

        if (it != gatherers.end()) {
            // The gatherer function needs to be called with the master's local
            // result, and it will handle receiving from workers.
            return it->second(mpi_, size(), is_master(), local_result);
        }

        DFTRACER_UTILS_LOG_DEBUG("Gathering results");
        // Fallback for non-registered types.
        // The master gathers results from all workers.
        std::vector<std::any> all_results;
        all_results.reserve(size());
        all_results.push_back(local_result);  // Add master's own result

        for (int rank = 1; rank < size(); ++rank) {
            std::any rank_result;
            mpi_recv_any(rank_result, rank, 0, mpi_.comm());
            all_results.push_back(std::move(rank_result));
        }
        return std::any(all_results);

    } catch (const std::bad_any_cast& e) {
        DFTRACER_UTILS_LOG_ERROR("Failed to cast result for gathering: %s",
                                 e.what());
        return local_result;
    }
}

}  // namespace dftracer::utils
