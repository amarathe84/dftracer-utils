#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_EXECUTOR_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_EXECUTOR_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/executors/executor.h>
#include <dftracer/utils/utils/mpi.h>

#include <any>
#include <string>
#include <chrono>
#include <memory>
#include <sstream>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>
#include <set>
#include <optional>

namespace dftracer::utils {

class MPIExecutor : public Executor {
public:
    MPIExecutor();
    ~MPIExecutor() override = default;
    MPIExecutor(const MPIExecutor&) = delete;
    MPIExecutor& operator=(const MPIExecutor&) = delete;
    MPIExecutor(MPIExecutor&&) = default;
    MPIExecutor& operator=(MPIExecutor&&) = default;

    std::any execute(const Pipeline& pipeline, std::any input, bool gather = true) override;
    
    inline int rank() const { return mpi_.rank(); }
    inline int size() const { return mpi_.size(); }
    inline bool is_master() const { return mpi_.rank() == 0; }

private:
    MPIContext& mpi_;
    
    // Helper method for gathering results from all ranks
    std::any gather_results(const std::any& local_result);
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_EXECUTOR_H
