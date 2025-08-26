#ifndef DFTRACER_UTILS_PIPELINE_MPI_PIPELINE_H
#define DFTRACER_UTILS_PIPELINE_MPI_PIPELINE_H

#include <dftracer/utils/pipeline/pipeline.h>
#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/utils/mpi.h>
#include <any>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <chrono>
#include <sstream>
#include <typeindex>

namespace dftracer::utils {
class MPIPipeline : public Pipeline {
private:
    MPI& mpi_;
    
    // Task distribution tracking
    std::unordered_map<TaskIndex, int> task_assignments_;  // Which rank executes which task
    std::unordered_set<TaskIndex> local_tasks_;           // Tasks assigned to this rank
    std::unordered_map<TaskIndex, std::vector<uint8_t>> serialized_outputs_;  // Serialized task outputs

public:
    MPIPipeline();
    ~MPIPipeline() override = default;
    MPIPipeline(const MPIPipeline&) = delete;
    MPIPipeline& operator=(const MPIPipeline&) = delete;
    MPIPipeline(MPIPipeline&&) = default;
    MPIPipeline& operator=(MPIPipeline&&) = default;

    std::any execute(std::any in) override;
    inline int rank() const { return mpi_.rank(); }
    inline int size() const { return mpi_.size(); }
    inline bool is_master() const { return mpi_.rank() == 0; }

private:
    void distribute_tasks();
    void execute_local_tasks(const std::any& input);
    std::vector<uint8_t> broadcast_input(const std::any& input);
    void gather_results();
    std::vector<uint8_t> serialize_any(const std::any& data);
    std::any deserialize_any(const std::vector<uint8_t>& data);
    std::any get_final_result();
    bool can_execute_task(TaskIndex task_id) const;
    void wait_for_dependencies(TaskIndex task_id);
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_MPI_PIPELINE_H
