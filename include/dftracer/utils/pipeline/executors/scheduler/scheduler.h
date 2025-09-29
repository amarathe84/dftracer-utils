#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_H

#include <dftracer/utils/common/typedefs.h>
#include <dftracer/utils/pipeline/pipeline_output.h>

#include <any>

namespace dftracer::utils {
class Pipeline;

class Scheduler {
   public:
    virtual ~Scheduler() = default;

    virtual void reset() {}
    
    virtual PipelineOutput execute(const Pipeline& pipeline,
                                   const std::any& input) = 0;
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_SCHEDULER_H
