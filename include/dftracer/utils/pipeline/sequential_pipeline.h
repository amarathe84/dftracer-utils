#ifndef DFTRACER_UTILS_PIPELINE_SEQUENTIAL_PIPELINE_H
#define DFTRACER_UTILS_PIPELINE_SEQUENTIAL_PIPELINE_H

#include <dftracer/utils/pipeline/pipeline.h>

namespace dftracer::utils {

class SequentialPipeline : public Pipeline {
   public:
    SequentialPipeline() = default;
    ~SequentialPipeline() override = default;

    // Move-only class
    SequentialPipeline(const SequentialPipeline&) = delete;
    SequentialPipeline& operator=(const SequentialPipeline&) = delete;
    SequentialPipeline(SequentialPipeline&&) = default;
    SequentialPipeline& operator=(SequentialPipeline&&) = default;

    std::any execute(std::any in) override;

   private:
    std::any execute_sequential_internal(std::any in);
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_SEQUENTIAL_PIPELINE_H