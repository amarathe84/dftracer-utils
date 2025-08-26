#ifndef DFTRACER_UTILS_PIPELINE_THREAD_PIPELINE_H
#define DFTRACER_UTILS_PIPELINE_THREAD_PIPELINE_H

#include <dftracer/utils/pipeline/pipeline.h>
#include <future>
#include <vector>

namespace dftracer::utils {

class ThreadPipeline : public Pipeline {
public:
    ThreadPipeline() = default;
    ~ThreadPipeline() override = default;

    // Move-only class
    ThreadPipeline(const ThreadPipeline&) = delete;
    ThreadPipeline& operator=(const ThreadPipeline&) = delete;
    ThreadPipeline(ThreadPipeline&&) = default;
    ThreadPipeline& operator=(ThreadPipeline&&) = default;

    std::any execute(std::any in) override;

private:
    std::any execute_parallel_internal(std::any in);
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_THREAD_PIPELINE_H