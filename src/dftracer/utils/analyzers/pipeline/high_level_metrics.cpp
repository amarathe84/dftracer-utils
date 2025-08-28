#include <dftracer/utils/analyzers/pipeline/high_level_metrics.h>

namespace dftracer::utils::analyzers::pipeline {

Pipeline HLMPipelineGenerator::build(
    const std::vector<std::string>& traces, 
    size_t batch_size,
    const std::vector<std::string>& view_types) {
    
    // Get the trace reading pipeline
    auto trace_pipeline = TraceReader::build_trace_reading_pipeline(traces, batch_size);
    
    // TODO: Add high-level metrics computation tasks
    // For now, just return the trace reading pipeline
    // Future tasks will include:
    // - Timestamp normalization
    // - Hash mapping resolution
    // - Epoch processing
    // - Groupby aggregation for HLM computation
    
    return trace_pipeline;
}

}  // namespace dftracer::utils::analyzers::pipeline
