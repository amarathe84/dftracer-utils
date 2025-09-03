#include <dftracer/utils/analyzers/helpers/helpers.h>
#include <dftracer/utils/analyzers/pipeline/trace_reader.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/reader/reader.h>

#include <thread>

namespace dftracer::utils::analyzers {

using namespace dftracer::utils;

struct FileMetadata {
    std::string path;
    size_t size;
};

struct WorkInfo {
    std::string path;
    size_t start;
    size_t end;
};

Pipeline TraceReader::build() {
    Pipeline pipeline;
    std::vector<TaskIndex> metadata_indices;
    return pipeline;
}

}  // namespace dftracer::utils::analyzers
