#ifndef __DFTRACER_UTILS_ANALYZERS_ANALYZER_IMPL_H__
#define __DFTRACER_UTILS_ANALYZERS_ANALYZER_IMPL_H__

#include <spdlog/spdlog.h>

#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/json.h>

namespace dftracer {
namespace utils {
namespace analyzers {
namespace helpers {
using namespace dftracer::utils::json;

struct WorkInfo {
  std::string path;
  size_t start;
  size_t end;

  template <typename Archive>
  void serialize(Archive& ar) {
    ar(path, start, end);
  }
};

struct FileMetadata {
  std::string path;
  size_t size;

  template <typename Archive>
  void serialize(Archive& ar) {
    ar(path, size);
  }
};

inline auto get_traces_metadata(const std::vector<std::string>& traces) {
  return from_sequence(traces).map([](const std::string& path) {
    dftracer::utils::indexer::Indexer indexer(path, path + ".idx");
    indexer.build();
    auto max_bytes = indexer.get_max_bytes();
    spdlog::debug("Processing file: {} ({} bytes)", path, max_bytes);
    return FileMetadata{path, max_bytes};
  });
}


inline auto generate_chunks(const std::vector<std::string>& traces, size_t batch_size) {
  return get_traces_metadata(traces).flatmap([batch_size](const FileMetadata& file_info) {
    std::vector<WorkInfo> work_items;
    size_t start = 0;
    size_t end = 0;

    while (start < file_info.size) {
      end = std::min(start + batch_size, file_info.size);
      work_items.push_back({file_info.path, start, end});
      start = end;
    }

    return work_items;
  });
}

inline auto read_traces(const std::vector<std::string>& traces, size_t batch_size) {
  return generate_chunks(traces, batch_size).map_partitions([](const auto& partition) {
    std::vector<JsonDocument> results;

    // spdlog::info("Processing partition on thread: {}", std::hash<std::thread::id>{}(std::this_thread::get_id()));

    for (const auto& work : partition) {
        dftracer::utils::reader::Reader reader(work.path, work.path + ".idx");
        auto lines = reader.read_json_lines_bytes(work.start, work.end);
        results.insert(results.end(), lines.begin(), lines.end());
    }

    return results;
  });
}
}

template<typename Context>
AnalyzerResult Analyzer::analyze_trace(
    Context& ctx,
    const std::vector<std::string>& traces,
    const std::vector<std::string>& view_types,
    const std::vector<std::string>& exclude_characteristics,
    const std::unordered_map<std::string, std::string>& extra_columns
) {
    auto traces_bag = helpers::read_traces(traces, checkpoint_size_)
      // .repartition("16KB")
      .compute(ctx);

    spdlog::info("Traces bag size: {}", traces_bag.size());

    // for (const auto& trace : traces_bag) {
    //     spdlog::info("Processing trace: {}", trace.size());
    //     break;
    // }

    // spdlog::info("Chunks {}", chunks);
    // spdlog::info("Total chunks generated: {}", chunks.size());

    // if (chunks.empty()) {
    //     spdlog::warn("No chunks generated! Check input traces and file sizes.");
    //     return AnalyzerResult{};
    // }

    // for (const auto& chunk : chunks) {
    //     spdlog::info("Processing chunk: {} ({} - {})",
    //                chunk.path, chunk.start, chunk.end);
    //     break;
    // }

    return AnalyzerResult{};
}

} // namespace analyzers
} // namespace utils
} // namespace dftracer

#endif // __DFTRACER_UTILS_ANALYZERS_ANALYZER_IMPL_H__
