#include <dftracer/utils/analyzers/helpers/helpers.h>
#include <dftracer/utils/analyzers/pipeline/trace_reader.h>
#include <dftracer/utils/common/logging.h>
#include <dftracer/utils/indexer/indexer.h>
#include <dftracer/utils/pipeline/executors/thread_executor.h>
#include <dftracer/utils/pipeline/tasks/factory.h>
#include <dftracer/utils/reader/reader.h>
#include <dftracer/utils/utils/json.h>

#include <thread>

namespace dftracer::utils::analyzers {

using namespace dftracer::utils;

struct FileMetadata {
    std::string path;
    size_t size;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(path, size);
    }
};

struct WorkInfo {
    std::string path;
    size_t start;
    size_t end;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(path, start, end);
    }
};

Pipeline TraceReader::build() {
    Pipeline pipeline;
    std::vector<TaskIndex> metadata_indices;

    // Query every chunk to do the serial part
    // Set of tasks
    // -

    // Create individual metadata tasks for each trace file (source tasks, no
    // dependencies)
    for (size_t file_idx = 0; file_idx < traces.size(); ++file_idx) {
        const auto& trace_path = traces[file_idx];
        auto metadata_task = Tasks::map<std::string, FileMetadata>(
            [trace_path,
             file_idx](const std::string& input_path) -> FileMetadata {
                auto thread_id = std::this_thread::get_id();
                // Only process when input_path matches this task's assigned
                // file
                if (input_path == trace_path) {
                    DFTRACER_UTILS_LOG_DEBUG(
                        "[Thread %zu] Processing metadata for file [%zu]: %s",
                        std::hash<std::thread::id>{}(thread_id), file_idx,
                        trace_path.c_str());
                    Indexer indexer(trace_path, trace_path + ".idx");
                    indexer.build();
                    auto max_bytes = indexer.get_max_bytes();
                    DFTRACER_UTILS_LOG_DEBUG(
                        "[Thread %zu] Max bytes for %s: %zu",
                        std::hash<std::thread::id>{}(thread_id),
                        trace_path.c_str(), max_bytes);
                    return FileMetadata{trace_path, max_bytes};
                } else {
                    // Return empty metadata for non-matching files (will be
                    // filtered out downstream)
                    return FileMetadata{"", 0};
                }
            });

        auto metadata_idx = pipeline.add_task(std::move(metadata_task));
        metadata_indices.push_back(metadata_idx);
    }

    std::vector<TaskIndex> chunks_indices;

    // Create chunk generation tasks for each file (parallel, depends on
    // respective metadata)
    for (size_t i = 0; i < traces.size(); ++i) {
        auto chunks_task = Tasks::flatmap<FileMetadata, WorkInfo>(
            [&](const FileMetadata& file_info) -> std::vector<WorkInfo> {
                auto thread_id = std::this_thread::get_id();

                // Skip empty metadata (from non-matching files)
                if (file_info.path.empty() || file_info.size == 0) {
                    return {};
                }

                DFTRACER_UTILS_LOG_DEBUG(
                    "[Thread %zu] Creating chunks for file: %s, size: %zu",
                    std::hash<std::thread::id>{}(thread_id),
                    file_info.path.c_str(), file_info.size);
                std::vector<WorkInfo> work_items;
                size_t start = 0;

                while (start < file_info.size) {
                    size_t end = std::min(start + batch_size, file_info.size);
                    work_items.push_back({file_info.path, start, end});
                    start = end;
                }

                DFTRACER_UTILS_LOG_DEBUG(
                    "[Thread %zu] Created %zu work items for %s",
                    std::hash<std::thread::id>{}(thread_id), work_items.size(),
                    file_info.path.c_str());
                return work_items;
            });

        auto chunks_idx = pipeline.add_task(std::move(chunks_task));
        chunks_indices.push_back(chunks_idx);
        pipeline.add_dependency(metadata_indices[i], chunks_idx);
    }

    std::vector<TaskIndex> load_indices;

    // for (size_t i = 0; i < chunks_indices.size(); ++i) {
    //     auto load_task = Tasks::flatmap<WorkInfo, json::OwnedJsonDocuments>(
    //         [](const WorkInfo& work) -> std::vector<json::OwnedJsonDocuments>
    //         {
    //             auto thread_id = std::this_thread::get_id();

    //             // Skip empty work items
    //             if (work.path.empty()) {
    //                 return {};
    //             }

    //             DFTRACER_UTILS_LOG_DEBUG(
    //                 "[Thread %zu] Loading work item: %s [%zu-%zu]",
    //                 std::hash<std::thread::id>{}(thread_id),
    //                 work.path.c_str(), work.start, work.end);
    //             std::vector<json::OwnedJsonDocuments> results;

    //             try {
    //                 Reader reader(work.path, work.path + ".idx");
    //                 auto docs =
    //                 reader.read_json_lines_bytes_owned(work.start,
    //                                                                work.end);
    //                 DFTRACER_UTILS_LOG_INFO(
    //                     "[Thread %zu] Loaded %zu documents from %s",
    //                     std::hash<std::thread::id>{}(thread_id), docs.size(),
    //                     work.path.c_str());
    //                 results.push_back(std::move(docs));
    //             } catch (const std::exception& e) {
    //                 // noop
    //             }

    //             return results;
    //         });

    //     auto load_idx = pipeline.add_task(std::move(load_task));
    //     load_indices.push_back(load_idx);

    //     pipeline.add_dependency(chunks_indices[i], load_idx);
    // }

    std::vector<TaskIndex> parse_indices;

    // for (size_t i = 0; i < load_indices.size(); ++i) {
    //     auto parse_task = Tasks::flatmap<json::OwnedJsonDocuments, Trace>(
    //         [](const json::OwnedJsonDocuments& documents)
    //             -> std::vector<Trace> {
    //             std::vector<Trace> valid_records;

    //             for (const auto& doc : documents) {
    //                 try {
    //                     auto record = parse_trace_owned(doc);

    //                     if (!record.is_valid) {
    //                         continue;
    //                     }

    //                     DFTRACER_UTILS_LOG_INFO("Parsed trace: %s, duration
    //                     %f",
    //                                             record.func_name.c_str(),
    //                                             record.duration);
    //                     valid_records.push_back(std::move(record));
    //                 } catch (const std::exception& e) {
    //                     // noop
    //                 }
    //             }

    //             return valid_records;
    //         });

    //     auto parse_idx = pipeline.add_task(std::move(parse_task));
    //     parse_indices.push_back(parse_idx);

    //     pipeline.add_dependency(load_indices[i], parse_idx);
    // }

    return pipeline;
}

}  // namespace dftracer::utils::analyzers
