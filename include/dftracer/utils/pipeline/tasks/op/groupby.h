#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_GROUPBY_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_GROUPBY_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>

#include <functional>
#include <map>
#include <unordered_map>
#include <vector>

namespace dftracer::utils {

template <typename T, typename K, typename F = std::function<K(const T&)>>
class GroupByTask
    : public TypedTask<std::vector<T>, std::map<K, std::vector<T>>> {
   private:
    F key_extractor;

   public:
    GroupByTask(F extractor)
        : TypedTask<std::vector<T>, std::map<K, std::vector<T>>>(
              TaskType::GROUPBY),
          key_extractor(std::move(extractor)) {}

   protected:
    inline std::map<K, std::vector<T>> apply(std::vector<T> in) override {
        if (!this->validate(in))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        std::map<K, std::vector<T>> groups;

        for (const auto& element : in) {
            K key = key_extractor(element);
            groups[key].push_back(element);
        }

        return groups;
    }
};

template <typename T, typename K, typename F = std::function<K(const T&)>>
class UnorderedGroupByTask
    : public TypedTask<std::vector<T>, std::unordered_map<K, std::vector<T>>> {
   private:
    F key_extractor;

   public:
    UnorderedGroupByTask(F extractor)
        : TypedTask<std::vector<T>, std::unordered_map<K, std::vector<T>>>(
              TaskType::GROUPBY),
          key_extractor(std::move(extractor)) {}

   protected:
    inline std::unordered_map<K, std::vector<T>> apply(
        std::vector<T> in) override {
        if (!this->validate(in))
            throw PipelineError(PipelineError::TYPE_MISMATCH_ERROR,
                                "Input type validation failed");

        std::unordered_map<K, std::vector<T>> groups;

        for (const auto& element : in) {
            K key = key_extractor(element);
            groups[key].push_back(element);
        }

        return groups;
    }
};

// Convenience aliases
template <typename T, typename K, typename F = std::function<K(const T&)>>
using FastGroupByTask = UnorderedGroupByTask<T, K, F>;

// Stream operations (pipeline stages)
namespace stream_ops {

template <typename F>
struct GroupBy {
    F key_extractor;
    explicit GroupBy(F extractor) : key_extractor(std::move(extractor)) {}
};

template <typename F>
struct FastGroupBy {
    F key_extractor;
    explicit FastGroupBy(F extractor) : key_extractor(std::move(extractor)) {}
};

}  // namespace stream_ops

// Convenient factory functions for operations
namespace ops {

template <typename F>
stream_ops::GroupBy<F> groupby(F key_extractor) {
    return stream_ops::GroupBy<F>(std::move(key_extractor));
}

template <typename F>
stream_ops::FastGroupBy<F> fast_groupby(F key_extractor) {
    return stream_ops::FastGroupBy<F>(std::move(key_extractor));
}

}  // namespace ops

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_GROUPBY_H
