#ifndef DFTRACER_UTILS_PIPELINE_TASKS_OP_GROUPBY_H
#define DFTRACER_UTILS_PIPELINE_TASKS_OP_GROUPBY_H

#include <dftracer/utils/pipeline/error.h>
#include <dftracer/utils/pipeline/tasks/typed_task.h>
#include <vector>
#include <map>
#include <unordered_map>
#include <functional>

namespace dftracer::utils {

template <typename T, typename K, typename F = std::function<K(const T&)>>
class GroupByTask : public TypedTask<std::vector<T>, std::map<K, std::vector<T>>> {
   private:
    F key_extractor;

   public:
    GroupByTask(F extractor)
        : TypedTask<std::vector<T>, std::map<K, std::vector<T>>>(TaskType::GROUPBY),
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

// Fast version using unordered_map for better performance
template <typename T, typename K, typename F = std::function<K(const T&)>>
class FastGroupByTask : public TypedTask<std::vector<T>, std::unordered_map<K, std::vector<T>>> {
   private:
    F key_extractor;

   public:
    FastGroupByTask(F extractor)
        : TypedTask<std::vector<T>, std::unordered_map<K, std::vector<T>>>(TaskType::GROUPBY),
          key_extractor(std::move(extractor)) {}

   protected:
    inline std::unordered_map<K, std::vector<T>> apply(std::vector<T> in) override {
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

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_TASKS_OP_GROUPBY_H