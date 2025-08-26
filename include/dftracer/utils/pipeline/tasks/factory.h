#ifndef DFTRACER_UTILS_TASKS_FACTORY_H
#define DFTRACER_UTILS_TASKS_FACTORY_H

#include <dftracer/utils/pipeline/tasks/op/filter.h>
#include <dftracer/utils/pipeline/tasks/op/flatmap.h>
#include <dftracer/utils/pipeline/tasks/op/groupby.h>
#include <dftracer/utils/pipeline/tasks/op/map.h>
#include <dftracer/utils/pipeline/tasks/op/reduce.h>
#include <dftracer/utils/pipeline/tasks/op/sort.h>

#include <limits>

namespace dftracer::utils {

class Tasks {
   public:
    // Reduce operations - return unique_ptr for easy pipeline usage
    template <typename T>
    static std::unique_ptr<SumTask<T>> sum() {
        return std::make_unique<SumTask<T>>(std::plus<T>{}, T{});
    }

    template <typename T>
    static std::unique_ptr<ProductTask<T>> product() {
        return std::make_unique<ProductTask<T>>(std::multiplies<T>{}, T{1});
    }

    template <typename T>
    static std::unique_ptr<MaxTask<T>> max(
        T initial = std::numeric_limits<T>::lowest()) {
        return std::make_unique<MaxTask<T>>(
            [](const T& a, const T& b) { return std::max(a, b); }, initial);
    }

    template <typename T>
    static std::unique_ptr<MinTask<T>> min(
        T initial = std::numeric_limits<T>::max()) {
        return std::make_unique<MinTask<T>>(
            [](const T& a, const T& b) { return std::min(a, b); }, initial);
    }

    // Map operations - return unique_ptr for easy pipeline usage
    template <typename I, typename O, typename F>
    static std::unique_ptr<MapTask<I, O, F>> map(F func) {
        return std::make_unique<MapTask<I, O, F>>(std::move(func));
    }

    // Filter operations - return unique_ptr for easy pipeline usage
    template <typename T, typename F>
    static std::unique_ptr<FilterTask<T, F>> filter(F predicate) {
        return std::make_unique<FilterTask<T, F>>(std::move(predicate));
    }

    // FlatMap operations
    template <typename I, typename O, typename F>
    static FlatMapTask<I, O, F> flatmap(F func) {
        return FlatMapTask<I, O, F>(std::move(func));
    }

    // Sort operations
    template <typename T, typename F>
    static SortTask<T, F> sort(F comparator) {
        return SortTask<T, F>(std::move(comparator));
    }

    template <typename T>
    static DefaultSortTask<T> sort() {
        return DefaultSortTask<T>(std::less<T>{});
    }

    // GroupBy operations
    template <typename T, typename K, typename F>
    static GroupByTask<T, K, F> groupby(F key_extractor) {
        return GroupByTask<T, K, F>(std::move(key_extractor));
    }

    template <typename T, typename K, typename F>
    static FastGroupByTask<T, K, F> fast_groupby(F key_extractor) {
        return FastGroupByTask<T, K, F>(std::move(key_extractor));
    }
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_TASKS_FACTORY_H
