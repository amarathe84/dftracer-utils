#ifndef DFTRACER_UTILS_TASKS_FACTORY_H
#define DFTRACER_UTILS_TASKS_FACTORY_H

#include <dftracer/utils/pipeline/tasks/op/distinct.h>
#include <dftracer/utils/pipeline/tasks/op/filter.h>
#include <dftracer/utils/pipeline/tasks/op/flatmap.h>
#include <dftracer/utils/pipeline/tasks/op/groupby.h>
#include <dftracer/utils/pipeline/tasks/op/map.h>
#include <dftracer/utils/pipeline/tasks/op/reduce.h>
#include <dftracer/utils/pipeline/tasks/op/skip.h>
#include <dftracer/utils/pipeline/tasks/op/sort.h>
#include <dftracer/utils/pipeline/tasks/op/take.h>

#include <limits>

namespace dftracer::utils {

class Tasks {
   public:
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

    template <typename I, typename O, typename F>
    static std::unique_ptr<MapTask<I, O, F>> map(F func) {
        return std::make_unique<MapTask<I, O, F>>(std::move(func));
    }

    template <typename T, typename F>
    static std::unique_ptr<FilterTask<T, F>> filter(F predicate) {
        return std::make_unique<FilterTask<T, F>>(std::move(predicate));
    }

    template <typename I, typename O, typename F>
    static std::unique_ptr<FlatMapTask<I, O, F>> flatmap(F func) {
        return std::make_unique<FlatMapTask<I, O, F>>(std::move(func));
    }

    template <typename T, typename F>
    static std::unique_ptr<SortTask<T, F>> sort(F comparator) {
        return std::make_unique<SortTask<T, F>>(std::move(comparator));
    }

    template <typename T>
    static std::unique_ptr<DefaultSortTask<T>> sort() {
        return std::make_unique<DefaultSortTask<T>>(std::less<T>{});
    }

    template <typename T, typename K, typename F>
    static std::unique_ptr<GroupByTask<T, K, F>> groupby(F key_extractor) {
        return std::make_unique<GroupByTask<T, K, F>>(std::move(key_extractor));
    }

    template <typename T, typename K, typename F>
    static std::unique_ptr<FastGroupByTask<T, K, F>> fast_groupby(
        F key_extractor) {
        return std::make_unique<FastGroupByTask<T, K, F>>(
            std::move(key_extractor));
    }

    template <typename T>
    static std::unique_ptr<TakeTask<T>> take(size_t count) {
        return std::make_unique<TakeTask<T>>(count);
    }

    template <typename T>
    static std::unique_ptr<LimitTask<T>> limit(size_t count) {
        return std::make_unique<LimitTask<T>>(count);
    }

    template <typename T>
    static std::unique_ptr<SkipTask<T>> skip(size_t count) {
        return std::make_unique<SkipTask<T>>(count);
    }

    template <typename T>
    static std::unique_ptr<DropTask<T>> drop(size_t count) {
        return std::make_unique<DropTask<T>>(count);
    }

    template <typename T>
    static std::unique_ptr<DistinctTask<T>> distinct() {
        return std::make_unique<DistinctTask<T>>();
    }
};

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_TASKS_FACTORY_H
