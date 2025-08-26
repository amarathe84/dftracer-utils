#ifndef DFTRACER_UTILS_PIPELINE_STREAM_H
#define DFTRACER_UTILS_PIPELINE_STREAM_H

#include <dftracer/utils/pipeline/builder.h>
#include <dftracer/utils/pipeline/tasks/factory.h>
#include <dftracer/utils/pipeline/tasks/op/distinct.h>
#include <dftracer/utils/pipeline/tasks/op/filter.h>
#include <dftracer/utils/pipeline/tasks/op/flatmap.h>
#include <dftracer/utils/pipeline/tasks/op/groupby.h>
#include <dftracer/utils/pipeline/tasks/op/map.h>
#include <dftracer/utils/pipeline/tasks/op/reduce.h>
#include <dftracer/utils/pipeline/tasks/op/skip.h>
#include <dftracer/utils/pipeline/tasks/op/sort.h>
#include <dftracer/utils/pipeline/tasks/op/take.h>

#include <any>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dftracer::utils {

// Forward declarations
template <typename T>
class Stream;

namespace stream_ops {
// Execution operations
struct ExecuteSequential {};
struct ExecuteThreaded {};
struct ExecuteMPI {};
}  // namespace stream_ops

// Convenient factory functions for operations
namespace ops {
inline stream_ops::ExecuteSequential execute_sequential() {
    return stream_ops::ExecuteSequential{};
}
inline stream_ops::ExecuteThreaded execute_threaded() {
    return stream_ops::ExecuteThreaded{};
}
inline stream_ops::ExecuteMPI execute_mpi() { return stream_ops::ExecuteMPI{}; }
}  // namespace ops

// Wrap PipelineBuilder with pipe operator support
template <typename InputType>
class Stream {
   public:
    explicit Stream(std::any input) : builder_(std::move(input)) {}

    // Move-only semantics
    Stream(const Stream&) = delete;
    Stream& operator=(const Stream&) = delete;
    Stream(Stream&&) = default;
    Stream& operator=(Stream&&) = default;

    PipelineBuilder<InputType>&& get_builder() && {
        return std::move(builder_);
    }

    explicit Stream(PipelineBuilder<InputType>&& builder)
        : builder_(std::move(builder)) {}

    template <typename F>
    Stream<InputType> operator|(stream_ops::Filter<F>&& filter_op) && {
        return Stream<InputType>(
            std::move(builder_).filter(std::move(filter_op.predicate)));
    }

    template <typename F>
    auto operator|(stream_ops::Map<F>&& map_op) && {
        // Deduce output type from function
        using OutputType = std::invoke_result_t<F, InputType>;

        return Stream<OutputType>(std::move(builder_).template map<OutputType>(
            std::move(map_op.func)));
    }

    Stream<InputType> operator|(stream_ops::Sum&&) && {
        static_assert(std::is_arithmetic_v<InputType>,
                      "Sum requires arithmetic type");
        return Stream<InputType>(std::move(builder_).sum());
    }

    Stream<InputType> operator|(stream_ops::Product&&) && {
        static_assert(std::is_arithmetic_v<InputType>,
                      "Product requires arithmetic type");
        return Stream<InputType>(std::move(builder_).product());
    }

    template <typename T>
    Stream<InputType> operator|(stream_ops::Max<T>&& max_op) && {
        return Stream<InputType>(std::move(builder_).max(max_op.initial));
    }

    template <typename T>
    Stream<InputType> operator|(stream_ops::Min<T>&& min_op) && {
        return Stream<InputType>(std::move(builder_).min(min_op.initial));
    }

    Stream<InputType> operator|(stream_ops::Take&& take_op) && {
        return Stream<InputType>(std::move(builder_).take(take_op.count));
    }

    Stream<InputType> operator|(stream_ops::Limit&& limit_op) && {
        return Stream<InputType>(std::move(builder_).limit(limit_op.count));
    }

    Stream<InputType> operator|(stream_ops::Skip&& skip_op) && {
        return Stream<InputType>(std::move(builder_).skip(skip_op.count));
    }

    Stream<InputType> operator|(stream_ops::Drop&& drop_op) && {
        return Stream<InputType>(std::move(builder_).drop(drop_op.count));
    }

    Stream<InputType> operator|(stream_ops::Distinct&&) && {
        return Stream<InputType>(std::move(builder_).distinct());
    }

    template <typename F>
    auto operator|(stream_ops::FlatMap<F>&& flatmap_op) && {
        // FlatMap transforms vector<I> to vector<O>, need to deduce O from
        // function Assuming function type is I -> vector<O>
        using VectorType = std::invoke_result_t<F, InputType>;
        using OutputType = typename VectorType::value_type;

        return Stream<OutputType>(
            std::move(builder_).template flatmap<OutputType>(
                std::move(flatmap_op.func)));
    }

    template <typename F>
    Stream<InputType> operator|(stream_ops::Sort<F>&& sort_op) && {
        return Stream<InputType>(
            std::move(builder_).sort(std::move(sort_op.comparator)));
    }

    Stream<InputType> operator|(stream_ops::DefaultSort&&) && {
        return Stream<InputType>(std::move(builder_).sort());
    }

    template <typename F>
    auto operator|(stream_ops::GroupBy<F>&& groupby_op) && {
        // GroupBy transforms vector<T> to map<K, vector<T>>, need to deduce K
        // from function
        using KeyType = std::invoke_result_t<F, InputType>;
        using OutputType = std::map<KeyType, std::vector<InputType>>;

        return Stream<OutputType>(std::move(builder_).template groupby<KeyType>(
            std::move(groupby_op.key_extractor)));
    }

    template <typename F>
    auto operator|(stream_ops::FastGroupBy<F>&& groupby_op) && {
        // FastGroupBy transforms vector<T> to unordered_map<K, vector<T>>
        using KeyType = std::invoke_result_t<F, InputType>;
        using OutputType = std::unordered_map<KeyType, std::vector<InputType>>;

        return Stream<OutputType>(
            std::move(builder_).template fast_groupby<KeyType>(
                std::move(groupby_op.key_extractor)));
    }

    // Execution operations
    std::any operator|(stream_ops::ExecuteSequential&&) && {
        return std::move(builder_).execute_sequential();
    }

    std::any operator|(stream_ops::ExecuteThreaded&&) && {
        return std::move(builder_).execute_threaded();
    }

    std::any operator|(stream_ops::ExecuteMPI&&) && {
        return std::move(builder_).execute_mpi();
    }

   private:
    PipelineBuilder<InputType> builder_;
    template <typename>
    friend class Stream;
};

// Factory functions
template <typename T>
Stream<T> stream(const std::vector<T>& data) {
    return Stream<T>(std::any(data));
}

template <typename T>
Stream<T> stream(std::vector<T>&& data) {
    return Stream<T>(std::any(std::move(data)));
}

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_STREAM_H
