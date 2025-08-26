#ifndef DFTRACER_UTILS_PIPELINE_STREAM_H
#define DFTRACER_UTILS_PIPELINE_STREAM_H

#include <dftracer/utils/pipeline/builder.h>
#include <dftracer/utils/pipeline/tasks/factory.h>

#include <any>
#include <functional>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace dftracer::utils {

// Forward declarations
template <typename T>
class Stream;

// Stream operations (pipeline stages)
namespace stream_ops {

// Filter operation
template <typename F>
struct Filter {
    F predicate;
    explicit Filter(F pred) : predicate(std::move(pred)) {}
};

// Map operation
template <typename F>
struct Map {
    F func;
    explicit Map(F f) : func(std::move(f)) {}
};

// Reduce operations
struct Sum {};
struct Product {};
template <typename T>
struct Max {
    T initial;
};
template <typename T>
struct Min {
    T initial;
};

// Collection operations
struct Take {
    size_t count;
};
struct Limit {
    size_t count;
};
struct Skip {
    size_t count;
};
struct Drop {
    size_t count;
};
struct Distinct {};

// Execution operations
struct ExecuteSequential {};
struct ExecuteThreaded {};
struct ExecuteMPI {};

}  // namespace stream_ops

// Stream class - wraps PipelineBuilder with pipe operator support
template <typename InputType>
class Stream {
   public:
    explicit Stream(std::any input) : builder_(std::move(input)) {}

    // Move-only semantics
    Stream(const Stream&) = delete;
    Stream& operator=(const Stream&) = delete;
    Stream(Stream&&) = default;
    Stream& operator=(Stream&&) = default;

    // Allow access to builder
    PipelineBuilder<InputType>&& get_builder() && {
        return std::move(builder_);
    }

    // Constructor from moved builder
    explicit Stream(PipelineBuilder<InputType>&& builder)
        : builder_(std::move(builder)) {}

    // Pipe operations
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

// Factory function for creating streams
template <typename T>
Stream<T> stream(const std::vector<T>& data) {
    return Stream<T>(std::any(data));
}

template <typename T>
Stream<T> stream(std::vector<T>&& data) {
    return Stream<T>(std::any(std::move(data)));
}

// Convenient factory functions for operations
namespace ops {

template <typename F>
stream_ops::Filter<F> filter(F predicate) {
    return stream_ops::Filter<F>(std::move(predicate));
}

template <typename F>
stream_ops::Map<F> map(F func) {
    return stream_ops::Map<F>(std::move(func));
}

inline stream_ops::Sum sum() { return stream_ops::Sum{}; }

inline stream_ops::Product product() { return stream_ops::Product{}; }

template <typename T>
stream_ops::Max<T> max(T initial = std::numeric_limits<T>::lowest()) {
    return stream_ops::Max<T>{initial};
}

template <typename T>
stream_ops::Min<T> min(T initial = std::numeric_limits<T>::max()) {
    return stream_ops::Min<T>{initial};
}

inline stream_ops::Take take(size_t count) { return stream_ops::Take{count}; }

inline stream_ops::Limit limit(size_t count) {
    return stream_ops::Limit{count};
}

inline stream_ops::Skip skip(size_t count) { return stream_ops::Skip{count}; }

inline stream_ops::Drop drop(size_t count) { return stream_ops::Drop{count}; }

inline stream_ops::Distinct distinct() { return stream_ops::Distinct{}; }

// Execution operations
inline stream_ops::ExecuteSequential execute_sequential() {
    return stream_ops::ExecuteSequential{};
}

inline stream_ops::ExecuteThreaded execute_threaded() {
    return stream_ops::ExecuteThreaded{};
}

inline stream_ops::ExecuteMPI execute_mpi() { return stream_ops::ExecuteMPI{}; }

}  // namespace ops

}  // namespace dftracer::utils

#endif  // DFTRACER_UTILS_PIPELINE_STREAM_H
