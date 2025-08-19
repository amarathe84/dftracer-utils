#ifndef __DFTRACER_UTILS_PIPELINE_PIPELINE_H
#define __DFTRACER_UTILS_PIPELINE_PIPELINE_H

#include <vector>
#include <unordered_map>
#include <type_traits>

namespace dftracer {
namespace utils {
namespace pipeline {

// =============================
// Operation Types (zero overhead)
// =============================
struct IdentityOp {
    template <typename Context, typename T>
    auto operator()(const Context& context, const std::vector<T>& input) const {
        return input;
    }
};

template <typename PrevOp, typename Fn, typename InT, typename OutT>
struct MapOp {
    PrevOp prev_op;
    Fn fn;
    
    template <typename Context, typename Input>
    auto operator()(const Context& context, const Input& input) const {
        auto intermediate = prev_op(context, input);
        return context.template map<InT, OutT>(fn, intermediate);
    }
};

template <typename PrevOp, typename Fn, typename InT, typename OutT>
struct ReduceOp {
    PrevOp prev_op;
    Fn fn;
    
    template <typename Context, typename Input>
    auto operator()(const Context& context, const Input& input) const {
        auto intermediate = prev_op(context, input);
        auto result = context.template reduce<InT, OutT>(fn, intermediate);
        return std::vector<OutT>{result};
    }
};

template <typename PrevOp, typename ChunkFn, typename AggFn, typename FinalizeFn,
          typename InT, typename ChunkT, typename AggT, typename OutT>
struct AggregateOp {
    PrevOp prev_op;
    ChunkFn chunk_fn;
    AggFn agg_fn;
    FinalizeFn finalize_fn;
    
    template <typename Context, typename Input>
    auto operator()(const Context& context, const Input& input) const {
        auto intermediate = prev_op(context, input);
        auto result = context.template aggregate<InT, ChunkT, AggT, OutT>(
            chunk_fn, agg_fn, finalize_fn, intermediate);
        return std::vector<OutT>{result};
    }
};

template <typename PrevOp, typename KeyFn, typename AggFn, 
          typename InT, typename KeyT, typename OutT>
struct GroupByOp {
    PrevOp prev_op;
    KeyFn key_fn;
    AggFn agg_fn;
    
    template <typename Context, typename Input>
    auto operator()(const Context& context, const Input& input) const {
        auto intermediate = prev_op(context, input);
        auto result = context.template groupby_aggregate<InT, KeyT, OutT>(
            key_fn, agg_fn, intermediate);
        return std::vector<std::unordered_map<KeyT, OutT>>{result};
    }
};

// =============================
// Type-safe Pipeline (zero overhead)
// =============================
template <typename InputT, typename OutputT, typename Operation = IdentityOp>
class Pipeline {
private:
    Operation operation_;

public:
    explicit Pipeline() : operation_{} {}
    explicit Pipeline(Operation op) : operation_(std::move(op)) {}

    template <typename OutT, typename MapFn>
    auto map(MapFn&& fn) const {
        return Pipeline<InputT, OutT, MapOp<Operation, MapFn, OutputT, OutT>>{
            MapOp<Operation, MapFn, OutputT, OutT>{operation_, std::forward<MapFn>(fn)}
        };
    }

    template <typename OutT, typename ReduceFn>
    auto reduce(ReduceFn&& fn) const {
        return Pipeline<InputT, OutT, ReduceOp<Operation, ReduceFn, OutputT, OutT>>{
            ReduceOp<Operation, ReduceFn, OutputT, OutT>{operation_, std::forward<ReduceFn>(fn)}
        };
    }

    template <typename ChunkT, typename AggT, typename OutT, 
              typename ChunkFn, typename AggFn, typename FinalizeFn>
    auto aggregate(ChunkFn&& chunk_fn, AggFn&& agg_fn, FinalizeFn&& finalize_fn) const {
        return Pipeline<InputT, OutT, AggregateOp<Operation, ChunkFn, AggFn, FinalizeFn, OutputT, ChunkT, AggT, OutT>>{
            AggregateOp<Operation, ChunkFn, AggFn, FinalizeFn, OutputT, ChunkT, AggT, OutT>{
                operation_, 
                std::forward<ChunkFn>(chunk_fn),
                std::forward<AggFn>(agg_fn),
                std::forward<FinalizeFn>(finalize_fn)
            }
        };
    }

    template <typename KeyT, typename OutT, typename KeyFn, typename AggFn>
    auto groupby(KeyFn&& key_fn, AggFn&& agg_fn) const {
        return Pipeline<InputT, std::unordered_map<KeyT, OutT>, 
                       GroupByOp<Operation, KeyFn, AggFn, OutputT, KeyT, OutT>>{
            GroupByOp<Operation, KeyFn, AggFn, OutputT, KeyT, OutT>{
                operation_,
                std::forward<KeyFn>(key_fn), 
                std::forward<AggFn>(agg_fn)
            }
        };
    }

    template <typename Context>
    auto run(const Context& context, const std::vector<InputT>& input) const {
        return operation_(context, input);
    }
};

// =============================
// Pipeline creation helper
// =============================
template <typename T>
auto make_pipeline() {
    return Pipeline<T, T, IdentityOp>{};
}

// =============================
// Functional composition (zero overhead)
// =============================
template <typename... Ops>
struct ComposedOp {
    std::tuple<Ops...> ops;
    
    template <typename Context, typename Input>
    auto operator()(const Context& context, Input&& input) const {
        return apply_ops(context, std::forward<Input>(input), std::index_sequence_for<Ops...>{});
    }
    
private:
    template <typename Context, typename Input, std::size_t... Is>
    auto apply_ops(const Context& context, Input&& input, std::index_sequence<Is...>) const {
        auto result = input;
        ((result = std::get<Is>(ops)(context, result)), ...);
        return result;
    }
};

template <typename... Ops>
auto compose(Ops&&... ops) {
    return ComposedOp<std::decay_t<Ops>...>{std::make_tuple(std::forward<Ops>(ops)...)};
}

} // namespace pipeline
} // namespace utils
} // namespace dftracer

#endif // __DFTRACER_UTILS_PIPELINE_PIPELINE_H