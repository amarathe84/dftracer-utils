#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_H

#include <vector>
#include <unordered_map>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace execution_context {

template <typename Context>
class ExecutionContext {
public:
    template <typename InT, typename OutT, typename MapFn>
    auto map(MapFn&& fn, const std::vector<InT>& input) const -> std::vector<OutT> {
        return static_cast<const Context*>(this)->template map_impl<InT, OutT>(
            std::forward<MapFn>(fn), input);
    }

    template <typename InT, typename OutT, typename ReduceFn>
    auto reduce(ReduceFn&& fn, const std::vector<InT>& input) const -> OutT {
        return static_cast<const Context*>(this)->template reduce_impl<InT, OutT>(
            std::forward<ReduceFn>(fn), input);
    }

    template <typename InT, typename ChunkT, typename AggT, typename OutT, 
              typename ChunkFn, typename AggFn, typename FinalizeFn>
    auto aggregate(ChunkFn&& chunk_fn, AggFn&& agg_fn, FinalizeFn&& finalize_fn, 
                   const std::vector<InT>& input) const -> OutT {
        return static_cast<const Context*>(this)->template aggregate_impl<InT, ChunkT, AggT, OutT>(
            std::forward<ChunkFn>(chunk_fn), std::forward<AggFn>(agg_fn), 
            std::forward<FinalizeFn>(finalize_fn), input);
    }

    template <typename InT, typename KeyT, typename OutT, typename KeyFn, typename AggFn>
    auto groupby_aggregate(KeyFn&& key_fn, AggFn&& agg_fn, 
                          const std::vector<InT>& input) const -> std::unordered_map<KeyT, OutT> {
        return static_cast<const Context*>(this)->template groupby_aggregate_impl<InT, KeyT, OutT>(
            std::forward<KeyFn>(key_fn), std::forward<AggFn>(agg_fn), input);
    }
};

} // namespace execution_context
} // namespace pipeline
} // namespace utils
} // namespace dftracer

#endif // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_H
