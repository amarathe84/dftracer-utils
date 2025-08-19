#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_SEQUENTIAL_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_SEQUENTIAL_H

#include <vector>
#include <unordered_map>
#include <algorithm>
#include <numeric>

#include <dftracer/utils/pipeline/execution_context/execution_context.h>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace execution_context {

class SequentialContext : public ExecutionContext<SequentialContext> {
public:
    template <typename InT, typename OutT, typename MapFn>
    std::vector<OutT> map_impl(MapFn&& fn, const std::vector<InT>& input) const {
        std::vector<OutT> result;
        result.reserve(input.size());
        std::transform(input.begin(), input.end(), std::back_inserter(result), fn);
        return result;
    }

    template <typename InT, typename OutT, typename ReduceFn>
    OutT reduce_impl(ReduceFn&& fn, const std::vector<InT>& input) const {
        return fn(input);
    }

    template <typename InT, typename ChunkT, typename AggT, typename OutT, 
              typename ChunkFn, typename AggFn, typename FinalizeFn>
    OutT aggregate_impl(ChunkFn&& chunk_fn, AggFn&& agg_fn, FinalizeFn&& finalize_fn,
                       const std::vector<InT>& input) const {
        // Chunk phase
        ChunkT chunk_result = chunk_fn(input);
        
        // Aggregate phase (single chunk in sequential)
        std::vector<ChunkT> chunks = {chunk_result};
        AggT agg_result = agg_fn(chunks);
        
        // Finalize phase
        return finalize_fn(agg_result);
    }

    template <typename InT, typename KeyT, typename OutT, typename KeyFn, typename AggFn>
    std::unordered_map<KeyT, OutT> groupby_aggregate_impl(KeyFn&& key_fn, AggFn&& agg_fn,
                                                         const std::vector<InT>& input) const {
        // Group by key
        std::unordered_map<KeyT, std::vector<InT>> groups;
        for (const auto& item : input) {
            KeyT key = key_fn(item);
            groups[key].push_back(item);
        }
        
        // Aggregate each group
        return agg_fn(groups);
    }
};

} // namespace execution_context
} // namespace pipeline
} // namespace utils
} // namespace dftracer

#endif // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_SEQUENTIAL_H
