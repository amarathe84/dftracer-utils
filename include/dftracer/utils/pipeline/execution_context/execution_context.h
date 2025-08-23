#ifndef __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_EXECUTION_CONTEXT_H
#define __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_EXECUTION_CONTEXT_H

#include <cstddef>
#include <functional>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace context {

/**
 * @brief Base class for execution contexts.
 *
 * An execution context describes how to execute work locally (e.g., sequentially
 * or on a thread pool) and optionally how to coordinate across multiple
 * distributed ranks (e.g., under MPI). Concrete subclasses implement the
 * scheduling and collective primitives, while algorithms are implemented
 * externally in terms of these primitives.
 */
class ExecutionContext {
 public:
  /// Type of the task passed to parallel_for.
  using ForTask = std::function<void(std::size_t)>;

  virtual ~ExecutionContext() = default;

  /**
   * @brief Number of workers available in this context.
   *
   * For sequential contexts this returns 1; for thread pools it returns the
   * configured number of threads. For distributed contexts this returns the
   * number of local threads, not the total MPI ranks.
   */
  virtual std::size_t concurrency() const noexcept = 0;

  /**
   * @brief Execute a simple parallel for-loop over the range [0, n).
   *
   * Implementations must call @p task exactly once for each index i in
   * ascending order or in a way that is deterministic with respect to index
   * addressed writes (i.e., each iteration writes to a unique output slot).
   *
   * @param n    The number of iterations to execute.
   * @param task A functor taking the iteration index.
   */
  virtual void parallel_for(std::size_t n, const ForTask& task) = 0;

  /**
   * @brief Whether this context spans multiple MPI ranks.
   *
   * Returns false for purely local contexts.
   */
  virtual bool is_distributed() const noexcept { return false; }

  /**
   * @brief The rank of this context within the distributed communicator.
   *
   * Returns 0 for non-distributed contexts.
   */
  virtual std::size_t rank() const noexcept { return 0; }

  /**
   * @brief The size of the distributed communicator.
   *
   * Returns 1 for non-distributed contexts.
   */
  virtual std::size_t size() const noexcept { return 1; }

  /**
   * @brief Synchronize all ranks in distributed contexts.
   *
   * This is a no-op in non-distributed contexts. Use this to ensure all
   * previous work has completed before proceeding.
   */
  virtual void barrier() {}

  /**
   * @brief Reduction operations supported by all_reduce and scan.
   */
  enum class ReduceOp { SUM, MIN, MAX, BITOR, BITAND };

  /**
   * @brief Perform an in-place all-reduce on a buffer.
   *
   * Applies the specified operation to combine each element across all ranks.
   * The @p buf must contain @p count elements, each of size @p elem_size bytes.
   *
   * @param buf       Pointer to the buffer to reduce. Results are written
   *                  back into this buffer.
   * @param count     Number of elements to reduce.
   * @param elem_size Size of each element in bytes.
   * @param op        Reduction operation (e.g., SUM, MIN, MAX).
   */
  virtual void all_reduce(void* buf, std::size_t count, std::size_t elem_size,
                          ReduceOp op) {}

  /**
   * @brief Gather equal-sized values from all ranks.
   *
   * Each rank contributes @p send_count elements of size @p elem_size bytes.
   * The implementation concatenates all contributions into @p recv_buf in
   * rank order. In non-distributed contexts this simply copies the input.
   *
   * @param send_buf   Pointer to the elements contributed by this rank.
   * @param send_count Number of elements contributed by this rank.
   * @param elem_size  Size of each element in bytes.
   * @param recv_buf   Destination buffer for the gathered elements.
   */
  virtual void all_gather(const void* send_buf, std::size_t send_count,
                          std::size_t elem_size,
                          std::vector<std::byte>& recv_buf) {}

  /**
   * @brief Exchange variable-sized values between all ranks.
   *
   * Each rank sends @p send_counts[r] elements (each of size @p elem_size
   * bytes) to rank r. The implementation fills @p recv_buf with the
   * concatenated received elements and records the per-rank counts in
   * @p recv_counts.
   *
   * @param send_buf     Pointer to the send buffer containing the packed
   *                     elements. The buffer should be laid out as the
   *                     concatenation of all per-rank sub-buffers.
   * @param send_counts  Array of counts of elements to send to each rank.
   * @param elem_size    Size of each element in bytes.
   * @param recv_buf     Destination for the received elements.
   * @param recv_counts  Output array of counts of elements received from each
   *                     rank.
   */
  virtual void all_to_allv(const void* send_buf, const std::size_t* send_counts,
                           std::size_t elem_size,
                           std::vector<std::byte>& recv_buf,
                           std::vector<std::size_t>& recv_counts) {}
};

}  // namespace context
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_EXECUTION_CONTEXT_EXECUTION_CONTEXT_H
