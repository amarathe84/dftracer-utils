#include <cstddef>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace engines {
static inline std::size_t effective_stride(std::size_t stride, std::size_t elem) {
  return stride == 0 ? elem : stride;
}
} // namespace engine
} // namespace pipeline
} // namespace utils
} // namespace dftracer
