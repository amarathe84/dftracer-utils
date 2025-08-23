#ifndef __DFTRACER_UTILS_PIPELINE_ENGINE_BUFFER_H
#define __DFTRACER_UTILS_PIPELINE_ENGINE_BUFFER_H

#include <cstddef>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace engine {
struct ConstBuffer {
  const void* data = nullptr;  // base pointer to the first element
  std::size_t count = 0;       // number of elements
  std::size_t elem_size = 0;   // bytes per element
  std::size_t stride = 0;      // byte step between elements; 0 => elem_size
};

struct MutBuffer {
  void* data = nullptr;       // base pointer to the first element
  std::size_t count = 0;      // number of elements to write
  std::size_t elem_size = 0;  // bytes per element
  std::size_t stride = 0;     // byte step between elements; 0 => elem_size
};
}  // namespace engine
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ENGINE_BUFFER_H
