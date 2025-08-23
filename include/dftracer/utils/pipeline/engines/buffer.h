#ifndef __DFTRACER_UTILS_PIPELINE_ENGINES_BUFFER_H
#define __DFTRACER_UTILS_PIPELINE_ENGINES_BUFFER_H

#include <cstddef>
#include <vector>

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

template <class T>
inline ConstBuffer to_const_buffer(const std::vector<T>& v) {
  return ConstBuffer{static_cast<const void*>(v.data()), v.size(), sizeof(T),
                     0};
}

template <class T>
inline MutBuffer to_mut_buffer(std::vector<T>& v) {
  return MutBuffer{static_cast<void*>(v.data()), v.size(), sizeof(T), 0};
}
}  // namespace engine
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_ENGINES_BUFFER_H
