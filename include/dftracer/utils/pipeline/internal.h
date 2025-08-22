#ifndef __DFTRACER_UTILS_PIPELINE_INTERNAL_H
#define __DFTRACER_UTILS_PIPELINE_INTERNAL_H

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace internal {

enum class ExecutionStrategy { Sequential, Threaded, MPI };

template <typename F, typename T, typename = void>
struct is_map_function : std::false_type {};

template <typename F, typename T>
struct is_map_function<
    F, T, std::void_t<decltype(std::declval<F>()(std::declval<T>()))>>
    : std::true_type {};

template <typename F, typename T>
using map_result_t = decltype(std::declval<F>()(std::declval<T>()));

template <typename Operation>
struct is_leaf_bag : std::is_same<Operation, void> {};

template <typename T>
struct is_partitioned_data : std::false_type {};

template <typename T>
struct is_partitioned_data<std::vector<T>> : std::true_type {};

inline size_t parse_size_string(const std::string& size_str) {
  if (size_str.empty()) {
    throw std::invalid_argument("Empty size string");
  }

  size_t pos = 0;
  double value;

  try {
    value = std::stod(size_str, &pos);
  } catch (const std::exception& e) {
    throw std::invalid_argument("Invalid numeric value in size string: " +
                                size_str);
  }

  if (value < 0) {
    throw std::invalid_argument("Size cannot be negative");
  }

  std::string unit = size_str.substr(pos);
  std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);

  unit.erase(std::remove_if(unit.begin(), unit.end(), ::isspace), unit.end());

  if (unit == "b" || unit.empty()) return static_cast<size_t>(value);
  if (unit == "kb") return static_cast<size_t>(value * 1024);
  if (unit == "mb") return static_cast<size_t>(value * 1024 * 1024);
  if (unit == "gb") return static_cast<size_t>(value * 1024 * 1024 * 1024);

  throw std::invalid_argument("Unknown size unit: " + unit);
}

}  // namespace internal
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_INTERNAL_H
