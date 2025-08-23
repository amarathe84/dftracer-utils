#ifndef __DFTRACER_UTILS_PIPELINE_OPERATOR_REPARTITION_H
#define __DFTRACER_UTILS_PIPELINE_OPERATOR_REPARTITION_H

#include <dftracer/utils/pipeline/operators/operator.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <typeindex>
#include <vector>

namespace dftracer {
namespace utils {
namespace pipeline {
namespace operators {
enum class RepartitionStrategy : uint8_t {
  SizeString,
  NumPartitions,
  Hash,
};

template <typename T>
class RepartitionOperator : public Operator {
  RepartitionStrategy strategy_;

 public:
  RepartitionOperator(Op op, RepartitionStrategy strategy) : Operator(op), strategy_(strategy) {}

  std::type_index input_type() const override {
    return std::type_index(typeid(std::vector<T>));
  }

  std::type_index output_type() const override {
    return std::type_index(typeid(std::vector<std::vector<T>>));
  }

  RepartitionStrategy strategy() const { return strategy_; }
};

template <typename T, typename HashFunc>
class HashRepartitionOperator : public RepartitionOperator<T> {
 private:
  HashFunc hash_func_;

 public:
  HashRepartitionOperator(HashFunc hash_func)
      : RepartitionOperator<T>(Op::REPARTITION_BY_HASH, RepartitionStrategy::Hash),
        hash_func_(hash_func) {}

  const HashFunc& hash_function() const { return hash_func_; }
};

template <typename T>
class NumPartitionsRepartitionOperator : public RepartitionOperator<T> {
 private:
  size_t num_partitions_;

 public:
  NumPartitionsRepartitionOperator(size_t num_partitions)
      : RepartitionOperator<T>(Op::REPARTITION_BY_NUM_PARTITIONS, RepartitionStrategy::NumPartitions),
        num_partitions_(num_partitions) {}

  size_t num_partitions() const { return num_partitions_; }
};

template <typename T>
class SizeStringRepartitionOperator : public RepartitionOperator<T> {
 private:
  size_t target_bytes_;

 public:
  SizeStringRepartitionOperator(const std::string& size_str)
      : RepartitionOperator<T>(Op::REPARTITION_BY_SIZE, RepartitionStrategy::SizeString),
        target_bytes_(parse_size_string(size_str)) {}

  size_t target_bytes() const { return target_bytes_; }

 private:
  size_t parse_size_string(const std::string& size_str) {
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
};
}  // namespace operators
}  // namespace pipeline
}  // namespace utils
}  // namespace dftracer

#endif  // __DFTRACER_UTILS_PIPELINE_OPERATOR_REPARTITION_H
