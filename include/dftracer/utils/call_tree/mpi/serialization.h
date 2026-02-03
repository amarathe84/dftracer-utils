#ifndef DFTRACER_UTILS_CALL_TREE_MPI_SERIALIZATION_H
#define DFTRACER_UTILS_CALL_TREE_MPI_SERIALIZATION_H

/**
 * @file serialization.h
 * @brief Utility functions for serialization of MPI data
 */

#include <cstdint>
#include <string>
#include <vector>

namespace dftracer::utils::call_tree {
namespace serialization {

// Write primitives
void write_uint32(std::vector<char>& buffer, std::uint32_t value);
void write_uint64(std::vector<char>& buffer, std::uint64_t value);
void write_int(std::vector<char>& buffer, int value);
void write_string(std::vector<char>& buffer, const std::string& str);

// Read primitives
std::uint32_t read_uint32(const char* data, size_t& offset);
std::uint64_t read_uint64(const char* data, size_t& offset);
int read_int(const char* data, size_t& offset);
std::string read_string(const char* data, size_t& offset);

}  // namespace serialization
}  // namespace dftracer::utils::call_tree

#endif  // DFTRACER_UTILS_CALL_TREE_MPI_SERIALIZATION_H
