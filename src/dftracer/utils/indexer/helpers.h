#ifndef DFTRACER_UTILS_INDEXER_HELPERS_H
#define DFTRACER_UTILS_INDEXER_HELPERS_H

#include <ctime>
#include <string>

std::string get_logical_path(const std::string &path);
time_t get_file_modification_time(const std::string &file_path);
std::uint64_t calculate_file_hash(const std::string &file_path);
std::uint64_t file_size_bytes(const std::string &path);
bool index_exists_and_valid(const std::string &idx_path);

#endif  // DFTRACER_UTILS_INDEXER_HELPERS_H
