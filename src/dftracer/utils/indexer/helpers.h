#ifndef DFTRACER_UTILS_INDEXER_HELPERS_H
#define DFTRACER_UTILS_INDEXER_HELPERS_H

#include <ctime>
#include <string>

std::string get_logical_path(const std::string &path);

time_t get_file_modification_time(const std::string &file_path);

std::string calculate_file_sha256(const std::string &file_path);

std::size_t file_size_bytes(const std::string &path);

#endif  // DFTRACER_UTILS_INDEXER_HELPERS_H
