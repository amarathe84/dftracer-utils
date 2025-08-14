#ifndef __DFTRACER_UTILS_UTILS_FILE_H
#define __DFTRACER_UTILS_UTILS_FILE_H

#include <ctime>
#include <string>

namespace dftracer::utils {
namespace utils {

/**
 * Get the modification time of a file
 * @param file_path Path to the file
 * @return Modification time as time_t, or 0 if file doesn't exist or error
 * occurred
 */
time_t get_file_modification_time(const std::string &file_path);

}  // namespace utils
}  // namespace dftracer::utils

#endif  // __DFTRACER_UTILS_UTILS_FILE_H