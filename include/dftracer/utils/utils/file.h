#ifndef DFTRACER_UTILS_UTILS_FILE_H
#define DFTRACER_UTILS_UTILS_FILE_H

#include <ctime>
#include <string>

namespace dftracer::utils::utils {

/**
 * Get the modification time of a file
 * @param file_path Path to the file
 * @return Modification time as time_t, or 0 if file doesn't exist or error
 * occurred
 */
time_t get_file_modification_time(const std::string &file_path);

}  // namespace dftracer

#endif // DFTRACER_UTILS_UTILS_FILE_H
