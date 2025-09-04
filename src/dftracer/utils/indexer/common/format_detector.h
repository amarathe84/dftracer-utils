#ifndef DFTRACER_UTILS_INDEXER_COMMON_FORMAT_DETECTOR_H
#define DFTRACER_UTILS_INDEXER_COMMON_FORMAT_DETECTOR_H

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>

namespace dftracer::utils::gzip_common {

/**
 * Enumeration of supported archive formats
 */
enum class ArchiveFormat {
    GZIP,      // Standard GZIP file
    TAR_GZ,    // TAR.GZ archive (tar files compressed with gzip)
    UNKNOWN    // Unrecognized or unsupported format
};

/**
 * Format detector for archive files
 */
class FormatDetector {
   public:
    /**
     * Detect the format of a file based on its path and content
     * @param file_path Path to the file
     * @return Detected format
     */
    static ArchiveFormat detect_format(const std::string& file_path);

    /**
     * Detect the format by examining file content
     * @param file Open file handle positioned at the beginning
     * @return Detected format
     */
    static ArchiveFormat detect_format_from_content(FILE* file);

    /**
     * Check if a file is a TAR.GZ based on content analysis
     * @param file Open file handle positioned at the beginning
     * @return true if the file is TAR.GZ, false otherwise
     */
    static bool is_tar_gz(FILE* file);

    /**
     * Check if a file is a standard GZIP file
     * @param file Open file handle positioned at the beginning
     * @return true if the file is GZIP, false otherwise
     */
    static bool is_gzip(FILE* file);

   private:
    /**
     * Check for GZIP magic header
     * @param file Open file handle
     * @return true if GZIP magic header is found
     */
    static bool has_gzip_magic(FILE* file);

    /**
     * Check for TAR header after GZIP decompression
     * @param file Open file handle
     * @return true if TAR header is found
     */
    static bool has_tar_header_after_gzip(FILE* file);

    /**
     * Read and validate TAR header
     * @param header 512-byte buffer containing potential TAR header
     * @return true if valid TAR header
     */
    static bool is_valid_tar_header(const unsigned char* header);

    /**
     * Calculate TAR header checksum
     * @param header 512-byte TAR header
     * @return calculated checksum
     */
    static unsigned int calculate_tar_checksum(const unsigned char* header);
};

}  // namespace dftracer::utils::gzip_common

#endif  // DFTRACER_UTILS_INDEXER_COMMON_FORMAT_DETECTOR_H