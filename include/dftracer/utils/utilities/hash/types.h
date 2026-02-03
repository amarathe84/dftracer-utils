#ifndef DFTRACER_UTILS_UTILITIES_HASH_TYPES_H
#define DFTRACER_UTILS_UTILITIES_HASH_TYPES_H

#include <cstddef>

namespace dftracer::utils::utilities::hash {
/**
 * @brief Hash algorithm to use for text hashing.
 */
enum class HashAlgorithm {
    XXH3_64,  // XXH3 64-bit hash
    XXH64,    // XXH64 hash
    STD       // std::hash (platform-dependent)
};

/**
 * @brief Hash value for a string/line.
 */
struct Hash {
    std::size_t value = 0;

    Hash() = default;

    explicit Hash(std::size_t v) : value(v) {}

    // Equality for caching support
    bool operator==(const Hash& other) const { return value == other.value; }

    bool operator!=(const Hash& other) const { return !(*this == other); }
};
}  // namespace dftracer::utils::utilities::hash

#endif  // DFTRACER_UTILS_UTILITIES_HASH_TYPES_H
