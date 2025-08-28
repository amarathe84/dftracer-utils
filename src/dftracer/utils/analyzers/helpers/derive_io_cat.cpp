#include <dftracer/utils/analyzers/constants.h>
#include <dftracer/utils/analyzers/helpers/helpers.h>

using namespace dftracer::utils::analyzers::constants;

static const std::unordered_map<std::string, std::string> POSIX_IO_CAT_MAPPING =
    {{"read", "read"},       {"pread", "read"},     {"pread64", "read"},
     {"readv", "read"},      {"preadv", "read"},    {"write", "write"},
     {"pwrite", "write"},    {"pwrite64", "write"}, {"writev", "write"},
     {"pwritev", "write"},   {"open", "open"},      {"open64", "open"},
     {"openat", "open"},     {"close", "close"},    {"__xstat64", "stat"},
     {"__lxstat64", "stat"}, {"stat", "stat"},      {"lstat", "stat"},
     {"fstat", "stat"}};

std::string derive_io_cat(const std::string& func_name) {
    if (POSIX_METADATA_FUNCTIONS.find(func_name) !=
        POSIX_METADATA_FUNCTIONS.end()) {
        return "metadata";
    }

    auto it = POSIX_IO_CAT_MAPPING.find(func_name);
    if (it != POSIX_IO_CAT_MAPPING.end()) {
        return it->second;
    }

    return "other";
}
