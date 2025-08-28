#include <dftracer/utils/analyzers/constants.h>

#include <dftracer/utils/analyzers/helpers/helpers.h>

#include <limits>
#include <vector>
#include <algorithm>
#include <dftracer/utils/analyzers/trace.h>

using namespace dftracer::utils::analyzers;
using namespace dftracer::utils::analyzers::constants;

static const std::vector<double> SIZE_BINS = {-std::numeric_limits<double>::infinity(),
                                       4 * KiB,
                                       16 * KiB,
                                       64 * KiB,
                                       256 * KiB,
                                       1 * MiB,
                                       4 * MiB,
                                       16 * MiB,
                                       64 * MiB,
                                       256 * MiB,
                                       1 * GiB,
                                       4 * GiB,
                                       std::numeric_limits<double>::infinity()};

static const std::vector<std::string> SIZE_BIN_SUFFIXES = {
    "0_4kib",       "4kib_16kib",  "16kib_64kib", "64kib_256kib",
    "256kib_1mib",  "1mib_4mib",   "4mib_16mib",  "16mib_64mib",
    "64mib_256mib", "256mib_1gib", "1gib_4gib",   "4gib_plus"};

static const std::string SIZE_BIN_PREFIX = "size_bin_";

static const std::vector<std::string> SIZE_BIN_LABELS = {
    "<4 KiB",           "4 KiB - 16 KiB",  "16 KiB - 64 KiB",
    "64 KiB - 256 KiB", "256 KiB - 1 MiB", "1 MiB - 4 MiB",
    "4 MiB - 16 MiB",   "16 MiB - 64 MiB", "64 MiB - 256 MiB",
    "256 MiB - 1 GiB",  "1 GiB - 4 GiB",   ">4 GiB"};

static const std::vector<std::string> SIZE_BIN_NAMES = {
    "<4 kiB", "4 KiB",  "16 KiB", "64 KiB",  "256 KiB", "1 MiB",
    "4 MiB",  "16 MiB", "64 MiB", "256 MiB", "1 GiB",   ">4 GiB"};



std::size_t get_num_size_bins() {
  return SIZE_BINS.size() - 1;
}

std::size_t get_size_bin_index(std::uint64_t size) {
    double size_double = static_cast<double>(size);

    auto it = std::upper_bound(SIZE_BINS.begin(),
                               SIZE_BINS.end(), size_double);
    std::size_t bin_index = static_cast<std::size_t>(std::distance(SIZE_BINS.begin(), it) - 1);

    // Adjust to match Python's bin placement (shift one bin earlier)
    if (bin_index > 0) {
        bin_index = bin_index - 1;
    }

    bin_index =
        std::max(static_cast<std::size_t>(0),
                 std::min(bin_index,
                           SIZE_BIN_SUFFIXES.size()) -
                 1);

    return bin_index;
}

void set_size_bins(Trace& trace) {
    // Initialize all bins as -1 first to mimic NaN
    for (const auto& suffix : SIZE_BIN_SUFFIXES) {
        std::string bin_name = SIZE_BIN_PREFIX + suffix;
        trace.bin_fields[bin_name] = -1;
    }

    if (trace.size >= 0) {
        size_t bin_index =
            static_cast<size_t>(get_size_bin_index(trace.size));
        std::string matching_bin = SIZE_BIN_PREFIX +
                                   SIZE_BIN_SUFFIXES[bin_index];
        trace.bin_fields[matching_bin] = 1;
    }
}

