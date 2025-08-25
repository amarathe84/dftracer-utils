#ifndef DFTRACER_UTILS_ANALYZERS_CONSTANTS_H
#define DFTRACER_UTILS_ANALYZERS_CONSTANTS_H

#include <limits>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dftracer::utils::analyzers::constants {

// Enums
enum class AccessPattern { SEQUENTIAL = 0, RANDOM = 1 };

enum class EventType {
    ATTACH_REASONS,
    COMPUTE_HLM,
    COMPUTE_MAIN_VIEW,
    COMPUTE_METRIC_BOUNDARIES,
    COMPUTE_PERSPECTIVES,
    COMPUTE_VIEW,
    DETECT_CHARACTERISTICS,
    EVALUATE_VIEW,
    READ_TRACES,
    SAVE_VIEWS
};

enum class IOCategory {
    READ = 1,
    WRITE = 2,
    METADATA = 3,
    PCTL = 4,
    IPC = 5,
    OTHER = 6,
    SYNC = 7
};

std::ostream& operator<<(std::ostream& os, const IOCategory& io_cat);

enum class Layer { APP, DATALOADER, NETCDF, PNETCDF, HDF5, MPI, POSIX };

// Column names
constexpr const char* COL_ACC_PAT = "acc_pat";
constexpr const char* COL_APP_NAME = "app_name";
constexpr const char* COL_BEHAVIOR = "behavior";
constexpr const char* COL_CATEGORY = "cat";
constexpr const char* COL_COUNT = "count";
constexpr const char* COL_EPOCH = "epoch";
constexpr const char* COL_FILE_DIR = "file_dir";
constexpr const char* COL_FILE_NAME = "file_name";
constexpr const char* COL_FILE_PATTERN = "file_pattern";
constexpr const char* COL_FUNC_NAME = "func_name";
constexpr const char* COL_HOST_NAME = "host_name";
constexpr const char* COL_IO_CAT = "io_cat";
constexpr const char* COL_NODE_NAME = "node_name";
constexpr const char* COL_PROC_ID = "proc_id";
constexpr const char* COL_PROC_NAME = "proc_name";
constexpr const char* COL_RANK = "rank";
constexpr const char* COL_SIZE = "size";
constexpr const char* COL_TIME = "time";
constexpr const char* COL_TIME_OVERALL = "time_overall";
constexpr const char* COL_TIME_RANGE = "time_range";
constexpr const char* COL_TIME_START = "time_start";
constexpr const char* COL_TIME_END = "time_end";

// View types
extern const std::vector<std::pair<std::string, std::string>>
    LOGICAL_VIEW_TYPES;
extern const std::vector<std::string> VIEW_TYPES;

// Analysis constants
extern const std::vector<std::string> ACC_PAT_SUFFIXES;
extern const std::vector<std::string> DERIVED_MD_OPS;
extern const std::vector<std::string> IO_TYPES;
extern const std::vector<std::string> COMPACT_IO_TYPES;

// POSIX I/O function mappings
extern const std::unordered_set<std::string> POSIX_READ_FUNCTIONS;
extern const std::unordered_set<std::string> POSIX_WRITE_FUNCTIONS;
extern const std::unordered_set<std::string> POSIX_SYNC_FUNCTIONS;
extern const std::unordered_set<std::string> POSIX_METADATA_FUNCTIONS;
extern const std::unordered_set<std::string> POSIX_PCTL_FUNCTIONS;
extern const std::unordered_set<std::string> POSIX_IPC_FUNCTIONS;

IOCategory get_io_cat(const std::string& func_name);

// File patterns to ignore
extern const std::vector<std::string> IGNORED_FILE_PATTERNS;

// Size constants
constexpr double KiB = 1024.0;
constexpr double MiB = KiB * KiB;
constexpr double GiB = KiB * MiB;

extern const std::string SIZE_BIN_PREFIX;
extern const std::vector<double> SIZE_BINS;
extern const std::vector<std::string> SIZE_BIN_LABELS;
extern const std::vector<std::string> SIZE_BIN_NAMES;
extern const std::vector<std::string> SIZE_BIN_SUFFIXES;

constexpr size_t NUM_SIZE_BINS = 12;  // SIZE_BINS.size() - 1

// Patterns and separators
constexpr const char* FILE_PATTERN_PLACEHOLDER = "[0-9]";
constexpr const char* PROC_NAME_SEPARATOR = "#";

// Humanized column mappings
extern const std::unordered_map<std::string, std::string> HUMANIZED_COLS;
extern const std::unordered_map<std::string, std::string> HUMANIZED_METRICS;
extern const std::unordered_map<std::string, std::string> HUMANIZED_VIEW_TYPES;

// Event constants
constexpr const char* EVENT_ATT_REASONS = "attach_reasons";
constexpr const char* EVENT_COMP_HLM = "compute_hlm";
constexpr const char* EVENT_COMP_MAIN_VIEW = "compute_main_view";
constexpr const char* EVENT_COMP_METBD = "compute_metric_boundaries";
constexpr const char* EVENT_COMP_PERS = "compute_perspectives";
constexpr const char* EVENT_COMP_ROOT_VIEWS = "compute_root_views";
constexpr const char* EVENT_COMP_VIEW = "compute_view";
constexpr const char* EVENT_DET_CHAR = "detect_characteristics";
constexpr const char* EVENT_READ_TRACES = "read_traces";
constexpr const char* EVENT_SAVE_VIEWS = "save_views";

// Checkpoint constants
constexpr const char* CHECKPOINT_FLAT_VIEW = "_flat_view";
constexpr const char* CHECKPOINT_HLM = "_hlm";
constexpr const char* CHECKPOINT_MAIN_VIEW = "_main_view";
constexpr const char* CHECKPOINT_RAW_STATS = "_raw_stats";
constexpr const char* CHECKPOINT_VIEW = "_view";

// HLM constants

struct HLM_AGG {
    static constexpr const char* TIME = "time";
    static constexpr const char* COUNT = "count";
    static constexpr const char* SIZE = "size";
};

extern const std::vector<std::string> HLM_EXTRA_COLS;
constexpr const char* PARTITION_SIZE = "128MB";
constexpr bool VIEW_PERMUTATIONS = false;

const double DEFAULT_TIME_RESOLUTION = 1e6;
const double DEFAULT_TIME_GRANULARITY = 1e6;

// AI DFTracer constants for epoch processing
namespace ai_dftracer {
constexpr const char* ROOT_NAME = "ai_root";
constexpr const char* ROOT_CAT = "ai_root";
constexpr const char* ITER_COUNT_NAME = "count";
constexpr const char* INIT_NAME = "init";
constexpr const char* BLOCK_NAME = "block";
constexpr const char* ITER_NAME = "iter";
constexpr const char* CTX_SEPARATOR = ".";

// Categories
constexpr const char* CATEGORY_COMPUTE = "compute";
constexpr const char* CATEGORY_DATA = "data";
constexpr const char* CATEGORY_DATALOADER = "dataloader";
constexpr const char* CATEGORY_COMM = "comm";
constexpr const char* CATEGORY_DEVICE = "device";
constexpr const char* CATEGORY_CHECKPOINT = "checkpoint";
constexpr const char* CATEGORY_PIPELINE = "pipeline";

// Pipeline functions
constexpr const char* PIPELINE_EPOCH = "epoch";
constexpr const char* PIPELINE_TRAIN = "train";
constexpr const char* PIPELINE_EVALUATE = "evaluate";
constexpr const char* PIPELINE_TEST = "test";

// Helper functions
inline std::string get_block(const std::string& func_name) {
    return func_name + CTX_SEPARATOR + BLOCK_NAME;
}

inline std::string get_iter(const std::string& func_name) {
    return func_name + CTX_SEPARATOR + ITER_NAME;
}

inline std::string get_init(const std::string& func_name) {
    return func_name + CTX_SEPARATOR + INIT_NAME;
}

// Check if a record matches epoch criteria
inline bool is_epoch_event(const std::string& cat,
                           const std::string& func_name) {
    return cat == CATEGORY_PIPELINE &&
           (func_name == get_block(PIPELINE_EPOCH) ||
            func_name == PIPELINE_EPOCH);
}
}  // namespace ai_dftracer
}  // namespace dftracer::utils::analyzers::constants

#endif  // DFTRACER_UTILS_ANALYZERS_CONSTANTS_H
