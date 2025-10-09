#include "dftracer/utils/analyzers/constants.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

namespace dftracer::utils::analyzers::constants {
std::ostream& operator<<(std::ostream& os, const IOCategory& io_cat) {
    os << std::to_string(static_cast<std::uint64_t>(io_cat));
    return os;
}

// View types
const std::vector<std::pair<std::string, std::string>> LOGICAL_VIEW_TYPES = {
    {"file_name", "file_dir"},  {"file_name", "file_pattern"},
    {"proc_name", "app_name"},  {"proc_name", "host_name"},
    {"proc_name", "node_name"}, {"proc_name", "proc_id"},
    {"proc_name", "rank"},      {"proc_name", "thread_id"}};

const std::vector<std::string> VIEW_TYPES = {"file_name", "proc_name",
                                             "time_range"};

// Analysis constants
const std::vector<std::string> ACC_PAT_SUFFIXES = {"time", "size", "count"};
const std::vector<std::string> DERIVED_MD_OPS = {"close", "open", "seek",
                                                 "stat"};
const std::vector<std::string> IO_TYPES = {"read", "write", "metadata"};
const std::vector<std::string> COMPACT_IO_TYPES = {"R", "W", "M"};

// POSIX I/O function mappings
const std::unordered_set<std::string> POSIX_READ_FUNCTIONS = {
    "fread", "pread", "preadv", "read", "readv"};

const std::unordered_set<std::string> POSIX_WRITE_FUNCTIONS = {
    "fwrite", "pwrite", "pwritev", "write", "writev"};

const std::unordered_set<std::string> POSIX_SYNC_FUNCTIONS = {
    "fsync", "fdatasync", "msync", "sync"};

const std::unordered_set<std::string> POSIX_PCTL_FUNCTIONS = {
    "exec", "exit", "fork", "kill", "pipe", "wait"};

const std::unordered_set<std::string> POSIX_IPC_FUNCTIONS = {
    "msgctl", "msgget", "msgrcv", "msgsnd", "semctl", "semget",
    "semop",  "shmat",  "shmctl", "shmdt",  "shmget"};

const std::unordered_set<std::string> POSIX_METADATA_FUNCTIONS = {
    "__fxstat", "__fxstat64", "__lxstat", "__lxstat64", "__xstat", "__xstat64",
    "access",   "close",      "closedir", "fclose",     "fcntl",   "fopen",
    "fopen64",  "fseek",      "fstat",    "fstatat",    "ftell",   "ftruncate",
    "link",     "lseek",      "lseek64",  "mkdir",      "open",    "open64",
    "opendir",  "readdir",    "readlink", "remove",     "rename",  "rmdir",
    "seek",     "stat",       "unlink"};

IOCategory get_io_cat(const std::string& func_name) {
    // if func_name is within POSIX_METADATA_FUNCTIONS then use metadata
    if (POSIX_METADATA_FUNCTIONS.count(func_name)) {
        return IOCategory::METADATA;
    }

    if (POSIX_READ_FUNCTIONS.count(func_name)) {
        return IOCategory::READ;
    }

    if (POSIX_WRITE_FUNCTIONS.count(func_name)) {
        return IOCategory::WRITE;
    }

    if (POSIX_SYNC_FUNCTIONS.count(func_name)) {
        return IOCategory::SYNC;
    }

    if (POSIX_PCTL_FUNCTIONS.count(func_name)) {
        return IOCategory::PCTL;
    }

    if (POSIX_IPC_FUNCTIONS.count(func_name)) {
        return IOCategory::IPC;
    }

    return IOCategory::OTHER;
}

// File patterns to ignore (from Python dftracer.py lines 56-69)
const std::vector<std::string> IGNORED_FILE_PATTERNS = {"/dev/",
                                                        "/etc/",
                                                        "/gapps/python",
                                                        "/lib/python",
                                                        "/proc/",
                                                        "/software/",
                                                        "/sys/",
                                                        "/usr/lib",
                                                        "/usr/tce/backend",
                                                        "/usr/tce/packages",
                                                        "/venv",
                                                        "__pycache__"};

// Humanized column mappings
const std::unordered_map<std::string, std::string> HUMANIZED_COLS = {
    {"acc_pat", "Access Pattern"},
    {"app_io_time", "Application I/O Time"},
    {"app_name", "Application"},
    {"behavior", "Behavior"},
    {"cat", "Category"},
    {"checkpoint_io_time", "Checkpoint I/O Time"},
    {"compute_time", "Compute Time"},
    {"count", "Count"},
    {"file_dir", "File Directory"},
    {"file_name", "File"},
    {"file_pattern", "File Pattern"},
    {"func_name", "Function Name"},
    {"host_name", "Host"},
    {"io_cat", "I/O Category"},
    {"io_time", "I/O Time"},
    {"node_name", "Node"},
    {"proc_name", "Process"},
    {"rank", "Rank"},
    {"read_io_time", "Read I/O Time"},
    {"size", "Size"},
    {"time", "Time"},
    {"time_range", "Time Period"},
    {"u_app_compute_time", "Unoverlapped Application Compute Time"},
    {"u_app_io_time", "Unoverlapped Application I/O Time"},
    {"u_checkpoint_io_time", "Unoverlapped Checkpoint I/O Time"},
    {"u_compute_time", "Unoverlapped Compute Time"},
    {"u_io_time", "Unoverlapped I/O Time"},
    {"u_read_io_time", "Unoverlapped Read I/O Time"}};

const std::unordered_map<std::string, std::string> HUMANIZED_METRICS = {
    {"bw", "I/O Bandwidth"},
    {"intensity", "I/O Intensity"},
    {"iops", "I/O Operations per Second"},
    {"time", "I/O Time"}};

const std::unordered_map<std::string, std::string> HUMANIZED_VIEW_TYPES = {
    {"app_name", "App"},   {"file_dir", "File Directory"},
    {"file_name", "File"}, {"file_pattern", "File Pattern"},
    {"node_name", "Node"}, {"proc_name", "Process"},
    {"rank", "Rank"},      {"time_range", "Time Period"}};

// HLM constants
const std::vector<std::string> HLM_EXTRA_COLS = {"cat", "io_cat", "acc_pat",
                                                 "func_name"};

}  // namespace dftracer::utils::analyzers::constants
