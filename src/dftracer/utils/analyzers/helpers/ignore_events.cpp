#include <dftracer/utils/analyzers/helpers/helpers.h>

#include <string>
#include <unordered_set>

static const std::unordered_set<std::string> IGNORED_FUNC_NAMES = {
    "DLIOBenchmark.__init__",
    "DLIOBenchmark.initialize",
    "FileStorage.__init__",
    "IndexedBinaryMMapReader.__init__",
    "IndexedBinaryMMapReader.load_index",
    "IndexedBinaryMMapReader.next",
    "IndexedBinaryMMapReader.read_index",
    "NPZReader.__init__",
    "NPZReader.next",
    "NPZReader.read_index",
    "PyTorchCheckpointing.__init__",
    "PyTorchCheckpointing.finalize",
    "PyTorchCheckpointing.get_tensor",
    "SCRPyTorchCheckpointing.__init__",
    "SCRPyTorchCheckpointing.finalize",
    "SCRPyTorchCheckpointing.get_tensor",
    "TFCheckpointing.__init__",
    "TFCheckpointing.finalize",
    "TFCheckpointing.get_tensor",
    "TFDataLoader.__init__",
    "TFDataLoader.finalize",
    "TFDataLoader.next",
    "TFDataLoader.read",
    "TFFramework.get_loader",
    "TFFramework.init_loader",
    "TFFramework.is_nativeio_available",
    "TFFramework.trace_object",
    "TFReader.__init__",
    "TFReader.next",
    "TFReader.read_index",
    "TorchDataLoader.__init__",
    "TorchDataLoader.finalize",
    "TorchDataLoader.next",
    "TorchDataLoader.read",
    "TorchDataset.__init__",
    "TorchFramework.get_loader",
    "TorchFramework.init_loader",
    "TorchFramework.is_nativeio_available",
    "TorchFramework.trace_object"};


static const std::vector<std::string> IGNORED_FUNC_PATTERNS = {
    ".save_state", "checkpoint_end_", "checkpoint_start_"};

bool should_ignore_event(const std::string& func_name) {
    if (IGNORED_FUNC_NAMES.find(func_name) !=
        IGNORED_FUNC_NAMES.end()) {
        return true;
    }

    for (const auto& pattern : IGNORED_FUNC_PATTERNS) {
        if (func_name.find(pattern) != std::string::npos) {
            return true;
        }
    }

    return false;
}

