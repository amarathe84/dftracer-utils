#include <dftracer/utils/call_graph/call_graph_mpi.h>
#include <dftracer/utils/common/format_detector.h>
#include <dftracer/utils/reader/line_processor.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

#include <yyjson.h>
#include <zlib.h>

namespace fs = std::filesystem;

namespace dftracer::utils::call_graph {

// ============================================================================
// Serialization Utilities
// ============================================================================

namespace serialization {

void write_uint32(std::vector<char>& buffer, std::uint32_t value) {
    buffer.insert(buffer.end(), 
                  reinterpret_cast<const char*>(&value),
                  reinterpret_cast<const char*>(&value) + sizeof(value));
}

void write_uint64(std::vector<char>& buffer, std::uint64_t value) {
    buffer.insert(buffer.end(),
                  reinterpret_cast<const char*>(&value),
                  reinterpret_cast<const char*>(&value) + sizeof(value));
}

void write_int(std::vector<char>& buffer, int value) {
    buffer.insert(buffer.end(),
                  reinterpret_cast<const char*>(&value),
                  reinterpret_cast<const char*>(&value) + sizeof(value));
}

void write_string(std::vector<char>& buffer, const std::string& str) {
    std::uint32_t len = static_cast<std::uint32_t>(str.size());
    write_uint32(buffer, len);
    buffer.insert(buffer.end(), str.begin(), str.end());
}

std::uint32_t read_uint32(const char* data, size_t& offset) {
    std::uint32_t value;
    std::memcpy(&value, data + offset, sizeof(value));
    offset += sizeof(value);
    return value;
}

std::uint64_t read_uint64(const char* data, size_t& offset) {
    std::uint64_t value;
    std::memcpy(&value, data + offset, sizeof(value));
    offset += sizeof(value);
    return value;
}

int read_int(const char* data, size_t& offset) {
    int value;
    std::memcpy(&value, data + offset, sizeof(value));
    offset += sizeof(value);
    return value;
}

std::string read_string(const char* data, size_t& offset) {
    std::uint32_t len = read_uint32(data, offset);
    std::string str(data + offset, len);
    offset += len;
    return str;
}

} // namespace serialization

// ============================================================================
// SerializableCallNode Implementation
// ============================================================================

std::vector<char> SerializableCallNode::serialize() const {
    std::vector<char> buffer;
    
    serialization::write_uint64(buffer, id);
    serialization::write_string(buffer, name);
    serialization::write_string(buffer, category);
    serialization::write_uint64(buffer, start_time);
    serialization::write_uint64(buffer, duration);
    serialization::write_int(buffer, level);
    serialization::write_uint64(buffer, parent_id);
    
    // Children
    serialization::write_uint32(buffer, static_cast<std::uint32_t>(children.size()));
    for (auto child_id : children) {
        serialization::write_uint64(buffer, child_id);
    }
    
    // Args
    serialization::write_uint32(buffer, static_cast<std::uint32_t>(args.size()));
    for (const auto& [key, value] : args) {
        serialization::write_string(buffer, key);
        serialization::write_string(buffer, value);
    }
    
    return buffer;
}

SerializableCallNode SerializableCallNode::deserialize(const char* data, size_t& offset) {
    SerializableCallNode node;
    
    node.id = serialization::read_uint64(data, offset);
    node.name = serialization::read_string(data, offset);
    node.category = serialization::read_string(data, offset);
    node.start_time = serialization::read_uint64(data, offset);
    node.duration = serialization::read_uint64(data, offset);
    node.level = serialization::read_int(data, offset);
    node.parent_id = serialization::read_uint64(data, offset);
    
    // Children
    std::uint32_t num_children = serialization::read_uint32(data, offset);
    node.children.reserve(num_children);
    for (std::uint32_t i = 0; i < num_children; i++) {
        node.children.push_back(serialization::read_uint64(data, offset));
    }
    
    // Args
    std::uint32_t num_args = serialization::read_uint32(data, offset);
    for (std::uint32_t i = 0; i < num_args; i++) {
        std::string key = serialization::read_string(data, offset);
        std::string value = serialization::read_string(data, offset);
        node.args[key] = value;
    }
    
    return node;
}

// ============================================================================
// SerializableProcessGraph Implementation
// ============================================================================

std::vector<char> SerializableProcessGraph::serialize() const {
    std::vector<char> buffer;
    
    // Key
    serialization::write_uint32(buffer, key.pid);
    serialization::write_uint32(buffer, key.tid);
    serialization::write_uint32(buffer, key.node_id);
    
    // Nodes
    serialization::write_uint32(buffer, static_cast<std::uint32_t>(nodes.size()));
    for (const auto& node : nodes) {
        auto node_data = node.serialize();
        serialization::write_uint32(buffer, static_cast<std::uint32_t>(node_data.size()));
        buffer.insert(buffer.end(), node_data.begin(), node_data.end());
    }
    
    // Root calls
    serialization::write_uint32(buffer, static_cast<std::uint32_t>(root_calls.size()));
    for (auto id : root_calls) {
        serialization::write_uint64(buffer, id);
    }
    
    // Call sequence
    serialization::write_uint32(buffer, static_cast<std::uint32_t>(call_sequence.size()));
    for (auto id : call_sequence) {
        serialization::write_uint64(buffer, id);
    }
    
    return buffer;
}

SerializableProcessGraph SerializableProcessGraph::deserialize(const char* data, size_t& offset) {
    SerializableProcessGraph graph;
    
    // Key
    graph.key.pid = serialization::read_uint32(data, offset);
    graph.key.tid = serialization::read_uint32(data, offset);
    graph.key.node_id = serialization::read_uint32(data, offset);
    
    // Nodes
    std::uint32_t num_nodes = serialization::read_uint32(data, offset);
    graph.nodes.reserve(num_nodes);
    for (std::uint32_t i = 0; i < num_nodes; i++) {
        std::uint32_t node_size = serialization::read_uint32(data, offset);
        (void)node_size; // Not needed for deserialization
        graph.nodes.push_back(SerializableCallNode::deserialize(data, offset));
    }
    
    // Root calls
    std::uint32_t num_roots = serialization::read_uint32(data, offset);
    graph.root_calls.reserve(num_roots);
    for (std::uint32_t i = 0; i < num_roots; i++) {
        graph.root_calls.push_back(serialization::read_uint64(data, offset));
    }
    
    // Call sequence
    std::uint32_t num_seq = serialization::read_uint32(data, offset);
    graph.call_sequence.reserve(num_seq);
    for (std::uint32_t i = 0; i < num_seq; i++) {
        graph.call_sequence.push_back(serialization::read_uint64(data, offset));
    }
    
    return graph;
}

// ============================================================================
// MPIFilteredTraceReader Implementation
// ============================================================================

MPIFilteredTraceReader::MPIFilteredTraceReader(const std::set<std::uint32_t>& allowed_pids)
    : allowed_pids_(allowed_pids)
    , processed_count_(0)
    , filtered_count_(0) {
}

bool MPIFilteredTraceReader::read(const std::string& trace_file, CallGraph& graph) {
    // Check if it's a gzip file
    ArchiveFormat format = FormatDetector::detect(trace_file);
    
    if (format == ArchiveFormat::GZIP) {
        // Try to use indexer
        std::string idx_file = trace_file + ".zindex";
        if (fs::exists(idx_file)) {
            return read_with_indexer(trace_file, idx_file, graph);
        }
    }
    
    // Fall back to direct reading for plain text files
    std::ifstream file(trace_file);
    if (!file.is_open()) {
        std::cerr << "Cannot open trace file: " << trace_file << std::endl;
        return false;
    }
    
    std::string line;
    size_t line_count = 0;
    
    while (std::getline(file, line)) {
        line_count++;
        
        // Skip brackets and empty lines
        if (line.empty() || line == "[" || line == "]") {
            continue;
        }
        
        // Remove trailing comma
        if (!line.empty() && line.back() == ',') {
            line.pop_back();
        }
        
        yyjson_doc* doc = yyjson_read(line.c_str(), line.length(), 0);
        if (!doc) {
            continue;
        }
        
        yyjson_val* root = yyjson_doc_get_root(doc);
        if (!root) {
            yyjson_doc_free(doc);
            continue;
        }
        
        // Check PID filter
        yyjson_val* pid_val = yyjson_obj_get(root, "pid");
        if (pid_val) {
            std::uint32_t pid = static_cast<std::uint32_t>(yyjson_get_uint(pid_val));
            
            // Only process if PID is in our allowed set
            if (allowed_pids_.find(pid) != allowed_pids_.end()) {
                // Use the standard TraceReader processing
                TraceReader reader;
                if (reader.process_trace_line(line, graph)) {
                    processed_count_++;
                }
            } else {
                filtered_count_++;
            }
        }
        
        yyjson_doc_free(doc);
    }
    
    return true;
}

/**
 * Line processor for filtered reading with indexer
 */
class FilteredLineProcessor : public LineProcessor {
public:
    FilteredLineProcessor(const std::set<std::uint32_t>& allowed_pids,
                         CallGraph& graph,
                         std::size_t& processed_count,
                         std::size_t& filtered_count)
        : allowed_pids_(allowed_pids)
        , graph_(graph)
        , processed_count_(processed_count)
        , filtered_count_(filtered_count)
        , reader_() {
    }
    
    bool process(const char* data, std::size_t length) override {
        if (length == 0) {
            return true;
        }
        
        std::string line(data, length);
        
        // Skip brackets
        if (line == "[" || line == "]") {
            return true;
        }
        
        // Remove trailing comma
        if (!line.empty() && line.back() == ',') {
            line.pop_back();
        }
        
        // Quick PID check
        yyjson_doc* doc = yyjson_read(line.c_str(), line.length(), 0);
        if (!doc) {
            return true;
        }
        
        yyjson_val* root = yyjson_doc_get_root(doc);
        if (!root) {
            yyjson_doc_free(doc);
            return true;
        }
        
        yyjson_val* pid_val = yyjson_obj_get(root, "pid");
        if (pid_val) {
            std::uint32_t pid = static_cast<std::uint32_t>(yyjson_get_uint(pid_val));
            
            if (allowed_pids_.find(pid) != allowed_pids_.end()) {
                if (reader_.process_trace_line(line, graph_)) {
                    processed_count_++;
                }
            } else {
                filtered_count_++;
            }
        }
        
        yyjson_doc_free(doc);
        return true;
    }

private:
    const std::set<std::uint32_t>& allowed_pids_;
    CallGraph& graph_;
    std::size_t& processed_count_;
    std::size_t& filtered_count_;
    TraceReader reader_;
};

bool MPIFilteredTraceReader::read_with_indexer(const std::string& trace_file,
                                               const std::string& index_file,
                                               CallGraph& graph) {
    try {
        auto reader = ReaderFactory::create(trace_file, index_file);
        if (!reader || !reader->is_valid()) {
            std::cerr << "Failed to create reader for " << trace_file << std::endl;
            return read(trace_file, graph); // Fallback
        }
        
        FilteredLineProcessor processor(allowed_pids_, graph, 
                                       processed_count_, filtered_count_);
        
        std::size_t num_lines = reader->get_num_lines();
        if (num_lines > 0) {
            reader->read_lines_with_processor(1, num_lines, processor);
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error reading with indexer: " << e.what() << std::endl;
        return false;
    }
}

bool MPIFilteredTraceReader::read_multiple(const std::vector<std::string>& trace_files,
                                          CallGraph& graph) {
    for (const auto& file : trace_files) {
        if (!read(file, graph)) {
            return false;
        }
    }
    return true;
}

// ============================================================================
// MPICallGraphBuilder Implementation
// ============================================================================

MPICallGraphBuilder::MPICallGraphBuilder(const MPICallGraphConfig& config)
    : config_(config)
    , call_graph_(std::make_unique<CallGraph>())
    , rank_(0)
    , world_size_(1)
    , mpi_initialized_(false)
    , trace_files_()
    , indexers_()
    , pid_index_map_()
    , assigned_pids_()
    , all_pids_()
    , initialized_(false)
    , pids_discovered_(false)
    , graphs_built_(false)
    , graphs_gathered_(false) {
}

MPICallGraphBuilder::~MPICallGraphBuilder() {
    if (initialized_) {
        cleanup();
    }
}

MPICallGraphBuilder::MPICallGraphBuilder(MPICallGraphBuilder&& other) noexcept
    : config_(std::move(other.config_))
    , call_graph_(std::move(other.call_graph_))
    , rank_(other.rank_)
    , world_size_(other.world_size_)
    , mpi_initialized_(other.mpi_initialized_)
    , trace_files_(std::move(other.trace_files_))
    , indexers_(std::move(other.indexers_))
    , pid_index_map_(std::move(other.pid_index_map_))
    , assigned_pids_(std::move(other.assigned_pids_))
    , all_pids_(std::move(other.all_pids_))
    , initialized_(other.initialized_)
    , pids_discovered_(other.pids_discovered_)
    , graphs_built_(other.graphs_built_)
    , graphs_gathered_(other.graphs_gathered_) {
    other.initialized_ = false;
}

MPICallGraphBuilder& MPICallGraphBuilder::operator=(MPICallGraphBuilder&& other) noexcept {
    if (this != &other) {
        if (initialized_) {
            cleanup();
        }
        config_ = std::move(other.config_);
        call_graph_ = std::move(other.call_graph_);
        rank_ = other.rank_;
        world_size_ = other.world_size_;
        mpi_initialized_ = other.mpi_initialized_;
        trace_files_ = std::move(other.trace_files_);
        indexers_ = std::move(other.indexers_);
        pid_index_map_ = std::move(other.pid_index_map_);
        assigned_pids_ = std::move(other.assigned_pids_);
        all_pids_ = std::move(other.all_pids_);
        initialized_ = other.initialized_;
        pids_discovered_ = other.pids_discovered_;
        graphs_built_ = other.graphs_built_;
        graphs_gathered_ = other.graphs_gathered_;
        other.initialized_ = false;
    }
    return *this;
}

void MPICallGraphBuilder::initialize() {
    if (initialized_) {
        return;
    }
    
#ifdef DFTRACER_UTILS_MPI_ENABLED
    int mpi_init = 0;
    MPI_Initialized(&mpi_init);
    if (mpi_init) {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
        MPI_Comm_size(MPI_COMM_WORLD, &world_size_);
        mpi_initialized_ = true;
    }
#endif
    
    call_graph_->initialize();
    initialized_ = true;
    
    if (rank_ == 0 && config_.verbose) {
        std::cout << "MPICallGraphBuilder initialized with " 
                  << world_size_ << " MPI ranks" << std::endl;
    }
}

void MPICallGraphBuilder::cleanup() {
    if (!initialized_) {
        return;
    }
    
    call_graph_->cleanup();
    indexers_.clear();
    trace_files_.clear();
    pid_index_map_.clear();
    assigned_pids_.clear();
    all_pids_.clear();
    
    initialized_ = false;
    pids_discovered_ = false;
    graphs_built_ = false;
    graphs_gathered_ = false;
}

void MPICallGraphBuilder::add_trace_files(const std::vector<std::string>& files) {
    for (const auto& file : files) {
        if (fs::exists(file) && fs::is_regular_file(file)) {
            trace_files_.push_back(file);
        } else if (rank_ == 0) {
            std::cerr << "Warning: File not found: " << file << std::endl;
        }
    }
}

void MPICallGraphBuilder::add_trace_directory(const std::string& directory,
                                             const std::string& pattern) {
    if (!fs::exists(directory) || !fs::is_directory(directory)) {
        if (rank_ == 0) {
            std::cerr << "Directory not found: " << directory << std::endl;
        }
        return;
    }
    
    // Recursively find all matching files
    for (const auto& entry : fs::recursive_directory_iterator(directory)) {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().filename().string();
            
            // Simple pattern matching for *.ext or *.part1.part2 patterns
            bool matches = false;
            if (pattern == "*") {
                matches = true;
            } else if (pattern.front() == '*') {
                // *.ext or *.pfw.gz pattern
                std::string suffix = pattern.substr(1);  // Remove the leading *
                matches = (filename.size() >= suffix.size() &&
                          filename.substr(filename.size() - suffix.size()) == suffix);
            } else {
                matches = (filename.find(pattern) != std::string::npos);
            }
            
            if (matches) {
                trace_files_.push_back(entry.path().string());
            }
        }
    }
    
    std::sort(trace_files_.begin(), trace_files_.end());
    
    if (rank_ == 0 && config_.verbose) {
        std::cout << "Found " << trace_files_.size() << " trace files in " 
                  << directory << std::endl;
    }
}

void MPICallGraphBuilder::create_indexer(const std::string& trace_file) {
    if (indexers_.find(trace_file) != indexers_.end()) {
        return;
    }
    
    ArchiveFormat format = FormatDetector::detect(trace_file);
    if (format != ArchiveFormat::GZIP) {
        return; // Only create indexers for gzip files
    }
    
    std::string idx_file = trace_file + ".zindex";
    std::uint64_t ckpt_size = config_.checkpoint_size > 0 
        ? config_.checkpoint_size 
        : Indexer::DEFAULT_CHECKPOINT_SIZE;
    
    try {
        auto indexer = IndexerFactory::create(trace_file, idx_file, ckpt_size, false);
        if (indexer) {
            // Build index if needed
            if (indexer->need_rebuild()) {
                if (rank_ == 0 && config_.verbose) {
                    std::cout << "Building index for " << trace_file << std::endl;
                }
                indexer->build();
            }
            indexers_[trace_file] = std::move(indexer);
        }
    } catch (const std::exception& e) {
        if (config_.verbose) {
            std::cerr << "Warning: Could not create indexer for " << trace_file 
                      << ": " << e.what() << std::endl;
        }
    }
}

std::set<std::uint32_t> MPICallGraphBuilder::scan_file_for_pids(const std::string& trace_file) {
    std::set<std::uint32_t> pids;
    
    // Check if it's a gzip file with an index
    ArchiveFormat format = FormatDetector::detect(trace_file);
    std::string idx_file = trace_file + ".zindex";
    
    if (format == ArchiveFormat::GZIP && fs::exists(idx_file)) {
        try {
            auto reader = ReaderFactory::create(trace_file, idx_file);
            if (reader && reader->is_valid()) {
                // Read first N lines to discover PIDs
                std::size_t num_lines = reader->get_num_lines();
                std::string content = reader->read_lines(1, std::min(num_lines, (std::size_t)100000));
                
                std::istringstream iss(content);
                std::string line;
                while (std::getline(iss, line)) {
                    if (line.empty() || line == "[" || line == "]") continue;
                    if (!line.empty() && line.back() == ',') line.pop_back();
                    
                    yyjson_doc* doc = yyjson_read(line.c_str(), line.length(), 0);
                    if (doc) {
                        yyjson_val* root = yyjson_doc_get_root(doc);
                        if (root) {
                            yyjson_val* pid_val = yyjson_obj_get(root, "pid");
                            if (pid_val) {
                                pids.insert(static_cast<std::uint32_t>(yyjson_get_uint(pid_val)));
                            }
                        }
                        yyjson_doc_free(doc);
                    }
                }
                
                return pids;
            }
        } catch (const std::exception& e) {
            // Fall through to direct reading
        }
    }
    
    // For gzip files without index, use gzopen
    if (format == ArchiveFormat::GZIP) {
        gzFile gz = gzopen(trace_file.c_str(), "rb");
        if (!gz) {
            return pids;
        }
        
        char buffer[65536];
        std::string current_line;
        int line_count = 0;
        
        while (line_count < 100000) {
            int bytes_read = gzread(gz, buffer, sizeof(buffer) - 1);
            if (bytes_read <= 0) break;
            buffer[bytes_read] = '\0';
            
            current_line += buffer;
            
            // Process complete lines
            size_t pos;
            while ((pos = current_line.find('\n')) != std::string::npos) {
                std::string line = current_line.substr(0, pos);
                current_line = current_line.substr(pos + 1);
                line_count++;
                
                if (line.empty() || line == "[" || line == "]") continue;
                if (!line.empty() && line.back() == ',') line.pop_back();
                
                yyjson_doc* doc = yyjson_read(line.c_str(), line.length(), 0);
                if (doc) {
                    yyjson_val* root = yyjson_doc_get_root(doc);
                    if (root) {
                        yyjson_val* pid_val = yyjson_obj_get(root, "pid");
                        if (pid_val) {
                            pids.insert(static_cast<std::uint32_t>(yyjson_get_uint(pid_val)));
                        }
                    }
                    yyjson_doc_free(doc);
                }
                
                if (line_count >= 100000) break;
            }
        }
        
        gzclose(gz);
        return pids;
    }
    
    // Fall back to direct file reading for plain text
    std::ifstream file(trace_file);
    if (!file.is_open()) {
        return pids;
    }
    
    std::string line;
    int line_count = 0;
    while (std::getline(file, line) && line_count < 100000) {
        line_count++;
        if (line.empty() || line == "[" || line == "]") continue;
        if (!line.empty() && line.back() == ',') line.pop_back();
        
        yyjson_doc* doc = yyjson_read(line.c_str(), line.length(), 0);
        if (doc) {
            yyjson_val* root = yyjson_doc_get_root(doc);
            if (root) {
                yyjson_val* pid_val = yyjson_obj_get(root, "pid");
                if (pid_val) {
                    pids.insert(static_cast<std::uint32_t>(yyjson_get_uint(pid_val)));
                }
            }
            yyjson_doc_free(doc);
        }
    }
    
    return pids;
}

void MPICallGraphBuilder::broadcast_string(std::string& str, int root) {
#ifdef DFTRACER_UTILS_MPI_ENABLED
    int len = static_cast<int>(str.size());
    MPI_Bcast(&len, 1, MPI_INT, root, MPI_COMM_WORLD);
    
    if (rank_ != root) {
        str.resize(len);
    }
    
    if (len > 0) {
        MPI_Bcast(&str[0], len, MPI_CHAR, root, MPI_COMM_WORLD);
    }
#else
    (void)str;
    (void)root;
#endif
}

void MPICallGraphBuilder::broadcast_pids(std::vector<std::uint32_t>& pids, int root) {
#ifdef DFTRACER_UTILS_MPI_ENABLED
    int count = static_cast<int>(pids.size());
    MPI_Bcast(&count, 1, MPI_INT, root, MPI_COMM_WORLD);
    
    if (rank_ != root) {
        pids.resize(count);
    }
    
    if (count > 0) {
        MPI_Bcast(pids.data(), count, MPI_UINT32_T, root, MPI_COMM_WORLD);
    }
#else
    (void)pids;
    (void)root;
#endif
}

void MPICallGraphBuilder::distribute_pids() {
    // Round-robin distribution
    assigned_pids_.clear();
    for (size_t i = static_cast<size_t>(rank_); i < all_pids_.size(); 
         i += static_cast<size_t>(world_size_)) {
        assigned_pids_.insert(all_pids_[i]);
    }
    
    if (config_.verbose) {
        std::cout << "[Rank " << rank_ << "] Assigned " << assigned_pids_.size() 
                  << " PIDs: ";
        bool first = true;
        for (auto pid : assigned_pids_) {
            if (!first) std::cout << ", ";
            std::cout << pid;
            first = false;
        }
        std::cout << std::endl;
    }
}

std::map<std::uint32_t, PIDIndexInfo> MPICallGraphBuilder::discover_pids() {
    if (!initialized_) {
        initialize();
    }
    
    if (rank_ == 0 && config_.verbose) {
        std::cout << "Phase 1: Discovering PIDs from " << trace_files_.size() 
                  << " trace files..." << std::endl;
    }
    
#ifdef DFTRACER_UTILS_MPI_ENABLED
    // Broadcast file list from rank 0
    int num_files = static_cast<int>(trace_files_.size());
    MPI_Bcast(&num_files, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    if (rank_ != 0) {
        trace_files_.resize(num_files);
    }
    
    for (int i = 0; i < num_files; i++) {
        broadcast_string(trace_files_[i], 0);
    }
#endif
    
    // Each rank scans files to discover PIDs
    std::set<std::uint32_t> local_pids;
    
    for (const auto& trace_file : trace_files_) {
        // Create indexer if needed
        create_indexer(trace_file);
        
        // Scan for PIDs
        auto file_pids = scan_file_for_pids(trace_file);
        local_pids.insert(file_pids.begin(), file_pids.end());
        
        // Store PID index info
        for (auto pid : file_pids) {
            if (pid_index_map_.find(pid) == pid_index_map_.end()) {
                pid_index_map_[pid] = PIDIndexInfo(pid, 0, 0, 0, trace_file);
            }
        }
    }
    
#ifdef DFTRACER_UTILS_MPI_ENABLED
    // Gather all PIDs to rank 0
    std::vector<std::uint32_t> local_pid_vec(local_pids.begin(), local_pids.end());
    int local_count = static_cast<int>(local_pid_vec.size());
    
    std::vector<int> recv_counts(world_size_);
    MPI_Gather(&local_count, 1, MPI_INT, recv_counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    std::vector<std::uint32_t> all_pids_gathered;
    std::vector<int> displacements(world_size_);
    
    if (rank_ == 0) {
        int total = 0;
        for (int i = 0; i < world_size_; i++) {
            displacements[i] = total;
            total += recv_counts[i];
        }
        all_pids_gathered.resize(total);
    }
    
    MPI_Gatherv(local_pid_vec.data(), local_count, MPI_UINT32_T,
                all_pids_gathered.data(), recv_counts.data(), displacements.data(),
                MPI_UINT32_T, 0, MPI_COMM_WORLD);
    
    // Remove duplicates and sort on rank 0
    if (rank_ == 0) {
        std::set<std::uint32_t> unique_pids(all_pids_gathered.begin(), 
                                            all_pids_gathered.end());
        all_pids_.assign(unique_pids.begin(), unique_pids.end());
        std::sort(all_pids_.begin(), all_pids_.end());
        
        if (config_.verbose) {
            std::cout << "Discovered " << all_pids_.size() << " unique PIDs" << std::endl;
        }
    }
    
    // Broadcast unique PIDs to all ranks
    broadcast_pids(all_pids_, 0);
    
    // Distribute PIDs across ranks
    distribute_pids();
    
    MPI_Barrier(MPI_COMM_WORLD);
#else
    all_pids_.assign(local_pids.begin(), local_pids.end());
    assigned_pids_ = local_pids;
#endif
    
    pids_discovered_ = true;
    return pid_index_map_;
}

bool MPICallGraphBuilder::read_traces_for_pids(const std::vector<std::string>& files,
                                               const std::set<std::uint32_t>& pids) {
    MPIFilteredTraceReader reader(pids);
    return reader.read_multiple(files, *call_graph_);
}

MPICallGraphResult MPICallGraphBuilder::build() {
    MPICallGraphResult result;
    
    if (!pids_discovered_) {
        discover_pids();
    }
    
    if (assigned_pids_.empty()) {
        if (config_.verbose) {
            std::cout << "[Rank " << rank_ << "] No PIDs assigned, skipping build" << std::endl;
        }
        result.success = true;
        graphs_built_ = true;
        return result;
    }
    
    if (rank_ == 0 && config_.verbose) {
        std::cout << "Phase 2: Building call graphs..." << std::endl;
    }
    
#ifdef DFTRACER_UTILS_MPI_ENABLED
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Use pipeline for parallel trace reading
    if (config_.num_threads > 0) {
        // Create pipeline with thread executor
        Pipeline pipeline("call_graph_build");
        auto executor = ExecutorFactory::create_thread(config_.num_threads);
        
        // For now, use simple sequential processing
        // Pipeline can be expanded for more complex workflows
        read_traces_for_pids(trace_files_, assigned_pids_);
    } else {
        // Sequential processing
        read_traces_for_pids(trace_files_, assigned_pids_);
    }
    
    // Build hierarchy
    call_graph_->build_hierarchy();
    
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end_time - start_time;
    
    result.elapsed_time_s = elapsed.count();
    result.local_pids = assigned_pids_.size();
    result.local_events = 0;
    
    // Count events
    for (const auto& key : call_graph_->keys()) {
        auto* graph = call_graph_->get(key);
        if (graph) {
            result.local_events += graph->calls.size();
        }
    }
    
#ifdef DFTRACER_UTILS_MPI_ENABLED
    // Gather statistics
    MPI_Reduce(&result.local_pids, &result.total_pids, 1, 
               MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&result.local_events, &result.total_events, 1,
               MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    
    double max_time = 0;
    MPI_Reduce(&result.elapsed_time_s, &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    result.elapsed_time_s = max_time;
#else
    result.total_pids = result.local_pids;
    result.total_events = result.local_events;
#endif
    
    result.success = true;
    graphs_built_ = true;
    
    if (rank_ == 0 && config_.verbose) {
        std::cout << "Build completed in " << result.elapsed_time_s << " seconds" << std::endl;
        std::cout << "Total PIDs: " << result.total_pids << std::endl;
        std::cout << "Total events: " << result.total_events << std::endl;
    }
    
    return result;
}

SerializableProcessGraph MPICallGraphBuilder::convert_to_serializable(
    const ProcessCallGraph& graph) const {
    
    SerializableProcessGraph result;
    result.key = graph.key;
    result.root_calls = graph.root_calls;
    result.call_sequence = graph.call_sequence;
    
    for (const auto& [id, node] : graph.calls) {
        SerializableCallNode snode;
        snode.id = node->get_id();
        snode.name = node->get_name();
        snode.category = node->get_category();
        snode.start_time = node->get_start_time();
        snode.duration = node->get_duration();
        snode.level = node->get_level();
        snode.parent_id = node->get_parent_id();
        snode.children = node->get_children();
        snode.args = node->get_args();
        result.nodes.push_back(std::move(snode));
    }
    
    return result;
}

void MPICallGraphBuilder::merge_from_serializable(const SerializableProcessGraph& serializable) {
    ProcessCallGraph& graph = (*call_graph_)[serializable.key];
    graph.key = serializable.key;
    graph.root_calls = serializable.root_calls;
    graph.call_sequence = serializable.call_sequence;
    
    for (const auto& snode : serializable.nodes) {
        auto node = call_graph_->get_factory().create_node(
            snode.id, snode.name, snode.category,
            snode.start_time, snode.duration, snode.level, snode.args);
        node->set_parent_id(snode.parent_id);
        for (auto child_id : snode.children) {
            node->add_child(child_id);
        }
        graph.calls[snode.id] = node;
    }
}

bool MPICallGraphBuilder::alltoall_graphs() {
#ifdef DFTRACER_UTILS_MPI_ENABLED
    // Serialize local graphs
    std::vector<SerializableProcessGraph> local_graphs;
    for (const auto& key : call_graph_->keys()) {
        auto* graph = call_graph_->get(key);
        if (graph) {
            local_graphs.push_back(convert_to_serializable(*graph));
        }
    }
    
    // Serialize to bytes
    std::vector<char> send_buffer;
    serialization::write_uint32(send_buffer, static_cast<std::uint32_t>(local_graphs.size()));
    for (const auto& graph : local_graphs) {
        auto data = graph.serialize();
        serialization::write_uint32(send_buffer, static_cast<std::uint32_t>(data.size()));
        send_buffer.insert(send_buffer.end(), data.begin(), data.end());
    }
    
    // Gather send sizes
    int send_size = static_cast<int>(send_buffer.size());
    std::vector<int> recv_sizes(world_size_);
    MPI_Allgather(&send_size, 1, MPI_INT, recv_sizes.data(), 1, MPI_INT, MPI_COMM_WORLD);
    
    // Calculate displacements
    std::vector<int> displacements(world_size_);
    int total_recv = 0;
    for (int i = 0; i < world_size_; i++) {
        displacements[i] = total_recv;
        total_recv += recv_sizes[i];
    }
    
    // Allgatherv to collect all data
    std::vector<char> recv_buffer(total_recv);
    MPI_Allgatherv(send_buffer.data(), send_size, MPI_CHAR,
                   recv_buffer.data(), recv_sizes.data(), displacements.data(),
                   MPI_CHAR, MPI_COMM_WORLD);
    
    // Deserialize graphs from other ranks
    for (int r = 0; r < world_size_; r++) {
        if (r == rank_) continue; // Skip our own data
        
        size_t offset = static_cast<size_t>(displacements[r]);
        std::uint32_t num_graphs = serialization::read_uint32(recv_buffer.data(), offset);
        
        for (std::uint32_t i = 0; i < num_graphs; i++) {
            std::uint32_t graph_size = serialization::read_uint32(recv_buffer.data(), offset);
            (void)graph_size;
            auto graph = SerializableProcessGraph::deserialize(recv_buffer.data(), offset);
            merge_from_serializable(graph);
        }
    }
    
    return true;
#else
    return true; // No-op without MPI
#endif
}

bool MPICallGraphBuilder::gather() {
    if (!graphs_built_) {
        return false;
    }
    
    if (rank_ == 0 && config_.verbose) {
        std::cout << "Phase 3: Gathering call graphs (all-to-all)..." << std::endl;
    }
    
#ifdef DFTRACER_UTILS_MPI_ENABLED
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    
    bool success = alltoall_graphs();
    
#ifdef DFTRACER_UTILS_MPI_ENABLED
    MPI_Barrier(MPI_COMM_WORLD);
#endif
    
    graphs_gathered_ = success;
    
    if (rank_ == 0 && config_.verbose) {
        std::cout << "Gather completed. Total graphs: " << call_graph_->size() << std::endl;
    }
    
    return success;
}

bool MPICallGraphBuilder::save(const std::string& filename) const {
    // Only rank 0 saves (all ranks have same data after gather)
    if (rank_ != 0) {
        return true;
    }
    
    std::ofstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Cannot open output file: " << filename << std::endl;
        return false;
    }
    
    // Write header
    CallGraphFileHeader header;
    header.num_process_graphs = static_cast<std::uint32_t>(call_graph_->size());
    
    // Count total events
    std::uint64_t total_events = 0;
    for (const auto& key : call_graph_->keys()) {
        auto* graph = call_graph_->get(key);
        if (graph) {
            total_events += graph->calls.size();
        }
    }
    header.total_events = total_events;
    header.data_offset = sizeof(CallGraphFileHeader);
    
    file.write(reinterpret_cast<const char*>(&header), sizeof(header));
    
    // Write each process graph
    for (const auto& key : call_graph_->keys()) {
        auto* graph = call_graph_->get(key);
        if (graph) {
            auto serializable = const_cast<MPICallGraphBuilder*>(this)->convert_to_serializable(*graph);
            auto data = serializable.serialize();
            std::uint32_t size = static_cast<std::uint32_t>(data.size());
            file.write(reinterpret_cast<const char*>(&size), sizeof(size));
            file.write(data.data(), data.size());
        }
    }
    
    if (config_.verbose) {
        std::cout << "Saved call graph to " << filename << std::endl;
    }
    
    return true;
}

std::unique_ptr<CallGraph> MPICallGraphBuilder::load(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Cannot open file: " << filename << std::endl;
        return nullptr;
    }
    
    // Read header
    CallGraphFileHeader header;
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    
    if (!header.is_valid()) {
        std::cerr << "Invalid call graph file format" << std::endl;
        return nullptr;
    }
    
    auto call_graph = std::make_unique<CallGraph>();
    call_graph->initialize();
    
    // Read each process graph
    for (std::uint32_t i = 0; i < header.num_process_graphs; i++) {
        std::uint32_t size;
        file.read(reinterpret_cast<char*>(&size), sizeof(size));
        
        std::vector<char> data(size);
        file.read(data.data(), size);
        
        size_t offset = 0;
        auto serializable = SerializableProcessGraph::deserialize(data.data(), offset);
        
        // Merge into call graph
        ProcessCallGraph& graph = (*call_graph)[serializable.key];
        graph.key = serializable.key;
        graph.root_calls = serializable.root_calls;
        graph.call_sequence = serializable.call_sequence;
        
        for (const auto& snode : serializable.nodes) {
            auto node = call_graph->get_factory().create_node(
                snode.id, snode.name, snode.category,
                snode.start_time, snode.duration, snode.level, snode.args);
            node->set_parent_id(snode.parent_id);
            for (auto child_id : snode.children) {
                node->add_child(child_id);
            }
            graph.calls[snode.id] = node;
        }
    }
    
    return call_graph;
}

void MPICallGraphBuilder::print_summary() const {
    std::size_t local_graphs = call_graph_->size();
    std::size_t local_events = 0;
    
    for (const auto& key : call_graph_->keys()) {
        auto* graph = call_graph_->get(key);
        if (graph) {
            local_events += graph->calls.size();
        }
    }
    
#ifdef DFTRACER_UTILS_MPI_ENABLED
    std::size_t total_graphs = 0;
    std::size_t total_events = 0;
    
    MPI_Reduce(&local_graphs, &total_graphs, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_events, &total_events, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    
    if (rank_ == 0) {
        std::cout << "\n============ MPI Call Graph Summary ============" << std::endl;
        std::cout << "MPI Ranks: " << world_size_ << std::endl;
        std::cout << "Total PIDs: " << all_pids_.size() << std::endl;
        std::cout << "Total process graphs: " << total_graphs << std::endl;
        std::cout << "Total events: " << total_events << std::endl;
        std::cout << "================================================\n" << std::endl;
    }
    
    // Each rank prints its summary
    for (int r = 0; r < world_size_; r++) {
        if (r == rank_) {
            std::cout << "[Rank " << rank_ << "] Local Summary:" << std::endl;
            std::cout << "  Assigned PIDs: " << assigned_pids_.size() << std::endl;
            std::cout << "  Process graphs: " << local_graphs << std::endl;
            std::cout << "  Events: " << local_events << std::endl;
            std::flush(std::cout);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
#else
    std::cout << "\n============ Call Graph Summary ============" << std::endl;
    std::cout << "Total PIDs: " << all_pids_.size() << std::endl;
    std::cout << "Total process graphs: " << local_graphs << std::endl;
    std::cout << "Total events: " << local_events << std::endl;
    std::cout << "============================================\n" << std::endl;
#endif
}

} // namespace dftracer::utils::call_graph
