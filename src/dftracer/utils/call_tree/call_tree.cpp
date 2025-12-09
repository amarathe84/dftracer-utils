#include <dftracer/utils/call_tree/call_tree.h>
#include <dftracer/utils/call_tree/call_tree_internal.h>
#include <dftracer/utils/call_tree/json_serializer.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <set>
#include <cstring>
#include <ctime>
#include <unistd.h>

namespace fs = std::filesystem;

namespace dftracer::utils::call_tree {

namespace internal {

/**
 * Internal implementation class (PIMPL pattern)
 * Hides complex CallTree internals from public API
 */
class CallTreeImpl {
public:
    CallTree graph;
    std::vector<std::string> trace_files;
    std::string trace_directory;
    std::string output_path;
    bool is_generated;
    
    CallTreeImpl() : is_generated(false) {
        graph.initialize();
    }
    
    ~CallTreeImpl() {
        graph.cleanup();
    }
    
    bool find_trace_files(const std::string& dir, const std::string& pattern) {
        trace_files.clear();
        trace_directory = dir;
        
        if (!fs::exists(dir) || !fs::is_directory(dir)) {
            std::cerr << "Error: Directory not found: " << dir << std::endl;
            return false;
        }
        
        // Recursively find matching files
        for (const auto& entry : fs::recursive_directory_iterator(dir)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                
                // Simple pattern matching for *.ext
                bool matches = false;
                if (pattern == "*") {
                    matches = true;
                } else if (pattern.front() == '*') {
                    std::string suffix = pattern.substr(1);
                    matches = (filename.size() >= suffix.size() &&
                              filename.substr(filename.size() - suffix.size()) == suffix);
                } else {
                    matches = (filename.find(pattern) != std::string::npos);
                }
                
                if (matches) {
                    trace_files.push_back(entry.path().string());
                }
            }
        }
        
        std::sort(trace_files.begin(), trace_files.end());
        return !trace_files.empty();
    }
    
    bool load_traces() {
        if (trace_files.empty()) {
            std::cerr << "Error: No trace files to load" << std::endl;
            return false;
        }
        
        TraceReader reader;
        bool success = reader.read_multiple(trace_files, graph);
        
        if (success) {
            graph.build_hierarchy();
        }
        
        return success;
    }
    
    void traverse_depth_first(const ProcessCallTree& process_graph,
                              std::uint64_t node_id,
                              std::vector<CallTreeNodeInfo>& nodes) const {
        auto it = process_graph.calls.find(node_id);
        if (it == process_graph.calls.end()) {
            return;
        }
        
        const auto& node = it->second;
        
        // Add current node
        CallTreeNodeInfo info;
        info.id = node->get_id();
        info.name = node->get_name();
        info.category = node->get_category();
        info.start_time_us = node->get_start_time();
        info.duration_us = node->get_duration();
        info.level = node->get_level();
        info.parent_id = node->get_parent_id();
        info.num_children = node->get_children().size();
        
        nodes.push_back(info);
        
        // Recursively traverse children
        for (std::uint64_t child_id : node->get_children()) {
            traverse_depth_first(process_graph, child_id, nodes);
        }
    }
    
    void print_node_recursive(const ProcessCallTree& process_graph,
                             std::uint64_t node_id,
                             int indent,
                             int max_depth,
                             std::ostream& out) const {
        if (max_depth > 0 && indent >= max_depth) {
            return;
        }
        
        auto it = process_graph.calls.find(node_id);
        if (it == process_graph.calls.end()) {
            return;
        }
        
        const auto& node = it->second;
        
        // Print indentation
        for (int i = 0; i < indent; i++) {
            out << "  ";
        }
        
        // Print node info
        out << node->get_name() << " [" << node->get_category() << "] "
            << "level=" << node->get_level() << " "
            << "dur=" << (node->get_duration() / 1000.0) << "ms "
            << "children=" << node->get_children().size() << std::endl;
        
        // Print children
        for (std::uint64_t child_id : node->get_children()) {
            print_node_recursive(process_graph, child_id, indent + 1, max_depth, out);
        }
    }
    
    void compute_level_stats(const ProcessCallTree& process_graph,
                            std::uint64_t node_id,
                            std::vector<std::uint64_t>& total_time_per_level,
                            std::vector<size_t>& count_per_level,
                            int& max_level,
                            size_t& leaf_count) const {
        auto it = process_graph.calls.find(node_id);
        if (it == process_graph.calls.end()) {
            return;
        }
        
        const auto& node = it->second;
        int level = node->get_level();
        
        if (level >= static_cast<int>(total_time_per_level.size())) {
            total_time_per_level.resize(level + 1, 0);
            count_per_level.resize(level + 1, 0);
        }
        
        total_time_per_level[level] += node->get_duration();
        count_per_level[level]++;
        
        if (level > max_level) {
            max_level = level;
        }
        
        if (node->get_children().empty()) {
            leaf_count++;
        }
        
        for (std::uint64_t child_id : node->get_children()) {
            compute_level_stats(process_graph, child_id, total_time_per_level, 
                               count_per_level, max_level, leaf_count);
        }
    }
};

} // namespace internal

// ============================================================================
// CallTree Public API Implementation
// ============================================================================

CallTree::CallTree() : impl_(std::make_unique<internal::CallTreeImpl>()) {
}

CallTree::~CallTree() = default;

CallTree::CallTree(CallTree&&) noexcept = default;
CallTree& CallTree::operator=(CallTree&&) noexcept = default;

bool CallTree::load_from_directory(const std::string& trace_dir, const std::string& pattern) {
    bool found = impl_->find_trace_files(trace_dir, pattern);
    
    if (found) {
        std::cout << "Found " << impl_->trace_files.size() << " trace files in " 
                  << trace_dir << std::endl;
        
        // Set default output path
        fs::path dir_path(trace_dir);
        std::string dir_name = dir_path.filename().string();
        impl_->output_path = dir_name + ".calltree";
    }
    
    return found;
}

bool CallTree::generate() {
    if (impl_->trace_files.empty()) {
        std::cerr << "Error: No trace files loaded. Call load_from_directory() first." << std::endl;
        return false;
    }
    
    std::cout << "Generating call tree from " << impl_->trace_files.size() 
              << " trace files..." << std::endl;
    
    bool success = impl_->load_traces();
    
    if (success) {
        impl_->is_generated = true;
        std::cout << "Call tree generation complete" << std::endl;
        std::cout << "  Total processes: " << impl_->graph.size() << std::endl;
    } else {
        std::cerr << "Error: Failed to generate call tree" << std::endl;
    }
    
    return success;
}

void CallTree::print_depth_first(int max_depth) const {
    if (!impl_->is_generated) {
        std::cerr << "Error: Call tree not generated. Call generate() first." << std::endl;
        return;
    }
    
    auto keys = impl_->graph.keys();
    
    for (const auto& key : keys) {
        auto* process_graph = impl_->graph.get(key);
        if (!process_graph) continue;
        
        std::cout << "\n=== Process/Thread: PID=" << key.pid 
                  << ", TID=" << key.tid << ", Node=" << key.node_id << " ===" << std::endl;
        std::cout << "Total nodes: " << process_graph->calls.size() << std::endl;
        std::cout << "Root calls: " << process_graph->root_calls.size() << std::endl;
        std::cout << std::endl;
        
        for (std::uint64_t root_id : process_graph->root_calls) {
            impl_->print_node_recursive(*process_graph, root_id, 0, max_depth, std::cout);
        }
    }
}

bool CallTree::print_depth_first_to_file(const std::string& filename, int max_depth) const {
    if (!impl_->is_generated) {
        std::cerr << "Error: Call tree not generated. Call generate() first." << std::endl;
        return false;
    }
    
    std::ofstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file for writing: " << filename << std::endl;
        return false;
    }
    
    auto keys = impl_->graph.keys();
    
    for (const auto& key : keys) {
        auto* process_graph = impl_->graph.get(key);
        if (!process_graph) continue;
        
        file << "\n=== Process/Thread: PID=" << key.pid 
             << ", TID=" << key.tid << ", Node=" << key.node_id << " ===" << std::endl;
        file << "Total nodes: " << process_graph->calls.size() << std::endl;
        file << "Root calls: " << process_graph->root_calls.size() << std::endl;
        file << std::endl;
        
        for (std::uint64_t root_id : process_graph->root_calls) {
            impl_->print_node_recursive(*process_graph, root_id, 0, max_depth, file);
        }
    }
    
    file.close();
    std::cout << "Call tree printed to: " << filename << std::endl;
    return true;
}

std::vector<CallTreeNodeInfo> CallTree::get_nodes_depth_first() const {
    std::vector<CallTreeNodeInfo> all_nodes;
    
    if (!impl_->is_generated) {
        std::cerr << "Error: Call tree not generated. Call generate() first." << std::endl;
        return all_nodes;
    }
    
    auto keys = impl_->graph.keys();
    
    for (const auto& key : keys) {
        auto* process_graph = impl_->graph.get(key);
        if (!process_graph) continue;
        
        for (std::uint64_t root_id : process_graph->root_calls) {
            impl_->traverse_depth_first(*process_graph, root_id, all_nodes);
        }
    }
    
    return all_nodes;
}

std::string CallTree::get_output_path() const {
    return impl_->output_path;
}

void CallTree::set_output_path(const std::string& path) {
    impl_->output_path = path;
}

bool CallTree::save_to_file(const std::string& filename) const {
    if (!impl_->is_generated) {
        std::cerr << "Error: Call tree not generated. Call generate() first." << std::endl;
        return false;
    }
    
    std::string output_file = filename.empty() ? impl_->output_path : filename;
    
    std::ofstream file(output_file, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file for writing: " << output_file << std::endl;
        return false;
    }
    
    // Write header
    const char magic[8] = {'C', 'A', 'L', 'L', 'T', 'R', 'E', 'E'};
    file.write(magic, 8);
    
    std::uint32_t version = 1;
    file.write(reinterpret_cast<const char*>(&version), sizeof(version));
    
    // Get nodes in depth-first order
    auto nodes = get_nodes_depth_first();
    
    std::uint64_t num_nodes = nodes.size();
    file.write(reinterpret_cast<const char*>(&num_nodes), sizeof(num_nodes));
    
    // Write each node
    for (const auto& node : nodes) {
        file.write(reinterpret_cast<const char*>(&node.id), sizeof(node.id));
        
        std::uint32_t name_len = node.name.size();
        file.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
        file.write(node.name.data(), name_len);
        
        std::uint32_t cat_len = node.category.size();
        file.write(reinterpret_cast<const char*>(&cat_len), sizeof(cat_len));
        file.write(node.category.data(), cat_len);
        
        file.write(reinterpret_cast<const char*>(&node.start_time_us), sizeof(node.start_time_us));
        file.write(reinterpret_cast<const char*>(&node.duration_us), sizeof(node.duration_us));
        file.write(reinterpret_cast<const char*>(&node.level), sizeof(node.level));
        file.write(reinterpret_cast<const char*>(&node.parent_id), sizeof(node.parent_id));
        
        std::uint64_t num_children = node.num_children;
        file.write(reinterpret_cast<const char*>(&num_children), sizeof(num_children));
    }
    
    file.close();
    std::cout << "Call tree saved to: " << output_file << std::endl;
    std::cout << "  Nodes written: " << nodes.size() << std::endl;
    
    return true;
}

bool CallTree::save_to_json(const std::string& filename) const {
    if (!impl_->is_generated) {
        std::cerr << "Error: Call tree not generated. Call generate() first." << std::endl;
        return false;
    }
    
    // Determine output file - use .pfw extension for compatibility with DFTracer tools
    std::string output_file = filename;
    if (output_file.empty()) {
        // Replace .calltree extension with .pfw if present, otherwise append
        std::string base = impl_->output_path;
        if (base.size() >= 9 && base.substr(base.size() - 9) == ".calltree") {
            base = base.substr(0, base.size() - 9);
        }
        output_file = base + ".pfw";
    }
    
    std::ofstream file(output_file);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file for writing: " << output_file << std::endl;
        return false;
    }
    
    std::cout << "Serializing call tree to JSON (Chrome Tracing format)..." << std::endl;
    
    // Create JSON serializer
    internal::JsonSerializer serializer;
    
    // Buffer for serialization (16KB should be enough for most events)
    const size_t BUFFER_SIZE = 16384;
    char buffer[BUFFER_SIZE];
    
    // Get hostname for identification
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    std::string hostname_hash = std::string(hostname);
    
    // Write opening bracket
    size_t written = serializer.initialize(buffer, hostname_hash);
    file.write(buffer, written);
    
    // Write metadata events for file header
    std::time_t now = std::time(nullptr);
    char timestamp[256];
    std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
    
    written = serializer.serialize_metadata(buffer, "timestamp", timestamp, "M", 0, 0, true);
    file.write(buffer, written - 1); // Don't write the newline yet
    file.write(",\n", 2); // Write comma separator
    
    written = serializer.serialize_metadata(buffer, "format", "call_tree", "M", 0, 0, true);
    file.write(buffer, written - 1);
    file.write(",\n", 2);
    
    // Get all process keys
    auto keys = impl_->graph.keys();
    
    // Track event index (similar to DFTracer)
    int event_index = 0;
    size_t total_events = 0;
    
    // Iterate over all processes/threads
    for (const auto& key : keys) {
        auto* process_graph = impl_->graph.get(key);
        if (!process_graph) continue;
        
        // Traverse and serialize nodes in depth-first order
        for (std::uint64_t root_id : process_graph->root_calls) {
            std::vector<std::uint64_t> stack;
            stack.push_back(root_id);
            
            while (!stack.empty()) {
                std::uint64_t node_id = stack.back();
                stack.pop_back();
                
                auto it = process_graph->calls.find(node_id);
                if (it == process_graph->calls.end()) continue;
                
                const auto& node = it->second;
                
                // Serialize this node
                written = serializer.serialize_node(buffer, event_index++, 
                                                   *node, key.pid, key.tid);
                
                // Write to file with comma separator (except last event)
                file.write(buffer, written - 1); // Don't write newline
                
                // Add children to stack in reverse order for depth-first
                const auto& children = node->get_children();
                for (auto it = children.rbegin(); it != children.rend(); ++it) {
                    stack.push_back(*it);
                }
                
                // Write comma separator for next event
                file.write(",\n", 2);
                total_events++;
            }
        }
    }
    
    // Write closing bracket (overwrites the last comma)
    file.seekp(-2, std::ios::cur); // Back up over ",\n"
    file.write("\n", 1); // Just write newline
    
    written = serializer.finalize(buffer, true);
    file.write(buffer, written);
    
    file.close();
    
    std::cout << "Call tree saved to JSON: " << output_file << std::endl;
    std::cout << "  Total events: " << total_events << std::endl;
    std::cout << "  Unique processes: " << keys.size() << std::endl;
    std::cout << "  Format: Chrome Tracing (compatible with Perfetto)" << std::endl;
    
    return true;
}

bool CallTree::load_from_file(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file for reading: " << filename << std::endl;
        return false;
    }
    
    // Read and verify header
    char magic[8];
    file.read(magic, 8);
    
    if (std::memcmp(magic, "CALLTREE", 8) != 0) {
        std::cerr << "Error: Invalid file format" << std::endl;
        return false;
    }
    
    std::uint32_t version;
    file.read(reinterpret_cast<char*>(&version), sizeof(version));
    
    if (version != 1) {
        std::cerr << "Error: Unsupported version: " << version << std::endl;
        return false;
    }
    
    std::uint64_t num_nodes;
    file.read(reinterpret_cast<char*>(&num_nodes), sizeof(num_nodes));
    
    std::cout << "Loading " << num_nodes << " nodes from " << filename << std::endl;
    
    // Note: This is a simplified load that just verifies the file
    // Full reconstruction would require rebuilding the CallTree structure
    
    file.close();
    std::cout << "Call tree file validated successfully" << std::endl;
    
    return true;
}

CallTreeStats CallTree::get_statistics() const {
    CallTreeStats stats;
    
    if (!impl_->is_generated) {
        return stats;
    }
    
    auto keys = impl_->graph.keys();
    stats.num_processes = keys.size();
    
    // Use set to count unique process+thread combinations
    std::set<std::pair<std::uint32_t, std::uint32_t>> unique_process_threads;
    for (const auto& key : keys) {
        unique_process_threads.insert({key.pid, key.tid});
    }
    stats.num_processes = unique_process_threads.size();
    
    std::vector<std::uint64_t> total_time_per_level;
    std::vector<size_t> count_per_level;
    int max_level = 0;
    size_t total_leaf_count = 0;
    size_t total_node_count = 0;
    
    for (const auto& key : keys) {
        auto* process_graph = impl_->graph.get(key);
        if (!process_graph) continue;
        
        total_node_count += process_graph->calls.size();
        
        for (std::uint64_t root_id : process_graph->root_calls) {
            impl_->compute_level_stats(*process_graph, root_id, 
                                      total_time_per_level, count_per_level,
                                      max_level, total_leaf_count);
        }
    }
    
    stats.total_nodes = total_node_count;
    stats.num_levels = max_level + 1;
    stats.num_leaf_nodes = total_leaf_count;
    
    // Compute average time per level
    stats.avg_time_per_level_us.resize(stats.num_levels);
    stats.nodes_per_level.resize(stats.num_levels);
    
    for (size_t i = 0; i < stats.num_levels; i++) {
        stats.nodes_per_level[i] = count_per_level[i];
        if (count_per_level[i] > 0) {
            stats.avg_time_per_level_us[i] = 
                static_cast<double>(total_time_per_level[i]) / count_per_level[i];
        } else {
            stats.avg_time_per_level_us[i] = 0.0;
        }
    }
    
    return stats;
}

void CallTree::print_statistics() const {
    auto stats = get_statistics();
    
    std::cout << "\n============ Call Tree Statistics ============" << std::endl;
    std::cout << "Total nodes:           " << stats.total_nodes << std::endl;
    std::cout << "Number of levels:      " << stats.num_levels << std::endl;
    std::cout << "Leaf nodes:            " << stats.num_leaf_nodes << std::endl;
    std::cout << "Unique processes:      " << stats.num_processes << std::endl;
    std::cout << std::endl;
    
    std::cout << "Per-Level Statistics:" << std::endl;
    std::cout << std::setw(8) << "Level" 
              << std::setw(12) << "Nodes" 
              << std::setw(20) << "Avg Time (ms)" << std::endl;
    std::cout << std::string(40, '-') << std::endl;
    
    for (size_t i = 0; i < stats.num_levels; i++) {
        std::cout << std::setw(8) << i
                  << std::setw(12) << stats.nodes_per_level[i]
                  << std::setw(20) << std::fixed << std::setprecision(3) 
                  << (stats.avg_time_per_level_us[i] / 1000.0) << std::endl;
    }
    
    std::cout << "=============================================\n" << std::endl;
}

bool CallTree::is_generated() const {
    return impl_->is_generated;
}

size_t CallTree::get_num_trace_files() const {
    return impl_->trace_files.size();
}

void CallTree::clear() {
    impl_->graph.cleanup();
    impl_->graph.initialize();
    impl_->trace_files.clear();
    impl_->trace_directory.clear();
    impl_->output_path.clear();
    impl_->is_generated = false;
}

} // namespace dftracer::utils::call_tree
