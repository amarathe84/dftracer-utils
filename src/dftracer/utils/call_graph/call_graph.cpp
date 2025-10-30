#include <dftracer/utils/call_graph/call_graph.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <yyjson.h>
#include <filesystem>

namespace dftracer::utils::call_graph {

// ============================================================================
// CallGraphNode Implementation
// ============================================================================

CallGraphNode::CallGraphNode()
    : id_(0)
    , name_()
    , category_()
    , start_time_(0)
    , duration_(0)
    , level_(0)
    , parent_id_(0)
    , args_()
    , children_()
    , initialized_(false)
    , cleaned_up_(false) {
}

CallGraphNode::CallGraphNode(std::uint64_t id, const std::string& name, const std::string& category)
    : id_(id)
    , name_(name)
    , category_(category)
    , start_time_(0)
    , duration_(0)
    , level_(0)
    , parent_id_(0)
    , args_()
    , children_()
    , initialized_(false)
    , cleaned_up_(false) {
}

CallGraphNode::~CallGraphNode() {
    if (!cleaned_up_) {
        cleanup();
    }
    // Clear all state
    id_ = 0;
    name_.clear();
    category_.clear();
    start_time_ = 0;
    duration_ = 0;
    level_ = 0;
    parent_id_ = 0;
    args_.clear();
    children_.clear();
    initialized_ = false;
    cleaned_up_ = true;
}

CallGraphNode::CallGraphNode(CallGraphNode&& other) noexcept
    : id_(other.id_)
    , name_(std::move(other.name_))
    , category_(std::move(other.category_))
    , start_time_(other.start_time_)
    , duration_(other.duration_)
    , level_(other.level_)
    , parent_id_(other.parent_id_)
    , args_(std::move(other.args_))
    , children_(std::move(other.children_))
    , initialized_(other.initialized_)
    , cleaned_up_(other.cleaned_up_) {
    // Reset other
    other.id_ = 0;
    other.start_time_ = 0;
    other.duration_ = 0;
    other.level_ = 0;
    other.parent_id_ = 0;
    other.initialized_ = false;
    other.cleaned_up_ = true;
}

CallGraphNode& CallGraphNode::operator=(CallGraphNode&& other) noexcept {
    if (this != &other) {
        // Clean up current resources
        if (!cleaned_up_) {
            cleanup();
        }
        
        // Move from other
        id_ = other.id_;
        name_ = std::move(other.name_);
        category_ = std::move(other.category_);
        start_time_ = other.start_time_;
        duration_ = other.duration_;
        level_ = other.level_;
        parent_id_ = other.parent_id_;
        args_ = std::move(other.args_);
        children_ = std::move(other.children_);
        initialized_ = other.initialized_;
        cleaned_up_ = other.cleaned_up_;
        
        // Reset other
        other.id_ = 0;
        other.start_time_ = 0;
        other.duration_ = 0;
        other.level_ = 0;
        other.parent_id_ = 0;
        other.initialized_ = false;
        other.cleaned_up_ = true;
    }
    return *this;
}

void CallGraphNode::initialize(std::uint64_t id, const std::string& name, const std::string& category,
                               std::uint64_t start_time, std::uint64_t duration, int level) {
    id_ = id;
    name_ = name;
    category_ = category;
    start_time_ = start_time;
    duration_ = duration;
    level_ = level;
    parent_id_ = 0;
    args_.clear();
    children_.clear();
    initialized_ = true;
    cleaned_up_ = false;
}

void CallGraphNode::cleanup() {
    if (cleaned_up_) {
        return;
    }
    
    // Clear containers to free memory
    args_.clear();
    children_.clear();
    name_.clear();
    category_.clear();
    
    cleaned_up_ = true;
}

// ============================================================================
// CallGraphFactory Implementation
// ============================================================================

CallGraphFactory::CallGraphFactory()
    : node_count_(0)
    , initialized_(false)
    , cleaned_up_(false)
    , managed_nodes_() {
}

CallGraphFactory::~CallGraphFactory() {
    if (!cleaned_up_) {
        cleanup();
    }
    node_count_ = 0;
    initialized_ = false;
    cleaned_up_ = true;
    managed_nodes_.clear();
}

void CallGraphFactory::initialize() {
    node_count_ = 0;
    managed_nodes_.clear();
    initialized_ = true;
    cleaned_up_ = false;
}

void CallGraphFactory::cleanup() {
    if (cleaned_up_) {
        return;
    }
    
    // Clean up all managed nodes
    for (auto& node : managed_nodes_) {
        if (node) {
            node->cleanup();
        }
    }
    managed_nodes_.clear();
    node_count_ = 0;
    cleaned_up_ = true;
}

std::shared_ptr<CallGraphNode> CallGraphFactory::create_node(
    std::uint64_t id,
    const std::string& name,
    const std::string& category,
    std::uint64_t start_time,
    std::uint64_t duration,
    int level,
    const std::unordered_map<std::string, std::string>& args) {
    
    auto node = std::make_shared<CallGraphNode>(id, name, category);
    node->initialize(id, name, category, start_time, duration, level);
    node->set_args(args);
    
    // Track the node for cleanup
    managed_nodes_.push_back(node);
    node_count_++;
    
    return node;
}

// ============================================================================
// TraceReader Implementation
// ============================================================================

bool TraceReader::read(const std::string& trace_file, CallGraph& graph) {
    std::ifstream file(trace_file);
    if (!file.is_open()) {
        std::cerr << "cant open trace file: " << trace_file << std::endl;
        return false;
    }
    
    std::cout << "reading trace file: " << trace_file << std::endl;
    
    std::string line;
    size_t line_count = 0;
    size_t processed = 0;
    size_t report_interval = 10000;
    
    while (std::getline(file, line)) {
        line_count++;
        
        // progress indicator
        if (line_count % report_interval == 0) {
            std::cout << "  processed " << line_count << " lines, " << processed << " traces..." << std::endl;
        }
        
        // skip brackets and empty lines
        if (line.empty() || line == "[" || line == "]") {
            continue;
        }
        
        // remove trailing comma
        if (!line.empty() && line.back() == ',') {
            line.pop_back();
        }
        
        if (process_trace_line(line, graph)) {
            processed++;
        } else {
            // Don't spam errors for metadata entries
            if (line_count < 10) {
                std::cerr << "failed to parse line " << line_count << " in " << trace_file << std::endl;
            }
        }
    }
    
    std::cout << "processed " << processed << " trace entries from " << trace_file << std::endl;
    
    return true;
}

bool TraceReader::read_multiple(const std::vector<std::string>& trace_files, CallGraph& graph) {
    bool all_success = true;
    
    std::cout << "reading " << trace_files.size() << " trace files..." << std::endl;
    
    size_t file_num = 0;
    for (const auto& file : trace_files) {
        file_num++;
        std::cout << "[" << file_num << "/" << trace_files.size() << "] ";
        if (!read(file, graph)) {
            std::cerr << "failed to read: " << file << std::endl;
            all_success = false;
        }
    }
    
    // build parent child relationships after all traces loaded
    std::cout << "building call hierarchy for " << graph.size() << " process/thread/node combinations..." << std::endl;
    graph.build_hierarchy();
    
    return all_success;
}

bool TraceReader::read_directory(const std::string& directory, const std::string& pattern, CallGraph& graph) {
    namespace fs = std::filesystem;
    
    if (!fs::exists(directory) || !fs::is_directory(directory)) {
        std::cerr << "directory does not exist: " << directory << std::endl;
        return false;
    }
    
    std::vector<std::string> trace_files;
    
    // collect all matching files
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().filename().string();
            
            // simple pattern matching (for now, just check file extension)
            if (pattern == "*" || filename.find(pattern.substr(1)) != std::string::npos) {
                trace_files.push_back(entry.path().string());
            }
        }
    }
    
    if (trace_files.empty()) {
        std::cerr << "no trace files found in " << directory << " matching " << pattern << std::endl;
        return false;
    }
    
    // sort files for consistent processing order
    std::sort(trace_files.begin(), trace_files.end());
    
    std::cout << "found " << trace_files.size() << " trace files in " << directory << std::endl;
    
    return read_multiple(trace_files, graph);
}

bool TraceReader::process_trace_line(const std::string& line, CallGraph& graph) {
    yyjson_doc* doc = yyjson_read(line.c_str(), line.length(), 0);
    if (!doc) {
        return false;
    }
    
    yyjson_val* root = yyjson_doc_get_root(doc);
    if (!root) {
        yyjson_doc_free(doc);
        return false;
    }
    
    // get basic fields
    yyjson_val* id_val = yyjson_obj_get(root, "id");
    yyjson_val* name_val = yyjson_obj_get(root, "name");
    yyjson_val* cat_val = yyjson_obj_get(root, "cat");
    yyjson_val* pid_val = yyjson_obj_get(root, "pid");
    yyjson_val* ph_val = yyjson_obj_get(root, "ph");
    yyjson_val* ts_val = yyjson_obj_get(root, "ts");
    yyjson_val* dur_val = yyjson_obj_get(root, "dur");
    yyjson_val* args_val = yyjson_obj_get(root, "args");
    
    // skip metadata entries
    if (!ph_val || !yyjson_is_str(ph_val) || strcmp(yyjson_get_str(ph_val), "X") != 0) {
        yyjson_doc_free(doc);
        return true; // not an error just skip
    }
    
    if (!id_val || !name_val || !pid_val || !ts_val) {
        yyjson_doc_free(doc);
        return false;
    }
    
    std::uint64_t call_id = yyjson_get_uint(id_val);
    std::uint64_t pid = yyjson_get_uint(pid_val);
    std::string name = yyjson_get_str(name_val);
    std::string category = cat_val ? yyjson_get_str(cat_val) : "";
    std::uint64_t start_time = yyjson_get_uint(ts_val);
    std::uint64_t duration = dur_val ? yyjson_get_uint(dur_val) : 0;
    
    // get level, tid, and node_id from args
    int level = 0;
    std::uint32_t tid = 0;
    std::uint32_t node_id = 0;
    
    // Collect all args
    std::unordered_map<std::string, std::string> args;
    
    if (args_val && yyjson_is_obj(args_val)) {
        yyjson_val* level_val = yyjson_obj_get(args_val, "level");
        if (level_val) {
            level = yyjson_get_int(level_val);
        }
        
        yyjson_val* tid_val = yyjson_obj_get(args_val, "tid");
        if (tid_val) {
            tid = static_cast<std::uint32_t>(yyjson_get_uint(tid_val));
        }
        
        yyjson_val* node_val = yyjson_obj_get(args_val, "node_id");
        if (node_val) {
            node_id = static_cast<std::uint32_t>(yyjson_get_uint(node_val));
        }
        
        // Store all args
        yyjson_obj_iter iter;
        yyjson_obj_iter_init(args_val, &iter);
        yyjson_val* arg_key, *arg_val;
        while ((arg_key = yyjson_obj_iter_next(&iter))) {
            arg_val = yyjson_obj_iter_get_val(arg_key);
            if (yyjson_is_str(arg_val)) {
                args[yyjson_get_str(arg_key)] = yyjson_get_str(arg_val);
            } else if (yyjson_is_int(arg_val)) {
                args[yyjson_get_str(arg_key)] = std::to_string(yyjson_get_int(arg_val));
            } else if (yyjson_is_uint(arg_val)) {
                args[yyjson_get_str(arg_key)] = std::to_string(yyjson_get_uint(arg_val));
            }
        }
    }
    
    // Create function call using factory
    ProcessKey key(static_cast<std::uint32_t>(pid), tid, node_id);
    auto call = graph.get_factory().create_node(call_id, name, category, start_time, duration, level, args);
    
    // Add call to graph
    graph.add_call(key, call);
    
    yyjson_doc_free(doc);
    return true;
}

// ============================================================================
// CallGraph Implementation
// ============================================================================

CallGraph::CallGraph()
    : process_graphs_()
    , factory_()
    , log_file_()
    , initialized_(false)
    , cleaned_up_(false) {
}

CallGraph::CallGraph(const std::string& log_file)
    : process_graphs_()
    , factory_()
    , log_file_(log_file)
    , initialized_(false)
    , cleaned_up_(false) {
}

CallGraph::~CallGraph() {
    if (!cleaned_up_) {
        cleanup();
    }
    // Clear all state
    process_graphs_.clear();
    log_file_.clear();
    initialized_ = false;
    cleaned_up_ = true;
}

void CallGraph::initialize() {
    factory_.initialize();
    process_graphs_.clear();
    initialized_ = true;
    cleaned_up_ = false;
}

void CallGraph::cleanup() {
    if (cleaned_up_) {
        return;
    }
    
    // Clean up all process graphs
    for (auto& [key, graph] : process_graphs_) {
        if (graph) {
            graph->calls.clear();
            graph->root_calls.clear();
            graph->call_sequence.clear();
        }
    }
    process_graphs_.clear();
    
    // Clean up factory
    factory_.cleanup();
    
    cleaned_up_ = true;
}

bool CallGraph::load(const std::string& trace_file) {
    if (!initialized_) {
        initialize();
    }
    log_file_ = trace_file;
    TraceReader reader;
    return reader.read(trace_file, *this);
}

void CallGraph::add_call(const ProcessKey& key, std::shared_ptr<CallGraphNode> call) {
    // make sure process graph exists
    if (process_graphs_.find(key) == process_graphs_.end()) {
        process_graphs_[key] = std::make_unique<ProcessCallGraph>();
        process_graphs_[key]->key = key;
    }
    
    ProcessCallGraph* graph = process_graphs_[key].get();
    graph->calls[call->get_id()] = call;
    graph->call_sequence.push_back(call->get_id());
}

void CallGraph::build_hierarchy() {
    std::cout << "building hierarchy for " << process_graphs_.size() << " process graphs..." << std::endl;
    
    size_t count = 0;
    for (auto& [key, graph] : process_graphs_) {
        count++;
        if (count % 10 == 0 || count == process_graphs_.size()) {
            std::cout << "  processed " << count << "/" << process_graphs_.size() << " processes..." << std::endl;
        }
        build_hierarchy_internal(graph.get());
    }
    
    std::cout << "hierarchy building complete" << std::endl;
}

void CallGraph::build_hierarchy_for_process(const ProcessKey& key) {
    auto it = process_graphs_.find(key);
    if (it != process_graphs_.end()) {
        build_hierarchy_internal(it->second.get());
    }
}

void CallGraph::build_hierarchy_internal(ProcessCallGraph* graph) {
    // Skip if already built (root_calls is populated)
    if (!graph->root_calls.empty()) {
        return;
    }
    
    std::vector<std::shared_ptr<CallGraphNode>> sorted_calls;
    sorted_calls.reserve(graph->calls.size());
    
    for (auto& [id, call] : graph->calls) {
        sorted_calls.push_back(call);
    }
    
    // sort by start time to build hierarchy
    std::sort(sorted_calls.begin(), sorted_calls.end(), 
              [](const auto& a, const auto& b) {
                  return a->get_start_time() < b->get_start_time();
              });
    
    // find parents for each call
    for (auto& call : sorted_calls) {
        bool found_parent = false;
        
        // look for parent that contains this call
        for (auto& potential_parent : sorted_calls) {
            if (potential_parent->get_id() == call->get_id()) continue;
            
            std::uint64_t parent_end = potential_parent->get_start_time() + potential_parent->get_duration();
            
            // check if call is inside parent timespan and level is correct
            if (call->get_start_time() >= potential_parent->get_start_time() &&
                (call->get_start_time() + call->get_duration()) <= parent_end &&
                call->get_level() > potential_parent->get_level()) {
                
                // find closest parent by level
                if (!found_parent || 
                    potential_parent->get_level() > graph->calls[call->get_parent_id()]->get_level()) {
                    call->set_parent_id(potential_parent->get_id());
                    found_parent = true;
                }
            }
        }
        
        // add to parent children or root
        if (found_parent) {
            graph->calls[call->get_parent_id()]->add_child(call->get_id());
        } else {
            graph->root_calls.push_back(call->get_id());
        }
    }
}

ProcessCallGraph* CallGraph::get(const ProcessKey& key) {
    auto it = process_graphs_.find(key);
    if (it != process_graphs_.end()) {
        return it->second.get();
    }
    return nullptr;
}

ProcessCallGraph* CallGraph::get(std::uint32_t pid, std::uint32_t tid, std::uint32_t node_id) {
    return get(ProcessKey(pid, tid, node_id));
}

ProcessCallGraph& CallGraph::operator[](const ProcessKey& key) {
    auto it = process_graphs_.find(key);
    if (it == process_graphs_.end()) {
        // Create new process graph if it doesn't exist
        process_graphs_[key] = std::make_unique<ProcessCallGraph>();
        process_graphs_[key]->key = key;
        return *process_graphs_[key];
    }
    return *it->second;
}

std::vector<ProcessKey> CallGraph::keys() const {
    std::vector<ProcessKey> result;
    result.reserve(process_graphs_.size());
    for (const auto& [key, graph] : process_graphs_) {
        result.push_back(key);
    }
    return result;
}

void CallGraph::print(const ProcessKey& key) const {
    auto it = process_graphs_.find(key);
    if (it == process_graphs_.end()) {
        std::cout << "no graph for process key (pid=" << key.pid 
                  << ", tid=" << key.tid << ", node=" << key.node_id << ")" << std::endl;
        return;
    }
    
    const ProcessCallGraph& graph = *it->second;
    std::cout << "call graph for process key (pid=" << key.pid 
              << ", tid=" << key.tid << ", node=" << key.node_id << ")" << std::endl;
    std::cout << "total calls: " << graph.calls.size() << std::endl;
    std::cout << std::endl;
    
    // print root calls first
    for (std::uint64_t root_id : graph.root_calls) {
        print_calls_recursive(graph, root_id, 0);
    }
}

void CallGraph::print(std::uint32_t pid, std::uint32_t tid, std::uint32_t node_id) const {
    print(ProcessKey(pid, tid, node_id));
}

void CallGraph::print_calls_recursive(const ProcessCallGraph& graph, std::uint64_t call_id, int indent) const {
    auto it = graph.calls.find(call_id);
    if (it == graph.calls.end()) {
        return;
    }
    
    const auto& call = it->second;
    
    // print indentation
    for (int i = 0; i < indent; i++) {
        std::cout << "  ";
    }
    
    // print call info
    std::cout << call->get_name() << " [" << call->get_category() << "] "
              << "level=" << call->get_level() << " "
              << "dur=" << call->get_duration() << "us "
              << "ts=" << call->get_start_time() << std::endl;
    
    // print children
    for (std::uint64_t child_id : call->get_children()) {
        print_calls_recursive(graph, child_id, indent + 1);
    }
}

} // namespace dftracer::utils::call_graph