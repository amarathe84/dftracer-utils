#include <dftracer/utils/call_graph/call_graph.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <yyjson.h>

namespace dftracer::utils::call_graph {

CallGraph::CallGraph(const std::string& log_file) {
    if (!load(log_file)) {
        std::cerr << "Failed to load call graph from: " << log_file << std::endl;
    }
}

CallGraph::~CallGraph() {}

bool CallGraph::load(const std::string& trace_file) {
    std::ifstream file(trace_file);
    if (!file.is_open()) {
        std::cerr << "cant open trace file: " << trace_file << std::endl;
        return false;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        // skip brackets and empty lines
        if (line.empty() || line == "[" || line == "]") {
            continue;
        }
        
        // remove trailing comma
        if (!line.empty() && line.back() == ',') {
            line.pop_back();
        }
        
        if (!process_trace(line)) {
            std::cerr << "failed to parse line: " << line << std::endl;
        }
    }
    
    // build parent child relationships after all traces loaded
    build_hierarchy();
    
    return true;
}

void CallGraph::build_hierarchy() {
    // build parent child relationships
    for (auto& [key, graph] : process_graphs_) {
        std::vector<std::shared_ptr<FunctionCall>> sorted_calls;
        for (auto& [id, call] : graph->calls) {
            sorted_calls.push_back(call);
        }
        
        // sort by start time to build hierarchy
        std::sort(sorted_calls.begin(), sorted_calls.end(), 
                  [](const auto& a, const auto& b) {
                      return a->start_time < b->start_time;
                  });
        
        // find parents for each call
        for (auto& call : sorted_calls) {
            bool found_parent = false;
            
            // look for parent that contains this call
            for (auto& potential_parent : sorted_calls) {
                if (potential_parent->id == call->id) continue;
                
                std::uint64_t parent_end = potential_parent->start_time + potential_parent->duration;
                
                // check if call is inside parent timespan and level is correct
                if (call->start_time >= potential_parent->start_time &&
                    (call->start_time + call->duration) <= parent_end &&
                    call->level > potential_parent->level) {
                    
                    // find closest parent by level
                    if (!found_parent || 
                        potential_parent->level > graph->calls[call->parent_id]->level) {
                        call->parent_id = potential_parent->id;
                        found_parent = true;
                    }
                }
            }
            
            // add to parent children or root
            if (found_parent) {
                graph->calls[call->parent_id]->children.push_back(call->id);
            } else {
                graph->root_calls.push_back(call->id);
            }
        }
    }
}

bool CallGraph::process_trace(const std::string& line) {
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
    }
    
    // Create composite key
    ProcessKey key(static_cast<std::uint32_t>(pid), tid, node_id);
    
    // make sure process graph exists
    if (process_graphs_.find(key) == process_graphs_.end()) {
        process_graphs_[key] = std::make_unique<ProcessCallGraph>();
        process_graphs_[key]->key = key;
    }
    
    // create function call
    auto call = std::make_shared<FunctionCall>();
    call->id = call_id;
    call->name = name;
    call->category = category;
    call->start_time = start_time;
    call->duration = duration;
    call->level = level;
    call->parent_id = 0; // will be set later
    
    // store args
    if (args_val && yyjson_is_obj(args_val)) {
        yyjson_obj_iter iter;
        yyjson_obj_iter_init(args_val, &iter);
        yyjson_val* arg_key, *arg_val;
        while ((arg_key = yyjson_obj_iter_next(&iter))) {
            arg_val = yyjson_obj_iter_get_val(arg_key);
            if (yyjson_is_str(arg_val)) {
                call->args[yyjson_get_str(arg_key)] = yyjson_get_str(arg_val);
            } else if (yyjson_is_int(arg_val)) {
                call->args[yyjson_get_str(arg_key)] = std::to_string(yyjson_get_int(arg_val));
            } else if (yyjson_is_uint(arg_val)) {
                call->args[yyjson_get_str(arg_key)] = std::to_string(yyjson_get_uint(arg_val));
            }
        }
    }
    
    // add to process graph
    ProcessCallGraph* graph = process_graphs_[key].get();
    graph->calls[call_id] = call;
    graph->call_sequence.push_back(call_id);
    
    yyjson_doc_free(doc);
    return true;
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
    std::cout << call->name << " [" << call->category << "] "
              << "level=" << call->level << " "
              << "dur=" << call->duration << "us "
              << "ts=" << call->start_time << std::endl;
    
    // print children
    for (std::uint64_t child_id : call->children) {
        print_calls_recursive(graph, child_id, indent + 1);
    }
}

} // namespace dftracer::utils::call_graph