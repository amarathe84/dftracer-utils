#include <dftracer/utils/call_graph/call_graph.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <yyjson.h>

namespace dftracer::utils::call_graph {

CallGraph::CallGraph() {}

CallGraph::~CallGraph() {}

bool CallGraph::load_from_trace(const std::string& trace_file) {
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
        
        if (!parse_trace_line(line)) {
            std::cerr << "failed to parse line: " << line << std::endl;
        }
    }
    
    // build parent child relationships
    for (auto& [pid, graph] : process_graphs_) {
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
    
    return true;
}

bool CallGraph::parse_trace_line(const std::string& line) {
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
    
    // get level from args
    int level = 0;
    if (args_val && yyjson_is_obj(args_val)) {
        yyjson_val* level_val = yyjson_obj_get(args_val, "level");
        if (level_val) {
            level = yyjson_get_int(level_val);
        }
    }
    
    // make sure process graph exists
    if (process_graphs_.find(pid) == process_graphs_.end()) {
        process_graphs_[pid] = std::make_unique<ProcessCallGraph>();
        process_graphs_[pid]->process_id = pid;
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
        yyjson_val* key, *val;
        while ((key = yyjson_obj_iter_next(&iter))) {
            val = yyjson_obj_iter_get_val(key);
            if (yyjson_is_str(val)) {
                call->args[yyjson_get_str(key)] = yyjson_get_str(val);
            } else if (yyjson_is_int(val)) {
                call->args[yyjson_get_str(key)] = std::to_string(yyjson_get_int(val));
            } else if (yyjson_is_uint(val)) {
                call->args[yyjson_get_str(key)] = std::to_string(yyjson_get_uint(val));
            }
        }
    }
    
    // add to process graph
    ProcessCallGraph* graph = process_graphs_[pid].get();
    graph->calls[call_id] = call;
    graph->call_sequence.push_back(call_id);
    
    yyjson_doc_free(doc);
    return true;
}

ProcessCallGraph* CallGraph::get_process_graph(std::uint64_t pid) {
    auto it = process_graphs_.find(pid);
    if (it != process_graphs_.end()) {
        return it->second.get();
    }
    return nullptr;
}

std::vector<std::uint64_t> CallGraph::get_process_ids() const {
    std::vector<std::uint64_t> pids;
    for (const auto& [pid, graph] : process_graphs_) {
        pids.push_back(pid);
    }
    return pids;
}

void CallGraph::print_process_graph(std::uint64_t pid) const {
    auto it = process_graphs_.find(pid);
    if (it == process_graphs_.end()) {
        std::cout << "no graph for process " << pid << std::endl;
        return;
    }
    
    const ProcessCallGraph& graph = *it->second;
    std::cout << "call graph for process " << pid << std::endl;
    std::cout << "total calls: " << graph.calls.size() << std::endl;
    std::cout << std::endl;
    
    // print root calls first
    for (std::uint64_t root_id : graph.root_calls) {
        print_calls_recursive(graph, root_id, 0);
    }
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