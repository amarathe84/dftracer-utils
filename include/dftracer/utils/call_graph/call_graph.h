#ifndef DFTRACER_UTILS_CALL_GRAPH_H
#define DFTRACER_UTILS_CALL_GRAPH_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <cstdint>

namespace dftracer::utils::call_graph {

// holds info for each function call
struct FunctionCall {
    std::uint64_t id;
    std::string name;
    std::string category;
    std::uint64_t start_time;
    std::uint64_t duration;
    int level;
    std::uint64_t parent_id;
    std::unordered_map<std::string, std::string> args; // extra stuff from trace
    std::vector<std::uint64_t> children; // child calls
};

// call graph for one process
struct ProcessCallGraph {
    std::uint64_t process_id;
    std::unordered_map<std::uint64_t, std::shared_ptr<FunctionCall>> calls;
    std::vector<std::uint64_t> root_calls; // top level calls
    std::vector<std::uint64_t> call_sequence; // order they appear in trace
};

// main call graph with all processes
class CallGraph {
public:
    CallGraph();
    ~CallGraph();
    
    // load call graph from trace file
    bool load_from_trace(const std::string& trace_file);
    
    // get call graph for specific process
    ProcessCallGraph* get_process_graph(std::uint64_t pid);
    
    // get all process ids
    std::vector<std::uint64_t> get_process_ids() const;
    
    // print call graph for process
    void print_process_graph(std::uint64_t pid) const;

private:
    std::unordered_map<std::uint64_t, std::unique_ptr<ProcessCallGraph>> process_graphs_;
    
    // parse single trace line
    bool parse_trace_line(const std::string& line);
    
    // print calls recursively 
    void print_calls_recursive(const ProcessCallGraph& graph, std::uint64_t call_id, int indent) const;
};

} // namespace dftracer::utils::call_graph

#endif // DFTRACER_UTILS_CALL_GRAPH_H