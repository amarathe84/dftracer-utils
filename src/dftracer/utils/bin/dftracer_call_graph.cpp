#include <dftracer/utils/call_graph/call_graph.h>
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <set>
#include <map>

using namespace dftracer::utils::call_graph;

void print_usage(const char* program_name) {
    std::cerr << "usage: " << program_name << " <trace_file_or_directory> [options]" << std::endl;
    std::cerr << "  trace_file_or_directory: single file, multiple files (space-separated), or directory" << std::endl;
    std::cerr << "  options:" << std::endl;
    std::cerr << "    --pattern <pattern>  : file pattern when reading directory (default: *.pfw)" << std::endl;
    std::cerr << "    --limit <n>          : limit to first N trace files" << std::endl;
    std::cerr << "    --summary            : show summary statistics only" << std::endl;
    std::cerr << "    --detailed           : show detailed call graphs (default)" << std::endl;
}

void print_summary(const CallGraph& call_graph) {
    auto process_keys = call_graph.keys();
    
    // collect statistics
    std::set<std::uint32_t> unique_pids;
    std::set<std::uint32_t> unique_tids;
    std::set<std::uint32_t> unique_nodes;
    std::map<std::uint32_t, std::set<std::uint32_t>> pids_per_node;
    
    size_t total_calls = 0;
    
    for (const auto& key : process_keys) {
        unique_pids.insert(key.pid);
        unique_tids.insert(key.tid);
        unique_nodes.insert(key.node_id);
        pids_per_node[key.node_id].insert(key.pid);
        
        auto* graph = const_cast<CallGraph&>(call_graph).get(key);
        if (graph) {
            total_calls += graph->calls.size();
        }
    }
    
    std::cout << "\n============ SUMMARY ============" << std::endl;
    std::cout << "Total process/thread/node combinations: " << process_keys.size() << std::endl;
    std::cout << "Unique nodes: " << unique_nodes.size() << std::endl;
    std::cout << "Unique processes: " << unique_pids.size() << std::endl;
    std::cout << "Unique threads: " << unique_tids.size() << std::endl;
    std::cout << "Total function calls: " << total_calls << std::endl;
    
    std::cout << "\nProcesses per node:" << std::endl;
    for (const auto& [node_id, pids] : pids_per_node) {
        std::cout << "  Node " << node_id << ": " << pids.size() << " process(es)" << std::endl;
    }
    std::cout << "================================\n" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }
    
    std::string input_path = argv[1];
    std::string pattern = "*.pfw";
    bool summary_only = false;
    bool detailed = true;
    
    // parse options
    for (int i = 2; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--pattern" && i + 1 < argc) {
            pattern = argv[++i];
        } else if (arg == "--summary") {
            summary_only = true;
            detailed = false;
        } else if (arg == "--detailed") {
            detailed = true;
            summary_only = false;
        }
    }
    
    CallGraph call_graph;
    TraceReader reader;
    
    namespace fs = std::filesystem;
    
    // determine if input is file or directory
    if (fs::is_directory(input_path)) {
        std::cout << "loading traces from directory: " << input_path << std::endl;
        if (!reader.read_directory(input_path, pattern, call_graph)) {
            std::cerr << "failed to load traces from directory" << std::endl;
            return 1;
        }
    } else if (fs::is_regular_file(input_path)) {
        std::cout << "loading trace from file: " << input_path << std::endl;
        if (!reader.read(input_path, call_graph)) {
            std::cerr << "failed to load trace file" << std::endl;
            return 1;
        }
    } else {
        std::cerr << "input path is neither a file nor directory: " << input_path << std::endl;
        return 1;
    }
    
    if (call_graph.empty()) {
        std::cerr << "failed to load traces or no data found" << std::endl;
        return 1;
    }
    
    std::cout << "traces loaded successfully" << std::endl;
    
    // print summary
    print_summary(call_graph);
    
    // print detailed call graphs if requested
    if (detailed && !summary_only) {
        auto process_keys = call_graph.keys();
        
        for (const auto& key : process_keys) {
            std::cout << std::endl;
            std::cout << "========================================" << std::endl;
            call_graph.print(key);
            std::cout << "========================================" << std::endl;
        }
    }
    
    return 0;
}