#include <dftracer/utils/call_graph/call_graph.h>
#include <iostream>
#include <string>

using namespace dftracer::utils::call_graph;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "usage: " << argv[0] << " <trace_file>" << std::endl;
        return 1;
    }
    
    std::string trace_file = argv[1];
    
    std::cout << "loading trace from: " << trace_file << std::endl;
    
    // Constructor-based loading - modern C++ style
    CallGraph call_graph(trace_file);
    
    if (call_graph.empty()) {
        std::cerr << "failed to load trace file or no data found" << std::endl;
        return 1;
    }
    
    std::cout << "trace loaded successfully" << std::endl;
    
    // get all process keys (PID, TID, NodeID combinations)
    auto process_keys = call_graph.keys();
    
    std::cout << "found " << process_keys.size() << " process/thread/node combination(s)" << std::endl;
    
    // print call graph for each process key
    for (const auto& key : process_keys) {
        std::cout << std::endl;
        std::cout << "========================================" << std::endl;
        call_graph.print(key);
        std::cout << "========================================" << std::endl;
    }
    
    return 0;
}