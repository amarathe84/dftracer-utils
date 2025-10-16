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
    
    CallGraph call_graph;
    
    std::cout << "loading trace from: " << trace_file << std::endl;
    
    if (!call_graph.load_from_trace(trace_file)) {
        std::cerr << "failed to load trace file" << std::endl;
        return 1;
    }
    
    std::cout << "trace loaded successfully" << std::endl;
    
    // get all process ids
    auto pids = call_graph.get_process_ids();
    
    std::cout << "found " << pids.size() << " process(es)" << std::endl;
    
    // print call graph for each process
    for (auto pid : pids) {
        std::cout << std::endl;
        std::cout << "========================================" << std::endl;
        call_graph.print_process_graph(pid);
        std::cout << "========================================" << std::endl;
    }
    
    return 0;
}