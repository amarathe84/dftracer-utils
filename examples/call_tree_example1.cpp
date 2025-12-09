/**
 * Example 1: Basic usage of CallTree API with nodes-1 trace
 * Demonstrates all basic operations
 */

#include <dftracer/utils/call_tree/call_tree.h>
#include <iostream>

using namespace dftracer::utils::call_tree;

int main(int argc, char* argv[]) {
    std::cout << "=== CallTree API Example 1: Basic Usage ===" << std::endl;
    std::cout << std::endl;
    
    // Specify trace path
    std::string trace_path = "/g/g92/marathe1/myworkspace/dldl/dftracer-utils/trace_short/cosmoflow_a100/nodes-1";
    if (argc > 1) {
        trace_path = argv[1];
    }
    
    std::cout << "Step 1: Load trace files from directory" << std::endl;
    CallTree tree;
    if (!tree.load_from_directory(trace_path)) {
        std::cerr << "Failed to load traces from: " << trace_path << std::endl;
        return 1;
    }
    std::cout << "  Loaded " << tree.get_num_trace_files() << " trace files" << std::endl;
    std::cout << std::endl;
    
    // Generate call tree
    std::cout << "Step 2: Generate call tree structure" << std::endl;
    if (!tree.generate()) {
        std::cerr << "Failed to generate call tree" << std::endl;
        return 1;
    }
    std::cout << std::endl;
    
    // Print statistics
    std::cout << "Step 3: Print aggregate statistics" << std::endl;
    tree.print_statistics();
    
    // Traverse and print in depth-first order
    std::cout << "Step 4: Traverse and print call tree (depth-first, max depth=3)" << std::endl;
    tree.print_depth_first(3);  // Limit to 3 levels for readability
    std::cout << std::endl;
    
    // Get list of nodes
    std::cout << "Step 5: Get list of nodes in traversal order" << std::endl;
    auto nodes = tree.get_nodes_depth_first();
    std::cout << "  Retrieved " << nodes.size() << " nodes" << std::endl;
    std::cout << "  First 5 nodes:" << std::endl;
    for (size_t i = 0; i < std::min(nodes.size(), size_t(5)); i++) {
        const auto& node = nodes[i];
        std::cout << "    [" << i << "] " << node.name 
                  << " (level=" << node.level 
                  << ", duration=" << (node.duration_us / 1000.0) << "ms)" << std::endl;
    }
    std::cout << std::endl;
    
    // Save to file
    std::cout << "Step 6: Serialize call tree to binary file" << std::endl;
    std::string output_file = tree.get_output_path();
    std::cout << "  Default output path: " << output_file << std::endl;
    
    if (tree.save_to_file()) {
        std::cout << "  Successfully saved!" << std::endl;
    }
    std::cout << std::endl;
    
    // Save to JSON format
    std::cout << "Step 7: Serialize call tree to JSON (Chrome Tracing format)" << std::endl;
    if (tree.save_to_json()) {
        std::cout << "  Successfully saved to JSON!" << std::endl;
    }
    std::cout << std::endl;
    
    // Print tree to text file
    std::cout << "Step 8: Export call tree to text file" << std::endl;
    std::string text_file = "nodes-1_calltree.txt";
    if (tree.print_depth_first_to_file(text_file)) {
        std::cout << "  Exported to: " << text_file << std::endl;
    }
    
    std::cout << "\n=== Example completed successfully ===" << std::endl;
    
    return 0;
}
