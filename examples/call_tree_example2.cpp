/**
 * Example 2: Advanced usage with nodes-4 trace
 * Demonstrates handling larger traces with multiple nodes
 */

#include <dftracer/utils/call_tree/call_tree.h>
#include <iostream>
#include <iomanip>
#include <map>

using namespace dftracer::utils::call_tree;

int main(int argc, char* argv[]) {
    std::cout << "=== CallTree API Example 2: Multi-Node Traces ===" << std::endl;
    std::cout << std::endl;
    
    // Specify trace path for nodes-4
    std::string trace_path = "/g/g92/marathe1/myworkspace/dldl/dftracer-utils/trace_short/cosmoflow_a100/nodes-4";
    if (argc > 1) {
        trace_path = argv[1];
    }
    
    std::cout << "Loading traces from: " << trace_path << std::endl;
    std::cout << std::endl;
    
    CallTree tree;
    
    // Load with custom pattern
    if (!tree.load_from_directory(trace_path, "*.pfw.gz")) {
        std::cerr << "Failed to load traces" << std::endl;
        return 1;
    }
    
    std::cout << "Found " << tree.get_num_trace_files() << " trace files" << std::endl;
    std::cout << std::endl;
    
    // Generate call tree
    std::cout << "Generating call tree..." << std::endl;
    if (!tree.generate()) {
        std::cerr << "Failed to generate call tree" << std::endl;
        return 1;
    }
    
    // Get detailed statistics
    std::cout << "\n--- Detailed Statistics ---" << std::endl;
    auto stats = tree.get_statistics();
    
    std::cout << "Total nodes across all processes: " << stats.total_nodes << std::endl;
    std::cout << "Number of tree levels: " << stats.num_levels << std::endl;
    std::cout << "Leaf nodes: " << stats.num_leaf_nodes << std::endl;
    std::cout << "Unique process/thread combinations: " << stats.num_processes << std::endl;
    std::cout << std::endl;
    
    // Analyze per-level information
    std::cout << "--- Per-Level Analysis ---" << std::endl;
    std::cout << std::left << std::setw(10) << "Level" 
              << std::setw(15) << "Node Count" 
              << std::setw(20) << "Avg Time (ms)"
              << std::setw(15) << "% of Total" << std::endl;
    std::cout << std::string(60, '-') << std::endl;
    
    for (size_t i = 0; i < stats.num_levels; i++) {
        double percent = (static_cast<double>(stats.nodes_per_level[i]) / stats.total_nodes) * 100.0;
        std::cout << std::left << std::setw(10) << i
                  << std::setw(15) << stats.nodes_per_level[i]
                  << std::setw(20) << std::fixed << std::setprecision(3) 
                  << (stats.avg_time_per_level_us[i] / 1000.0)
                  << std::setw(15) << std::fixed << std::setprecision(1) 
                  << percent << "%" << std::endl;
    }
    std::cout << std::endl;
    
    // Get all nodes and analyze
    std::cout << "--- Node Analysis ---" << std::endl;
    auto nodes = tree.get_nodes_depth_first();
    
    // Count by category
    std::map<std::string, size_t> category_counts;
    for (const auto& node : nodes) {
        category_counts[node.category]++;
    }
    
    std::cout << "Nodes by category:" << std::endl;
    for (const auto& [category, count] : category_counts) {
        std::cout << "  " << std::left << std::setw(20) << category 
                  << ": " << count << " nodes" << std::endl;
    }
    std::cout << std::endl;
    
    // Save outputs
    std::cout << "--- Saving Outputs ---" << std::endl;
    
    // Set custom output path
    tree.set_output_path("nodes-4_calltree.bin");
    
    if (tree.save_to_file()) {
        std::cout << "Binary format saved: nodes-4_calltree.bin" << std::endl;
    }
    
    // Save to JSON (Chrome Tracing format)
    if (tree.save_to_json("nodes-4_calltree.pfw")) {
        std::cout << "JSON format saved: nodes-4_calltree.pfw (Chrome Tracing compatible)" << std::endl;
    }
    
    if (tree.print_depth_first_to_file("nodes-4_calltree_full.txt", 0)) {
        std::cout << "Full tree saved: nodes-4_calltree_full.txt" << std::endl;
    }
    
    if (tree.print_depth_first_to_file("nodes-4_calltree_summary.txt", 2)) {
        std::cout << "Summary (2 levels) saved: nodes-4_calltree_summary.txt" << std::endl;
    }
    
    std::cout << "\n=== Example completed successfully ===" << std::endl;
    
    return 0;
}
