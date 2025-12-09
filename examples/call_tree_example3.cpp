/**
 * Example 3: Using CallTree API programmatically
 * Shows how to use node information for custom analysis
 */

#include <dftracer/utils/call_tree/call_tree.h>
#include <iostream>
#include <algorithm>
#include <numeric>
#include <map>

using namespace dftracer::utils::call_tree;

void analyze_call_patterns(const std::vector<CallTreeNodeInfo>& nodes) {
    std::cout << "\n--- Call Pattern Analysis ---" << std::endl;
    
    // Find most frequently called functions
    std::map<std::string, size_t> call_counts;
    for (const auto& node : nodes) {
        call_counts[node.name]++;
    }
    
    // Sort by frequency
    std::vector<std::pair<std::string, size_t>> sorted_calls(call_counts.begin(), call_counts.end());
    std::sort(sorted_calls.begin(), sorted_calls.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    
    std::cout << "Top 10 most frequently called functions:" << std::endl;
    for (size_t i = 0; i < std::min(sorted_calls.size(), size_t(10)); i++) {
        std::cout << "  " << (i+1) << ". " << sorted_calls[i].first 
                  << " (" << sorted_calls[i].second << " calls)" << std::endl;
    }
}

void analyze_timing(const std::vector<CallTreeNodeInfo>& nodes) {
    std::cout << "\n--- Timing Analysis ---" << std::endl;
    
    if (nodes.empty()) {
        std::cout << "No nodes to analyze" << std::endl;
        return;
    }
    
    // Calculate timing statistics
    std::vector<std::uint64_t> durations;
    durations.reserve(nodes.size());
    
    for (const auto& node : nodes) {
        durations.push_back(node.duration_us);
    }
    
    std::sort(durations.begin(), durations.end());
    
    std::uint64_t total = std::accumulate(durations.begin(), durations.end(), 0ULL);
    double avg = static_cast<double>(total) / durations.size();
    
    std::uint64_t min_time = durations.front();
    std::uint64_t max_time = durations.back();
    std::uint64_t median = durations[durations.size() / 2];
    std::uint64_t p95 = durations[static_cast<size_t>(durations.size() * 0.95)];
    std::uint64_t p99 = durations[static_cast<size_t>(durations.size() * 0.99)];
    
    std::cout << "Duration statistics (milliseconds):" << std::endl;
    std::cout << "  Min:    " << (min_time / 1000.0) << " ms" << std::endl;
    std::cout << "  Max:    " << (max_time / 1000.0) << " ms" << std::endl;
    std::cout << "  Mean:   " << (avg / 1000.0) << " ms" << std::endl;
    std::cout << "  Median: " << (median / 1000.0) << " ms" << std::endl;
    std::cout << "  95th:   " << (p95 / 1000.0) << " ms" << std::endl;
    std::cout << "  99th:   " << (p99 / 1000.0) << " ms" << std::endl;
}

void find_critical_path(const std::vector<CallTreeNodeInfo>& nodes) {
    std::cout << "\n--- Critical Path (Longest Duration Chain) ---" << std::endl;
    
    // Find top 10 longest running calls
    std::vector<CallTreeNodeInfo> sorted_nodes = nodes;
    std::sort(sorted_nodes.begin(), sorted_nodes.end(),
              [](const auto& a, const auto& b) { return a.duration_us > b.duration_us; });
    
    std::cout << "Top 10 longest running calls:" << std::endl;
    for (size_t i = 0; i < std::min(sorted_nodes.size(), size_t(10)); i++) {
        const auto& node = sorted_nodes[i];
        std::cout << "  " << (i+1) << ". " << node.name 
                  << " [" << node.category << "]"
                  << " - " << (node.duration_us / 1000.0) << " ms"
                  << " (level " << node.level << ")" << std::endl;
    }
}

int main(int argc, char* argv[]) {
    std::cout << "=== CallTree API Example 3: Custom Analysis ===" << std::endl;
    
    std::string trace_path = "/g/g92/marathe1/myworkspace/dldl/dftracer-utils/trace_short/cosmoflow_a100/nodes-1";
    if (argc > 1) {
        trace_path = argv[1];
    }
    
    CallTree tree;
    
    std::cout << "\nLoading and generating call tree..." << std::endl;
    if (!tree.load_from_directory(trace_path)) {
        std::cerr << "Failed to load traces" << std::endl;
        return 1;
    }
    
    if (!tree.generate()) {
        std::cerr << "Failed to generate call tree" << std::endl;
        return 1;
    }
    
    // Get all nodes for analysis
    auto nodes = tree.get_nodes_depth_first();
    std::cout << "Analyzing " << nodes.size() << " nodes..." << std::endl;
    
    // Perform custom analyses
    analyze_call_patterns(nodes);
    analyze_timing(nodes);
    find_critical_path(nodes);
    
    // Also print the built-in statistics
    tree.print_statistics();
    
    // Save analysis results in JSON format for downstream processing
    std::cout << "\nSaving analysis results..." << std::endl;
    if (tree.save_to_json("analysis_output.pfw")) {
        std::cout << "✓ JSON output saved to: analysis_output.pfw" << std::endl;
        std::cout << "  This file can be imported into Chrome Tracing, Perfetto," << std::endl;
        std::cout << "  or analyzed with DFAnalyzer tools." << std::endl;
    }
    
    std::cout << "\n=== Analysis complete ===" << std::endl;
    
    return 0;
}
