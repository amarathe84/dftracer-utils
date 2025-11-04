#include <dftracer/utils/call_graph/call_graph.h>
#include <mpi.h>
#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <set>
#include <map>
#include <algorithm>
#include <fstream>
#include <yyjson.h>

using namespace dftracer::utils::call_graph;

/**
 * Lightweight process key discovery - only extract PIDs from trace without full parsing
 */
std::set<std::uint32_t> discover_process_ids(const std::vector<std::string>& trace_files) {
    std::set<std::uint32_t> pids;
    
    for (const auto& trace_file : trace_files) {
        std::ifstream file(trace_file);
        if (!file.is_open()) {
            std::cerr << "Warning: Cannot open " << trace_file << " for PID discovery" << std::endl;
            continue;
        }
        
        int line_count = 0;
        std::string line;
        while (std::getline(file, line)) {
            line_count++;
            // Skip empty lines and brackets
            if (line.empty() || line == "[" || line == "]") {
                continue;
            }
            
            // Remove trailing comma
            if (!line.empty() && line.back() == ',') {
                line.pop_back();
            }
            
            // Quick JSON parse to extract PID
            yyjson_doc* doc = yyjson_read(line.c_str(), line.length(), 0);
            if (doc) {
                yyjson_val* root = yyjson_doc_get_root(doc);
                if (root) {
                    yyjson_val* pid_val = yyjson_obj_get(root, "pid");
                    if (pid_val) {
                        std::uint32_t pid = static_cast<std::uint32_t>(yyjson_get_uint(pid_val));
                        pids.insert(pid);
                    }
                }
                yyjson_doc_free(doc);
            }
        }
        std::cerr << "[DEBUG] File " << trace_file << ": read " << line_count 
                  << " lines, found " << pids.size() << " PIDs" << std::endl;
    }
    
    return pids;
}

/**
 * Filter-aware TraceReader that only processes events for specific PIDs
 */
class FilteredTraceReader {
public:
    FilteredTraceReader(const std::set<std::uint32_t>& allowed_pids) 
        : allowed_pids_(allowed_pids) {}
    
    bool read(const std::string& trace_file, CallGraph& graph) {
        std::ifstream file(trace_file);
        if (!file.is_open()) {
            std::cerr << "Cannot open trace file: " << trace_file << std::endl;
            return false;
        }
        
        std::string line;
        size_t line_count = 0;
        size_t processed = 0;
        size_t filtered = 0;
        
        while (std::getline(file, line)) {
            line_count++;
            
            // Skip brackets and empty lines
            if (line.empty() || line == "[" || line == "]") {
                continue;
            }
            
            // Remove trailing comma
            if (!line.empty() && line.back() == ',') {
                line.pop_back();
            }
            
            yyjson_doc* doc = yyjson_read(line.c_str(), line.length(), 0);
            if (!doc) {
                continue;
            }
            
            yyjson_val* root = yyjson_doc_get_root(doc);
            if (!root) {
                yyjson_doc_free(doc);
                continue;
            }
            
            // Check PID filter
            yyjson_val* pid_val = yyjson_obj_get(root, "pid");
            if (pid_val) {
                std::uint32_t pid = static_cast<std::uint32_t>(yyjson_get_uint(pid_val));
                
                // Only process if PID is in our allowed set
                if (allowed_pids_.find(pid) != allowed_pids_.end()) {
                    // Use the standard TraceReader processing
                    TraceReader reader;
                    if (reader.process_trace_line(line, graph)) {
                        processed++;
                    }
                } else {
                    filtered++;
                }
            }
            
            yyjson_doc_free(doc);
        }
        
        return true;
    }
    
    bool read_multiple(const std::vector<std::string>& trace_files, CallGraph& graph) {
        for (const auto& file : trace_files) {
            if (!read(file, graph)) {
                return false;
            }
        }
        return true;
    }
    
private:
    std::set<std::uint32_t> allowed_pids_;
};

void print_usage(const char* program_name) {
    std::cerr << "usage: mpirun -np <N> " << program_name << " <trace_file_or_directory> [options]" << std::endl;
    std::cerr << "  trace_file_or_directory: single file, multiple files, or directory" << std::endl;
    std::cerr << "  options:" << std::endl;
    std::cerr << "    --pattern <pattern>  : file pattern when reading directory (default: *.pfw)" << std::endl;
    std::cerr << "    --summary            : show summary statistics only" << std::endl;
    std::cerr << "    --detailed           : show detailed call graphs (default)" << std::endl;
    std::cerr << std::endl;
    std::cerr << "MPI-enabled call graph generator:" << std::endl;
    std::cerr << "  - Automatically discovers all process IDs in trace files" << std::endl;
    std::cerr << "  - Distributes PIDs across MPI ranks for parallel processing" << std::endl;
    std::cerr << "  - Each rank generates call graphs for its assigned PIDs" << std::endl;
}

void print_summary(int rank, int world_size, const CallGraph& call_graph) {
    auto process_keys = call_graph.keys();
    
    // Collect local statistics
    std::set<std::uint32_t> local_pids;
    std::set<std::uint32_t> local_tids;
    std::set<std::uint32_t> local_nodes;
    size_t local_total_calls = 0;
    
    for (const auto& key : process_keys) {
        local_pids.insert(key.pid);
        local_tids.insert(key.tid);
        local_nodes.insert(key.node_id);
        
        auto* graph = const_cast<CallGraph&>(call_graph).get(key);
        if (graph) {
            local_total_calls += graph->calls.size();
        }
    }
    
    // Gather statistics to rank 0
    int local_pid_count = local_pids.size();
    int local_tid_count = local_tids.size();
    int local_node_count = local_nodes.size();
    size_t local_calls = local_total_calls;
    
    int total_pid_count = 0;
    int total_tid_count = 0;
    int total_node_count = 0;
    size_t total_calls = 0;
    
    MPI_Reduce(&local_pid_count, &total_pid_count, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_tid_count, &total_tid_count, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_node_count, &total_node_count, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_calls, &total_calls, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    
    if (rank == 0) {
        std::cout << "\n============ MPI SUMMARY (Aggregated) ============" << std::endl;
        std::cout << "MPI Ranks: " << world_size << std::endl;
        std::cout << "Total PIDs processed: " << total_pid_count << std::endl;
        std::cout << "Total TIDs processed: " << total_tid_count << std::endl;
        std::cout << "Total Nodes processed: " << total_node_count << std::endl;
        std::cout << "Total function calls: " << total_calls << std::endl;
        std::cout << "================================================\n" << std::endl;
    }
    
    // Each rank prints its local summary
    for (int r = 0; r < world_size; r++) {
        if (r == rank) {
            std::cout << "\n[Rank " << rank << "] Local Summary:" << std::endl;
            std::cout << "  PIDs: " << local_pid_count << " [";
            bool first = true;
            for (auto pid : local_pids) {
                if (!first) std::cout << ", ";
                std::cout << pid;
                first = false;
            }
            std::cout << "]" << std::endl;
            std::cout << "  TIDs: " << local_tid_count << std::endl;
            std::cout << "  Nodes: " << local_node_count << std::endl;
            std::cout << "  Function calls: " << local_calls << std::endl;
            std::flush(std::cout);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
}

int main(int argc, char* argv[]) {
    // Initialize MPI
    MPI_Init(&argc, &argv);
    
    int rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    
    if (argc < 2) {
        if (rank == 0) {
            print_usage(argv[0]);
        }
        MPI_Finalize();
        return 1;
    }
    
    std::string input_path = argv[1];
    std::string pattern = "*.pfw";
    bool summary_only = false;
    bool detailed = true;
    
    // Parse options
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
    
    namespace fs = std::filesystem;
    
    // Collect trace files
    std::vector<std::string> trace_files;
    
    if (rank == 0) {
        if (fs::is_directory(input_path)) {
            for (const auto& entry : fs::directory_iterator(input_path)) {
                if (entry.is_regular_file()) {
                    std::string filename = entry.path().filename().string();
                    if (pattern == "*" || filename.find(pattern.substr(1)) != std::string::npos) {
                        trace_files.push_back(entry.path().string());
                    }
                }
            }
            std::sort(trace_files.begin(), trace_files.end());
        } else if (fs::is_regular_file(input_path)) {
            trace_files.push_back(input_path);
        }
    }
    
    // Broadcast number of trace files
    int num_files = trace_files.size();
    MPI_Bcast(&num_files, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    if (num_files == 0) {
        if (rank == 0) {
            std::cerr << "No trace files found" << std::endl;
        }
        MPI_Finalize();
        return 1;
    }
    
    // Broadcast trace file paths
    if (rank != 0) {
        trace_files.resize(num_files);
    }
    
    for (int i = 0; i < num_files; i++) {
        int len = 0;
        if (rank == 0) {
            len = trace_files[i].size();
        }
        MPI_Bcast(&len, 1, MPI_INT, 0, MPI_COMM_WORLD);
        
        if (rank != 0) {
            trace_files[i].resize(len);
        }
        MPI_Bcast(&trace_files[i][0], len, MPI_CHAR, 0, MPI_COMM_WORLD);
    }
    
    if (rank == 0) {
        std::cout << "Discovering process IDs from " << num_files << " trace file(s)..." << std::endl;
    }
    
    // Phase 1: Discover all PIDs (all ranks do this in parallel)
    if (rank == 0) {
        std::cout << "[DEBUG] Starting PID discovery..." << std::endl;
    }
    std::set<std::uint32_t> all_pids = discover_process_ids(trace_files);
    if (rank == 0) {
        std::cout << "[DEBUG] Rank 0 discovered " << all_pids.size() << " PIDs" << std::endl;
    }
    
    // Gather all PIDs to rank 0
    int local_pid_count = all_pids.size();
    std::vector<std::uint32_t> local_pids(all_pids.begin(), all_pids.end());
    
    std::vector<int> pid_counts(world_size);
    MPI_Gather(&local_pid_count, 1, MPI_INT, pid_counts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    std::vector<std::uint32_t> global_pids;
    if (rank == 0) {
        std::vector<int> displacements(world_size);
        int total_pids = 0;
        for (int i = 0; i < world_size; i++) {
            displacements[i] = total_pids;
            total_pids += pid_counts[i];
        }
        global_pids.resize(total_pids);
        
        MPI_Gatherv(local_pids.data(), local_pid_count, MPI_UINT32_T,
                    global_pids.data(), pid_counts.data(), displacements.data(),
                    MPI_UINT32_T, 0, MPI_COMM_WORLD);
        
        // Remove duplicates
        std::set<std::uint32_t> unique_pids(global_pids.begin(), global_pids.end());
        global_pids.assign(unique_pids.begin(), unique_pids.end());
        std::sort(global_pids.begin(), global_pids.end());
        
        std::cout << "Found " << global_pids.size() << " unique process IDs" << std::endl;
    } else {
        MPI_Gatherv(local_pids.data(), local_pid_count, MPI_UINT32_T,
                    nullptr, nullptr, nullptr, MPI_UINT32_T, 0, MPI_COMM_WORLD);
    }
    
    // Broadcast total PID count
    int total_unique_pids = 0;
    if (rank == 0) {
        total_unique_pids = global_pids.size();
    }
    MPI_Bcast(&total_unique_pids, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    if (rank != 0) {
        global_pids.resize(total_unique_pids);
    }
    
    // Broadcast all PIDs
    MPI_Bcast(global_pids.data(), total_unique_pids, MPI_UINT32_T, 0, MPI_COMM_WORLD);
    
    // Phase 2: Distribute PIDs across ranks
    std::set<std::uint32_t> my_pids;
    for (size_t i = rank; i < global_pids.size(); i += world_size) {
        my_pids.insert(global_pids[i]);
    }
    
    if (rank == 0) {
        std::cout << "Distributing " << total_unique_pids << " PIDs across " 
                  << world_size << " MPI ranks..." << std::endl;
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    if (my_pids.empty()) {
        if (rank == 0) {
            std::cout << "Rank " << rank << " has no PIDs assigned (more ranks than PIDs)" << std::endl;
        }
    } else {
        std::cout << "[Rank " << rank << "] Processing " << my_pids.size() << " PID(s): ";
        bool first = true;
        for (auto pid : my_pids) {
            if (!first) std::cout << ", ";
            std::cout << pid;
            first = false;
        }
        std::cout << std::endl;
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Phase 3: Each rank processes its assigned PIDs
    CallGraph call_graph;
    call_graph.initialize();
    
    if (!my_pids.empty()) {
        FilteredTraceReader reader(my_pids);
        
        if (rank == 0) {
            std::cout << "\nStarting parallel call graph generation..." << std::endl;
        }
        
        MPI_Barrier(MPI_COMM_WORLD);
        
        double start_time = MPI_Wtime();
        
        reader.read_multiple(trace_files, call_graph);
        
        double end_time = MPI_Wtime();
        double local_time = end_time - start_time;
        
        // Build hierarchy
        call_graph.build_hierarchy();
        
        double max_time = 0;
        MPI_Reduce(&local_time, &max_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        
        if (rank == 0) {
            std::cout << "Call graph generation completed in " << max_time << " seconds" << std::endl;
        }
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Print summary
    print_summary(rank, world_size, call_graph);
    
    // Print detailed output if requested
    if (detailed && !summary_only && !my_pids.empty()) {
        for (int r = 0; r < world_size; r++) {
            if (r == rank) {
                auto process_keys = call_graph.keys();
                for (const auto& key : process_keys) {
                    std::cout << "\n[Rank " << rank << "] ";
                    std::cout << "========================================" << std::endl;
                    call_graph.print(key);
                    std::cout << "========================================" << std::endl;
                }
                std::flush(std::cout);
            }
            MPI_Barrier(MPI_COMM_WORLD);
        }
    }
    
    // Cleanup
    call_graph.cleanup();
    
    MPI_Finalize();
    return 0;
}
