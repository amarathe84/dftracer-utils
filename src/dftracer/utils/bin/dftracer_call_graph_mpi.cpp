/**
 * dftracer_call_graph_mpi - MPI-parallel call graph generation from trace files
 * 
 * This tool implements the following workflow:
 * 1. Bootstraps by scanning trace files to discover Process IDs using the indexer
 * 2. Each MPI rank reads its assigned PIDs into memory using the pipeline framework
 * 3. Generates call graphs based on the in-memory data structures
 * 4. Performs MPI all-to-all to ensure all ranks have the same call graph data
 * 5. Saves the call graph to a file and provides API for later reading
 * 
 * Usage:
 *   mpirun -np <N> dftracer_call_graph_mpi <trace_dir_or_files> [options]
 * 
 * Options:
 *   --pattern <pattern>  : File pattern for directory (default: *.pfw.gz)
 *   --output <file>      : Output file for call graph (default: call_graph.bin)
 *   --summary            : Show summary only
 *   --detailed           : Show detailed call graphs
 *   --verbose            : Enable verbose logging
 *   --threads <N>        : Number of threads for pipeline (default: auto)
 *   --no-gather          : Skip the all-to-all gather step
 *   --load <file>        : Load and display existing call graph file
 */

#include <dftracer/utils/call_graph/call_graph_mpi.h>
#include <dftracer/utils/call_graph/call_graph.h>
#include <mpi.h>

#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;
using namespace dftracer::utils::call_graph;

void print_usage(const char* program_name) {
    std::cerr << "Usage: mpirun -np <N> " << program_name << " <trace_dir_or_files> [options]" << std::endl;
    std::cerr << std::endl;
    std::cerr << "Arguments:" << std::endl;
    std::cerr << "  trace_dir_or_files     : Trace file(s) or directory containing trace files" << std::endl;
    std::cerr << std::endl;
    std::cerr << "Options:" << std::endl;
    std::cerr << "  --pattern <pattern>    : File pattern when reading directory (default: *.pfw.gz)" << std::endl;
    std::cerr << "  --output <file>        : Output file for call graph (default: call_graph.bin)" << std::endl;
    std::cerr << "  --summary              : Show summary statistics only" << std::endl;
    std::cerr << "  --detailed             : Show detailed call graphs (default)" << std::endl;
    std::cerr << "  --verbose              : Enable verbose logging" << std::endl;
    std::cerr << "  --threads <N>          : Number of threads for pipeline (0 = auto)" << std::endl;
    std::cerr << "  --no-gather            : Skip the all-to-all gather step" << std::endl;
    std::cerr << "  --load <file>          : Load and display existing call graph file" << std::endl;
    std::cerr << "  --help                 : Show this help message" << std::endl;
    std::cerr << std::endl;
    std::cerr << "MPI-enabled call graph generator with the following phases:" << std::endl;
    std::cerr << "  1. Bootstrap: Scans trace files and discovers Process IDs using indexer" << std::endl;
    std::cerr << "  2. Build: Each rank generates call graphs for assigned PIDs" << std::endl;
    std::cerr << "  3. Gather: All-to-all communication so all ranks have complete data" << std::endl;
    std::cerr << "  4. Save: Write call graph to file for later use" << std::endl;
    std::cerr << std::endl;
    std::cerr << "Example:" << std::endl;
    std::cerr << "  mpirun -np 4 " << program_name << " trace_short/cosmoflow_a100/nodes-4 --verbose" << std::endl;
}

void print_call_graph_detailed(const CallGraph& call_graph, int rank, int world_size) {
    for (int r = 0; r < world_size; r++) {
        if (r == rank) {
            auto process_keys = call_graph.keys();
            for (const auto& key : process_keys) {
                std::cout << "\n[Rank " << rank << "] ";
                std::cout << "========================================" << std::endl;
                const_cast<CallGraph&>(call_graph).print(key);
                std::cout << "========================================" << std::endl;
            }
            std::flush(std::cout);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
}

int load_and_display(const std::string& filename, bool summary_only, bool detailed) {
    std::cout << "Loading call graph from: " << filename << std::endl;
    
    auto call_graph = MPICallGraphBuilder::load(filename);
    if (!call_graph) {
        std::cerr << "Failed to load call graph from " << filename << std::endl;
        return 1;
    }
    
    std::cout << "Loaded call graph with " << call_graph->size() << " process graphs" << std::endl;
    
    if (!summary_only && detailed) {
        auto process_keys = call_graph->keys();
        for (const auto& key : process_keys) {
            std::cout << "========================================" << std::endl;
            call_graph->print(key);
            std::cout << "========================================" << std::endl;
        }
    }
    
    return 0;
}

int main(int argc, char* argv[]) {
    // Initialize MPI
    MPI_Init(&argc, &argv);
    
    int rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    
    // Parse arguments
    if (argc < 2) {
        if (rank == 0) {
            print_usage(argv[0]);
        }
        MPI_Finalize();
        return 1;
    }
    
    std::string input_path;
    std::string pattern = "*.pfw.gz";
    std::string output_file = "call_graph.bin";
    std::string load_file;
    bool summary_only = false;
    bool detailed = true;
    bool verbose = false;
    bool do_gather = true;
    std::size_t num_threads = 0;
    
    // Parse command line options
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg == "--help" || arg == "-h") {
            if (rank == 0) {
                print_usage(argv[0]);
            }
            MPI_Finalize();
            return 0;
        } else if (arg == "--pattern" && i + 1 < argc) {
            pattern = argv[++i];
        } else if (arg == "--output" && i + 1 < argc) {
            output_file = argv[++i];
        } else if (arg == "--load" && i + 1 < argc) {
            load_file = argv[++i];
        } else if (arg == "--summary") {
            summary_only = true;
            detailed = false;
        } else if (arg == "--detailed") {
            detailed = true;
            summary_only = false;
        } else if (arg == "--verbose") {
            verbose = true;
        } else if (arg == "--no-gather") {
            do_gather = false;
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoul(argv[++i]);
        } else if (arg[0] != '-') {
            input_path = arg;
        }
    }
    
    // Handle load mode
    if (!load_file.empty()) {
        int result = 0;
        if (rank == 0) {
            result = load_and_display(load_file, summary_only, detailed);
        }
        MPI_Finalize();
        return result;
    }
    
    if (input_path.empty()) {
        if (rank == 0) {
            std::cerr << "Error: No input path specified" << std::endl;
            print_usage(argv[0]);
        }
        MPI_Finalize();
        return 1;
    }
    
    // Create configuration
    MPICallGraphConfig config;
    config.file_pattern = pattern;
    config.output_file = output_file;
    config.verbose = verbose;
    config.summary_only = summary_only;
    config.num_threads = num_threads;
    
    // Create builder
    MPICallGraphBuilder builder(config);
    
    try {
        // Initialize
        builder.initialize();
        
        if (rank == 0) {
            std::cout << "============================================================" << std::endl;
            std::cout << "MPI Call Graph Generator" << std::endl;
            std::cout << "============================================================" << std::endl;
            std::cout << "MPI Ranks: " << world_size << std::endl;
            std::cout << "Input: " << input_path << std::endl;
            std::cout << "Pattern: " << pattern << std::endl;
            std::cout << "Output: " << output_file << std::endl;
            std::cout << "============================================================\n" << std::endl;
        }
        
        // Collect trace files
        if (rank == 0) {
            if (fs::is_directory(input_path)) {
                builder.add_trace_directory(input_path, pattern);
            } else if (fs::is_regular_file(input_path)) {
                builder.add_trace_files({input_path});
            } else {
                // Try to expand as glob pattern
                std::vector<std::string> files;
                for (int i = 1; i < argc && argv[i][0] != '-'; i++) {
                    std::string path = argv[i];
                    if (fs::exists(path) && fs::is_regular_file(path)) {
                        files.push_back(path);
                    }
                }
                if (!files.empty()) {
                    builder.add_trace_files(files);
                } else {
                    std::cerr << "No valid input files found" << std::endl;
                    MPI_Finalize();
                    return 1;
                }
            }
        }
        
        // Phase 1: Discover PIDs
        if (rank == 0) {
            std::cout << "Phase 1: Discovering Process IDs..." << std::endl;
        }
        
        auto pid_map = builder.discover_pids();
        
        MPI_Barrier(MPI_COMM_WORLD);
        
        if (rank == 0 && verbose) {
            std::cout << "Discovered " << pid_map.size() << " unique PIDs" << std::endl;
        }
        
        // Phase 2: Build call graphs
        if (rank == 0) {
            std::cout << "\nPhase 2: Building call graphs..." << std::endl;
        }
        
        auto result = builder.build();
        
        if (!result.success) {
            if (rank == 0) {
                std::cerr << "Build failed: " << result.error_message << std::endl;
            }
            MPI_Finalize();
            return 1;
        }
        
        MPI_Barrier(MPI_COMM_WORLD);
        
        // Phase 3: All-to-all gather
        if (do_gather) {
            if (rank == 0) {
                std::cout << "\nPhase 3: All-to-all gather..." << std::endl;
            }
            
            if (!builder.gather()) {
                if (rank == 0) {
                    std::cerr << "Gather failed" << std::endl;
                }
                MPI_Finalize();
                return 1;
            }
            
            MPI_Barrier(MPI_COMM_WORLD);
        }
        
        // Phase 4: Save to file
        if (rank == 0) {
            std::cout << "\nPhase 4: Saving call graph to " << output_file << "..." << std::endl;
        }
        
        if (!builder.save(output_file)) {
            if (rank == 0) {
                std::cerr << "Failed to save call graph" << std::endl;
            }
        }
        
        MPI_Barrier(MPI_COMM_WORLD);
        
        // Print summary
        builder.print_summary();
        
        // Print detailed output if requested
        if (detailed && !summary_only) {
            const auto& call_graph = builder.get_call_graph();
            auto assigned_pids = builder.get_assigned_pids();
            
            if (!assigned_pids.empty()) {
                print_call_graph_detailed(call_graph, rank, world_size);
            }
        }
        
        // Cleanup
        builder.cleanup();
        
        if (rank == 0) {
            std::cout << "\n============================================================" << std::endl;
            std::cout << "Call graph generation completed successfully!" << std::endl;
            std::cout << "Output saved to: " << output_file << std::endl;
            std::cout << "============================================================" << std::endl;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "[Rank " << rank << "] Error: " << e.what() << std::endl;
        MPI_Finalize();
        return 1;
    }
    
    MPI_Finalize();
    return 0;
}
