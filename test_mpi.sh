#!/bin/bash
set -e
echo "Testing MPI call graph with $(which mpirun)"
mpirun --version
echo "Running with 4 MPI ranks..."
mpirun -np 4 /g/g92/marathe1/myworkspace/dldl/dftracer-utils/build/bin/dftracer_call_graph_mpi /g/g92/marathe1/myworkspace/dldl/dftracer-utils/trace/bert_v100-1.pfw
