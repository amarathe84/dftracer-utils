#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_OP_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_OP_H

#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/serializer.h>
#include <mpi.h>

template <typename T>
void mpi_send(const T& value, int dest, int tag, MPI_Comm comm) {
    Serializer<T>::send(value, dest, tag, comm);
}

template <typename T>
void mpi_recv(T& value, int src, int tag, MPI_Comm comm) {
    Serializer<T>::recv(value, src, tag, comm);
}

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_OP_H
