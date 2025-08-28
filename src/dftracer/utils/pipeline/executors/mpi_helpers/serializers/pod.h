#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_POD_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_POD_H

#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/serializer.h>

template <typename T>
struct Serializer<T, typename std::enable_if<std::is_trivially_copyable_v<T> &&
                                                 !std::is_arithmetic_v<T>,
                                             void>::type> {
    static void send(const T& value, int dest, int tag, MPI_Comm comm) {
        MPI_Send(&value, sizeof(T), MPI_BYTE, dest, tag, comm);
    }

    static void recv(T& value, int src, int tag, MPI_Comm comm) {
        MPI_Recv(&value, sizeof(T), MPI_BYTE, src, tag, comm,
                 MPI_STATUS_IGNORE);
    }
};

#endif
