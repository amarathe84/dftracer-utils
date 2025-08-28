#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_VECTOR_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_VECTOR_H

#include <dftracer/utils/pipeline/executors/mpi_helpers/datatype.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/op.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/serializer.h>
#include <mpi.h>

#include <vector>

template <typename T>
struct Serializer<std::vector<T>> {
    static void send(const std::vector<T>& vec, int dest, int tag,
                     MPI_Comm comm) {
        mpi_send(static_cast<std::size_t>(vec.size()), dest, tag, comm);
        if constexpr (std::is_arithmetic_v<T>) {
            if (!vec.empty()) {
                MPI_Send(vec.data(), vec.size(), mpi_datatype<T>(), dest, tag,
                         comm);
            }
        } else {
            for (const auto& item : vec) {
                mpi_send(item, dest, tag, comm);
            }
        }
    }

    static void recv(std::vector<T>& vec, int src, int tag, MPI_Comm comm) {
        std::size_t size;
        mpi_recv(size, src, tag, comm);
        vec.resize(size);
        if constexpr (std::is_arithmetic_v<T>) {
            if (size > 0) {
                MPI_Recv(vec.data(), size, mpi_datatype<T>(), src, tag, comm,
                         MPI_STATUS_IGNORE);
            }
        } else {
            for (auto& item : vec) {
                mpi_recv(item, src, tag, comm);
            }
        }
    }
};

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_VECTOR_H
