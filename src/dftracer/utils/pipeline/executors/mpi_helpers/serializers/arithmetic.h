#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_ARITHMETIC_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_ARITHMETIC_H

#include <mpi.h>
#include <type_traits>

#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/serializer.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/datatype.h>

template <typename T>
struct Serializer<T, typename std::enable_if<std::is_arithmetic<T>::value>::type> {
  static void send(const T& value, int dest, int tag, MPI_Comm comm) {
    MPI_Send(&value, 1, mpi_datatype<T>(), dest, tag, comm);
  }

  static void recv(T& value, int src, int tag, MPI_Comm comm) {
    MPI_Recv(&value, 1, mpi_datatype<T>(), src, tag, comm, MPI_STATUS_IGNORE);
  }
};

#endif // DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_ARITHMETIC_H
