#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_STRING_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_STRING_H

#include <dftracer/utils/pipeline/executors/mpi_helpers/op.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/serializer.h>

template <>
struct Serializer<std::string> {
  static void send(const std::string& s, int dest, int tag, MPI_Comm comm) {
    mpi_send(static_cast<std::size_t>(s.size()), dest, tag, comm);
    if (!s.empty()) {
      MPI_Send(s.data(), s.size(), MPI_CHAR, dest, tag, comm);
    }
  }

  static void recv(std::string& s, int src, int tag, MPI_Comm comm) {
    std::size_t size;
    mpi_recv(size, src, tag, comm);
    s.resize(size);
    if (size > 0) {
      MPI_Recv(&s[0], size, MPI_CHAR, src, tag, comm, MPI_STATUS_IGNORE);
    }
  }
};

#endif // DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_STRING_H
