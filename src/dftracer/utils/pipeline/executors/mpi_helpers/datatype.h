#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_DATATYPE_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_DATATYPE_H

#include <mpi.h>

#include <cstdint>
#include <type_traits>

template <typename T>
MPI_Datatype mpi_datatype();

template <>
inline MPI_Datatype mpi_datatype<char>() {
  return MPI_CHAR;
}
template <>
inline MPI_Datatype mpi_datatype<int>() {
  return MPI_INT;
}
template <>
inline MPI_Datatype mpi_datatype<float>() {
  return MPI_FLOAT;
}
template <>
inline MPI_Datatype mpi_datatype<double>() {
  return MPI_DOUBLE;
}
template <>
inline MPI_Datatype mpi_datatype<std::int64_t>() {
  return MPI_LONG_LONG;
}
template <>
inline MPI_Datatype mpi_datatype<std::uint64_t>() {
  return MPI_UNSIGNED_LONG_LONG;
}

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_DATATYPE_H
