#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_SERIALIZER_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_SERIALIZER_H

template <typename T, typename Enable = void>
struct Serializer;

#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/arithmetic.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/pod.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/string.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/vector.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/unordered_map.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/any.h>

#endif // DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_SERIALIZER_H
