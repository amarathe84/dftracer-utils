#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_UNORDERED_MAP_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_UNORDERED_MAP_H

#include <dftracer/utils/pipeline/executors/mpi_helpers/datatype.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/op.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/serializer.h>
#include <mpi.h>

#include <unordered_map>

template <typename K, typename V>
struct Serializer<std::unordered_map<K, V>> {
    static void send(const std::unordered_map<K, V>& map, int dest, int tag,
                     MPI_Comm comm) {
        mpi_send(static_cast<std::size_t>(map.size()), dest, tag, comm);
        for (const auto& pair : map) {
            mpi_send(pair.first, dest, tag, comm);
            mpi_send(pair.second, dest, tag, comm);
        }
    }

    static void recv(std::unordered_map<K, V>& map, int src, int tag,
                     MPI_Comm comm) {
        std::size_t size;
        mpi_recv(size, src, tag, comm);
        map.clear();
        map.reserve(size);
        for (std::size_t i = 0; i < size; ++i) {
            K key;
            V value;
            mpi_recv(key, src, tag, comm);
            mpi_recv(value, src, tag, comm);
            map.emplace(std::move(key), std::move(value));
        }
    }
};

#endif  // DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_UNORDERED_MAP_H
