#ifndef DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_ANY_H
#define DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_ANY_H

#include <mpi.h>

#include <any>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <vector>

#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/serializer.h>
#include <dftracer/utils/pipeline/executors/mpi_helpers/op.h>

using AnySendFunc = std::function<void(const std::any&, int, int, MPI_Comm)>;
using AnyRecvFunc = std::function<void(std::any&, int, int, MPI_Comm)>;

extern std::unordered_map<std::type_index, AnySendFunc> any_senders;
extern std::unordered_map<std::type_index, AnyRecvFunc> any_receivers;
extern std::unordered_map<size_t, AnyRecvFunc> any_receivers_by_hash;

template <typename T>
void register_any_serializer() {
  any_senders[typeid(T)] = [](const std::any& a, int dest, int tag,
                               MPI_Comm comm) {
    mpi_send(std::any_cast<T>(a), dest, tag, comm);
  };
  any_receivers[typeid(T)] = [](std::any& a, int src, int tag, MPI_Comm comm) {
    T value;
    mpi_recv(value, src, tag, comm);
    a = value;
  };
  any_receivers_by_hash[typeid(T).hash_code()] = any_receivers[typeid(T)];
}

void register_common_any_serializers();


void mpi_send_any(const std::any& a, int dest, int tag, MPI_Comm comm);
void mpi_recv_any(std::any& out, int src, int tag, MPI_Comm comm);

#endif // DFTRACER_UTILS_PIPELINE_EXECUTORS_MPI_HELPERS_SERIALIZERS_ANY_H
