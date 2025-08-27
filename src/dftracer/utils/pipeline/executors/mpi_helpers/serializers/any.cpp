#include <dftracer/utils/pipeline/executors/mpi_helpers/serializers/any.h>

#include <string>
#include <vector>
#include <unordered_map>
#include <cstdint>
#include <stdexcept>

std::unordered_map<std::type_index, AnySendFunc> any_senders;
std::unordered_map<std::type_index, AnyRecvFunc> any_receivers;
std::unordered_map<size_t, AnyRecvFunc> any_receivers_by_hash;

void register_common_any_serializers() {
  register_any_serializer<int>();
  register_any_serializer<std::size_t>();
  register_any_serializer<std::uint64_t>();
  register_any_serializer<std::int64_t>();
  register_any_serializer<long>();
  register_any_serializer<float>();
  register_any_serializer<double>();
  register_any_serializer<std::string>();
  register_any_serializer<std::vector<int>>();
  register_any_serializer<std::vector<std::size_t>>();
  register_any_serializer<std::vector<std::uint64_t>>();
  register_any_serializer<std::vector<std::int64_t>>();
  register_any_serializer<std::vector<double>>();
  register_any_serializer<std::vector<std::string>>();
  register_any_serializer<std::unordered_map<std::string, int>>();
  register_any_serializer<std::unordered_map<std::string, double>>();
}

void mpi_send_any(const std::any& a, int dest, int tag, MPI_Comm comm) {
  std::type_index ti(a.type());
  auto it = any_senders.find(ti);
  if (it == any_senders.end()) {
    throw std::runtime_error("Unsupported type in std::any for MPI send: " +
                             std::string(ti.name()));
  }

  // Send type hash
  size_t hash = ti.hash_code();
  MPI_Send(&hash, 1, MPI_UNSIGNED_LONG, dest, tag, comm);

  // Send data
  it->second(a, dest, tag, comm);
}

void mpi_recv_any(std::any& out, int src, int tag, MPI_Comm comm) {
  size_t hash;
  MPI_Recv(&hash, 1, MPI_UNSIGNED_LONG, src, tag, comm, MPI_STATUS_IGNORE);

  auto it = any_receivers_by_hash.find(hash);
  if (it == any_receivers_by_hash.end()) {
    throw std::runtime_error("Unsupported type in std::any for MPI recv hash: " +
                             std::to_string(hash));
  }

  // Receive data
  it->second(out, src, tag, comm);
}
