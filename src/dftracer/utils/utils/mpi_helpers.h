#ifndef DFTRACER_UTILS_UTILS_MPI_HELPERS_H
#define DFTRACER_UTILS_UTILS_MPI_HELPERS_H

#include <any>
#include <string>
#include <vector>
#include <unordered_map>
#include <typeindex>
#include <type_traits>
#include <stdexcept>
#include <mpi.h>

template <typename T> MPI_Datatype mpi_datatype();
template <> inline MPI_Datatype mpi_datatype<int>()      { return MPI_INT; }
template <> inline MPI_Datatype mpi_datatype<int64_t>()  { return MPI_LONG_LONG; }
template <> inline MPI_Datatype mpi_datatype<uint64_t>() { return MPI_UNSIGNED_LONG_LONG; }
template <> inline MPI_Datatype mpi_datatype<float>()    { return MPI_FLOAT; }
template <> inline MPI_Datatype mpi_datatype<double>()   { return MPI_DOUBLE; }
template <> inline MPI_Datatype mpi_datatype<char>()     { return MPI_CHAR; }

template <typename T>
inline void mpi_send_vector(const std::vector<T>& v, int dest, int tag, MPI_Comm comm) {
    int n = static_cast<int>(v.size());
    MPI_Send(&n, 1, MPI_INT, dest, tag, comm);
    if (n) MPI_Send(v.data(), n, mpi_datatype<T>(), dest, tag, comm);
}
template <typename T>
inline void mpi_recv_vector(std::vector<T>& v, int src, int tag, MPI_Comm comm) {
    int n; MPI_Recv(&n, 1, MPI_INT, src, tag, comm, MPI_STATUS_IGNORE);
    v.resize(n);
    if (n) MPI_Recv(v.data(), n, mpi_datatype<T>(), src, tag, comm, MPI_STATUS_IGNORE);
}
inline void mpi_send_string(const std::string &s, int dest, int tag, MPI_Comm comm) {
    int n = static_cast<int>(s.size());
    MPI_Send(&n, 1, MPI_INT, dest, tag, comm);
    if (n) MPI_Send(s.data(), n, MPI_CHAR, dest, tag, comm);
}
inline void mpi_recv_string(std::string &s, int src, int tag, MPI_Comm comm) {
    int n; MPI_Recv(&n, 1, MPI_INT, src, tag, comm, MPI_STATUS_IGNORE);
    s.resize(n);
    if (n) MPI_Recv(s.data(), n, MPI_CHAR, src, tag, comm, MPI_STATUS_IGNORE);
}

// ===== Any support =====
// Whitelist the types you want to allow inside std::any.
// You can add/remove entries freely without pulling a full serializer.
enum class AnyTag : int {
    INT = 1,
    INT64,
    UINT64,
    FLOAT,
    DOUBLE,
    STRING,
    VEC_INT,
    VEC_INT64,
    VEC_UINT64,
    VEC_FLOAT,
    VEC_DOUBLE,
    VEC_STRING
};

// Map an any to a tag (throws if unsupported).
inline AnyTag any_tag_of(const std::any& a) {
    const std::type_index t{a.type()};
    if (t == typeid(int))                return AnyTag::INT;
    if (t == typeid(int64_t))            return AnyTag::INT64;
    if (t == typeid(uint64_t))           return AnyTag::UINT64;
    if (t == typeid(float))              return AnyTag::FLOAT;
    if (t == typeid(double))             return AnyTag::DOUBLE;
    if (t == typeid(std::string))        return AnyTag::STRING;
    if (t == typeid(std::vector<int>))        return AnyTag::VEC_INT;
    if (t == typeid(std::vector<int64_t>))    return AnyTag::VEC_INT64;
    if (t == typeid(std::vector<uint64_t>))   return AnyTag::VEC_UINT64;
    if (t == typeid(std::vector<float>))      return AnyTag::VEC_FLOAT;
    if (t == typeid(std::vector<double>))     return AnyTag::VEC_DOUBLE;
    if (t == typeid(std::vector<std::string>))return AnyTag::VEC_STRING;
    throw std::runtime_error("std::any holds unsupported type");
}

// Send one std::any (tag + payload).
inline void mpi_send_any(const std::any& a, int dest, int tag, MPI_Comm comm) {
    AnyTag at = any_tag_of(a);
    int tag_int = static_cast<int>(at);
    MPI_Send(&tag_int, 1, MPI_INT, dest, tag, comm);

    switch (at) {
        case AnyTag::INT:    { int v = std::any_cast<int>(a);
                               MPI_Send(&v, 1, MPI_INT, dest, tag, comm); break; }
        case AnyTag::INT64:  { auto v = std::any_cast<int64_t>(a);
                               MPI_Send(&v, 1, MPI_LONG_LONG, dest, tag, comm); break; }
        case AnyTag::UINT64: { auto v = std::any_cast<uint64_t>(a);
                               MPI_Send(&v, 1, MPI_UNSIGNED_LONG_LONG, dest, tag, comm); break; }
        case AnyTag::FLOAT:  { auto v = std::any_cast<float>(a);
                               MPI_Send(&v, 1, MPI_FLOAT, dest, tag, comm); break; }
        case AnyTag::DOUBLE: { auto v = std::any_cast<double>(a);
                               MPI_Send(&v, 1, MPI_DOUBLE, dest, tag, comm); break; }
        case AnyTag::STRING: { const auto& s = std::any_cast<const std::string&>(a);
                               mpi_send_string(s, dest, tag, comm); break; }
        case AnyTag::VEC_INT:    { const auto& v = std::any_cast<const std::vector<int>&>(a);
                                   mpi_send_vector(v, dest, tag, comm); break; }
        case AnyTag::VEC_INT64:  { const auto& v = std::any_cast<const std::vector<int64_t>&>(a);
                                   mpi_send_vector(v, dest, tag, comm); break; }
        case AnyTag::VEC_UINT64: { const auto& v = std::any_cast<const std::vector<uint64_t>&>(a);
                                   mpi_send_vector(v, dest, tag, comm); break; }
        case AnyTag::VEC_FLOAT:  { const auto& v = std::any_cast<const std::vector<float>&>(a);
                                   mpi_send_vector(v, dest, tag, comm); break; }
        case AnyTag::VEC_DOUBLE: { const auto& v = std::any_cast<const std::vector<double>&>(a);
                                   mpi_send_vector(v, dest, tag, comm); break; }
        case AnyTag::VEC_STRING: { const auto& vs = std::any_cast<const std::vector<std::string>&>(a);
                                   int n = static_cast<int>(vs.size());
                                   MPI_Send(&n, 1, MPI_INT, dest, tag, comm);
                                   for (const auto& s : vs) mpi_send_string(s, dest, tag, comm);
                                   break; }
    }
}

// Receive one std::any (tag + payload).
inline void mpi_recv_any(std::any& out, int src, int tag, MPI_Comm comm) {
    int tag_int;
    MPI_Recv(&tag_int, 1, MPI_INT, src, tag, comm, MPI_STATUS_IGNORE);
    AnyTag at = static_cast<AnyTag>(tag_int);

    switch (at) {
        case AnyTag::INT:    { int v; MPI_Recv(&v, 1, MPI_INT, src, tag, comm, MPI_STATUS_IGNORE);
                               out = v; break; }
        case AnyTag::INT64:  { int64_t v; MPI_Recv(&v, 1, MPI_LONG_LONG, src, tag, comm, MPI_STATUS_IGNORE);
                               out = v; break; }
        case AnyTag::UINT64: { uint64_t v; MPI_Recv(&v, 1, MPI_UNSIGNED_LONG_LONG, src, tag, comm, MPI_STATUS_IGNORE);
                               out = v; break; }
        case AnyTag::FLOAT:  { float v; MPI_Recv(&v, 1, MPI_FLOAT, src, tag, comm, MPI_STATUS_IGNORE);
                               out = v; break; }
        case AnyTag::DOUBLE: { double v; MPI_Recv(&v, 1, MPI_DOUBLE, src, tag, comm, MPI_STATUS_IGNORE);
                               out = v; break; }
        case AnyTag::STRING: { std::string s; mpi_recv_string(s, src, tag, comm);
                               out = std::move(s); break; }
        case AnyTag::VEC_INT:    { std::vector<int> v; mpi_recv_vector(v, src, tag, comm);
                                   out = std::move(v); break; }
        case AnyTag::VEC_INT64:  { std::vector<int64_t> v; mpi_recv_vector(v, src, tag, comm);
                                   out = std::move(v); break; }
        case AnyTag::VEC_UINT64: { std::vector<uint64_t> v; mpi_recv_vector(v, src, tag, comm);
                                   out = std::move(v); break; }
        case AnyTag::VEC_FLOAT:  { std::vector<float> v; mpi_recv_vector(v, src, tag, comm);
                                   out = std::move(v); break; }
        case AnyTag::VEC_DOUBLE: { std::vector<double> v; mpi_recv_vector(v, src, tag, comm);
                                   out = std::move(v); break; }
        case AnyTag::VEC_STRING: { int n; MPI_Recv(&n, 1, MPI_INT, src, tag, comm, MPI_STATUS_IGNORE);
                                   std::vector<std::string> vs; vs.reserve(n);
                                   for (int i=0;i<n;++i){ std::string s; mpi_recv_string(s, src, tag, comm); vs.push_back(std::move(s)); }
                                   out = std::move(vs); break; }
    }
}

// ===== Maps with std::any values (string->any shown; adapt K as needed) =====
inline void mpi_send_map_string_any(const std::unordered_map<std::string, std::any>& m,
                                    int dest, int tag, MPI_Comm comm)
{
    int n = static_cast<int>(m.size());
    MPI_Send(&n, 1, MPI_INT, dest, tag, comm);
    for (const auto& [k, v] : m) {
        mpi_send_string(k, dest, tag, comm);
        mpi_send_any(v,    dest, tag, comm);
    }
}
inline void mpi_recv_map_string_any(std::unordered_map<std::string, std::any>& m,
                                    int src, int tag, MPI_Comm comm)
{
    int n; MPI_Recv(&n, 1, MPI_INT, src, tag, comm, MPI_STATUS_IGNORE);
    m.clear(); m.reserve(static_cast<size_t>(n));
    for (int i=0;i<n;++i) {
        std::string k; mpi_recv_string(k, src, tag, comm);
        std::any v;   mpi_recv_any(v,    src, tag, comm);
        m.emplace(std::move(k), std::move(v));
    }
}

#endif  // DFTRACER_UTILS_UTILS_MPI_HELPERS_H
