#ifndef DFTRACER_UTILS_UTILS_MPI_H
#define DFTRACER_UTILS_UTILS_MPI_H

#include <dftracer/utils/common/config.h>

#if DFTRACER_UTILS_MPI_ENABLE == 1

#include <mpi.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

namespace dftracer {
namespace utils {

class MPI {
   public:
    /**
     * Singleton MPI instance
     */
    static MPI& instance();

    /**
     * safe to call multiple times
     */
    void init(int* argc = nullptr, char*** argv = nullptr);

    /**
     * safe to call multiple times
     */
    void finalize();
    bool is_initialized() const;
    bool is_finalized() const;
    int rank() const;
    int size() const;
    bool is_master() const { return rank() == 0; }
    void barrier();
    void broadcast(void* data, int count, MPI_Datatype datatype, int root);
    void send(const void* data, int count, MPI_Datatype datatype, int dest,
              int tag);
    void recv(void* data, int count, MPI_Datatype datatype, int source, int tag,
              MPI_Status* status = nullptr);
    void abort(int errorcode = 1);
    std::vector<uint8_t> broadcast_vector(const std::vector<uint8_t>& data,
                                          int root);
    void send_vector(const std::vector<uint8_t>& data, int dest, int tag);
    std::vector<uint8_t> recv_vector(int source, int tag);

   private:
    MPI() = default;
    ~MPI();

    MPI(const MPI&) = delete;
    MPI& operator=(const MPI&) = delete;
    MPI(MPI&&) = delete;
    MPI& operator=(MPI&&) = delete;

    bool initialized_ = false;
    bool finalized_ = false;
    bool we_initialized_ = false;

    mutable int cached_rank_ = -1;
    mutable int cached_size_ = -1;

    void update_cache() const;
    void check_error(int error_code, const char* operation) const;
};

class MPISession {
   public:
    MPISession(int* argc = nullptr, char*** argv = nullptr) {
        MPI::instance().init(argc, argv);
    }

    ~MPISession() { MPI::instance().finalize(); }

    // Non-copyable, non-movable
    MPISession(const MPISession&) = delete;
    MPISession& operator=(const MPISession&) = delete;
    MPISession(MPISession&&) = delete;
    MPISession& operator=(MPISession&&) = delete;
};

}  // namespace utils
}  // namespace dftracer

#else  // DFTRACER_UTILS_MPI_ENABLE != 1

namespace dftracer {
namespace utils {

class MPI {
   public:
    static MPI& instance() {
        static MPI instance_;
        return instance_;
    }

    void init(int* = nullptr, char*** = nullptr) {}
    void finalize() {}
    bool is_initialized() const { return false; }
    bool is_finalized() const { return false; }
    int rank() const { return 0; }
    int size() const { return 1; }
    bool is_master() const { return true; }
    void barrier() {}
    void abort(int = 1) { throw std::runtime_error("MPI is disabled"); }
    void broadcast(void*, int, int, int) {}
    void send(const void*, int, int, int, int) {}
    void recv(void*, int, int, int, int, void* = nullptr) {}
    std::vector<uint8_t> broadcast_vector(const std::vector<uint8_t>& data,
                                          int) {
        return data;
    }
    void send_vector(const std::vector<uint8_t>&, int, int) {}
    std::vector<uint8_t> recv_vector(int, int) { return {}; }
};

class MPISession {
   public:
    MPISession(int* = nullptr, char*** = nullptr) {}
};

}  // namespace utils
}  // namespace dftracer

#endif  // DFTRACER_UTILS_MPI_ENABLE

#endif  // DFTRACER_UTILS_UTILS_MPI_H
