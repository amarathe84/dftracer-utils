#ifndef DFTRACER_UTILS_UTILS_MPI_H
#define DFTRACER_UTILS_UTILS_MPI_H

#include <dftracer/utils/common/config.h>

#include <mpi.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

typedef MPI_Request* MPI_Request_ptr;
typedef MPI_Status* MPI_Status_ptr;

class MPIContext {
   public:
    /**
     * Singleton MPI instance
     */
    static MPIContext& instance();

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
    void isend(const void* data, int count, MPI_Datatype datatype, int dest,
               int tag, MPI_Request* request);
    void irecv(void* data, int count, MPI_Datatype datatype, int source,
               int tag, MPI_Request* request);
    bool test(MPI_Request* request, MPI_Status* status = nullptr);
    void wait(MPI_Request* request, MPI_Status* status = nullptr);
    int probe_any_source(int tag, MPI_Status* status = nullptr);
    inline MPI_Comm comm() const { return MPI_COMM_WORLD; }

   private:
    MPIContext() = default;
    ~MPIContext();

    MPIContext(const MPIContext&) = delete;
    MPIContext& operator=(const MPIContext&) = delete;
    MPIContext(MPIContext&&) = delete;
    MPIContext& operator=(MPIContext&&) = delete;

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
        MPIContext::instance().init(argc, argv);
    }

    ~MPISession() { MPIContext::instance().finalize(); }

    // Non-copyable, non-movable
    MPISession(const MPISession&) = delete;
    MPISession& operator=(const MPISession&) = delete;
    MPISession(MPISession&&) = delete;
    MPISession& operator=(MPISession&&) = delete;
};

#endif  // DFTRACER_UTILS_UTILS_MPI_H
