#include <dftracer/utils/utils/mpi.h>

#if DFTRACER_UTILS_MPI_ENABLE == 1

#include <iostream>
#include <sstream>

namespace dftracer {
namespace utils {

MPI& MPI::instance() {
    static MPI instance_;
    return instance_;
}

MPI::~MPI() {
    if (we_initialized_ && !finalized_) {
        // Clean up MPI if we initialized it and it hasn't been finalized
        try {
            finalize();
        } catch (...) {
            // Ignore exceptions in destructor
        }
    }
}

void MPI::init(int* argc, char*** argv) {
    int flag;
    MPI_Initialized(&flag);

    if (flag) {
        // MPI already initialized by someone else
        initialized_ = true;
        we_initialized_ = false;
        update_cache();
    } else {
        // We need to initialize MPI
        int error = MPI_Init(argc, argv);
        check_error(error, "MPI_Init");

        initialized_ = true;
        we_initialized_ = true;
        update_cache();

        if (is_master()) {
            std::cout << "[MPI] Initialized with " << size() << " processes"
                      << std::endl;
        }
    }
}

void MPI::finalize() {
    if (!initialized_ || finalized_) {
        return;  // Nothing to do
    }

    int flag;
    MPI_Finalized(&flag);

    if (!flag && we_initialized_) {
        // Only finalize if we initialized MPI and it hasn't been finalized
        int error = MPI_Finalize();
        check_error(error, "MPI_Finalize");
        finalized_ = true;

        // Clear cache since MPI is no longer available
        cached_rank_ = -1;
        cached_size_ = -1;
    }
}

bool MPI::is_initialized() const {
    int flag;
    MPI_Initialized(&flag);
    return flag != 0;
}

bool MPI::is_finalized() const {
    int flag;
    MPI_Finalized(&flag);
    return flag != 0;
}

int MPI::rank() const {
    if (cached_rank_ == -1) {
        update_cache();
    }
    return cached_rank_;
}

int MPI::size() const {
    if (cached_size_ == -1) {
        update_cache();
    }
    return cached_size_;
}

void MPI::barrier() {
    int error = MPI_Barrier(MPI_COMM_WORLD);
    check_error(error, "MPI_Barrier");
}

void MPI::broadcast(void* data, int count, MPI_Datatype datatype, int root) {
    int error = MPI_Bcast(data, count, datatype, root, MPI_COMM_WORLD);
    check_error(error, "MPI_Bcast");
}

void MPI::send(const void* data, int count, MPI_Datatype datatype, int dest,
               int tag) {
    int error = MPI_Send(data, count, datatype, dest, tag, MPI_COMM_WORLD);
    check_error(error, "MPI_Send");
}

void MPI::recv(void* data, int count, MPI_Datatype datatype, int source,
               int tag, MPI_Status* status) {
    MPI_Status local_status;
    MPI_Status* status_ptr = status ? status : &local_status;

    int error = MPI_Recv(data, count, datatype, source, tag, MPI_COMM_WORLD,
                         status_ptr);
    check_error(error, "MPI_Recv");
}

void MPI::abort(int errorcode) { MPI_Abort(MPI_COMM_WORLD, errorcode); }

std::vector<uint8_t> MPI::broadcast_vector(const std::vector<uint8_t>& data,
                                           int root) {
    // Broadcast the size first
    int data_size = static_cast<int>(data.size());
    broadcast(&data_size, 1, MPI_INT, root);

    std::vector<uint8_t> result;
    if (rank() == root) {
        result = data;  // Root already has the data
    } else {
        result.resize(data_size);  // Non-root processes allocate buffer
    }

    // Broadcast the actual data
    if (data_size > 0) {
        broadcast(result.data(), data_size, MPI_BYTE, root);
    }

    return result;
}

void MPI::send_vector(const std::vector<uint8_t>& data, int dest, int tag) {
    // Send size first
    int data_size = static_cast<int>(data.size());
    send(&data_size, 1, MPI_INT, dest, tag);

    // Send data
    if (data_size > 0) {
        send(data.data(), data_size, MPI_BYTE, dest, tag + 1000);
    }
}

std::vector<uint8_t> MPI::recv_vector(int source, int tag) {
    // Receive size first
    int data_size;
    recv(&data_size, 1, MPI_INT, source, tag);

    // Receive data
    std::vector<uint8_t> result(data_size);
    if (data_size > 0) {
        recv(result.data(), data_size, MPI_BYTE, source, tag + 1000);
    }

    return result;
}

void MPI::update_cache() const {
    if (!is_initialized()) {
        cached_rank_ = 0;
        cached_size_ = 1;
        return;
    }

    int error;

    error = MPI_Comm_rank(MPI_COMM_WORLD, &cached_rank_);
    if (error != MPI_SUCCESS) {
        cached_rank_ = 0;
    }

    error = MPI_Comm_size(MPI_COMM_WORLD, &cached_size_);
    if (error != MPI_SUCCESS) {
        cached_size_ = 1;
    }
}

void MPI::check_error(int error_code, const char* operation) const {
    if (error_code == MPI_SUCCESS) {
        return;
    }

    // Get error string
    char error_string[MPI_MAX_ERROR_STRING];
    int length;
    MPI_Error_string(error_code, error_string, &length);

    std::ostringstream oss;
    oss << "MPI Error in " << operation << ": " << error_string
        << " (code: " << error_code << ")";

    throw std::runtime_error(oss.str());
}

}  // namespace utils
}  // namespace dftracer

#endif  // DFTRACER_UTILS_MPI_ENABLE