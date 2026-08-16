#pragma once

#include <sys/uio.h>

#include <cstddef>
#include <vector>

namespace asyncnet {

enum class IOStatus {
    Ok,          // bytesTransferred > 0
    WouldBlock,  // EAGAIN/EWOULDBLOCK: fd has no more capacity right now
    Closed,      // peer performed an orderly shutdown (fillFrom only)
    Error,       // real error; see errnoValue
    NothingToDo, // no free space (fillFrom) / no data (drainTo): syscall skipped
};

struct IOOutcome {
    IOStatus status = IOStatus::NothingToDo;
    std::size_t bytesTransferred = 0;
    int errnoValue = 0; // valid when status == Error
};

// Fixed-capacity circular byte buffer. Fills via readv()/drains via
// writev() using up to two iovec entries to cover wrap-around, instead of
// linearizing via a copy or issuing two separate syscalls -- see
// architecture doc §7.
class RingBuffer {
public:
    explicit RingBuffer(std::size_t capacity);

    std::size_t size() const { return size_; }
    std::size_t capacity() const { return buffer_.size(); }
    std::size_t freeSpace() const { return capacity() - size_; }
    bool empty() const { return size_ == 0; }
    bool full() const { return size_ == capacity(); }

    // Raw memory access, independent of any fd. Both are bounded by what
    // actually fits/exists and report how many bytes they actually moved.
    std::size_t append(const void* data, std::size_t len);
    std::size_t peek(void* dest, std::size_t maxLen) const;
    void consume(std::size_t len);

    // Reads from fd into free space (up to two iovecs for the wrap).
    // Retries internally on EINTR. A 0-byte readv() result (peer closed)
    // is reported as Closed, never conflated with WouldBlock.
    IOOutcome fillFrom(int fd);

    // Writes to fd from used space (up to two iovecs for the wrap).
    // Retries internally on EINTR.
    IOOutcome drainTo(int fd);

private:
    struct IoVecPair {
        iovec vec[2]{};
        int count = 0;
    };

    IoVecPair writableRegions();
    IoVecPair readableRegions() const;

    std::vector<std::byte> buffer_;
    std::size_t head_ = 0; // index of the oldest valid byte
    std::size_t size_ = 0; // number of valid bytes currently stored
};

} // namespace asyncnet
