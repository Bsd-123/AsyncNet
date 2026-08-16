#include "asyncnet/ring_buffer.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

namespace asyncnet {

RingBuffer::RingBuffer(std::size_t capacity) : buffer_(capacity) {}

std::size_t RingBuffer::append(const void* data, std::size_t len) {
    const std::size_t n = std::min(len, freeSpace());
    if (n == 0) {
        return 0;
    }

    const std::size_t writePos = (head_ + size_) % capacity();
    const std::size_t firstChunk = std::min(n, capacity() - writePos);

    std::memcpy(buffer_.data() + writePos, data, firstChunk);
    if (firstChunk < n) {
        std::memcpy(buffer_.data(),
                    static_cast<const std::byte*>(data) + firstChunk,
                    n - firstChunk);
    }

    size_ += n;
    return n;
}

std::size_t RingBuffer::peek(void* dest, std::size_t maxLen) const {
    const std::size_t n = std::min(maxLen, size_);
    if (n == 0) {
        return 0;
    }

    const std::size_t firstChunk = std::min(n, capacity() - head_);
    std::memcpy(dest, buffer_.data() + head_, firstChunk);
    if (firstChunk < n) {
        std::memcpy(static_cast<std::byte*>(dest) + firstChunk, buffer_.data(),
                    n - firstChunk);
    }
    return n;
}

void RingBuffer::consume(std::size_t len) {
    const std::size_t n = std::min(len, size_);
    head_ = (head_ + n) % capacity();
    size_ -= n;
}

RingBuffer::IoVecPair RingBuffer::writableRegions() {
    IoVecPair result;
    const std::size_t free = freeSpace();
    if (free == 0) {
        return result;
    }

    const std::size_t writePos = (head_ + size_) % capacity();
    const std::size_t firstChunk = std::min(free, capacity() - writePos);

    result.vec[0].iov_base = buffer_.data() + writePos;
    result.vec[0].iov_len = firstChunk;
    result.count = 1;

    if (firstChunk < free) {
        result.vec[1].iov_base = buffer_.data();
        result.vec[1].iov_len = free - firstChunk;
        result.count = 2;
    }
    return result;
}

RingBuffer::IoVecPair RingBuffer::readableRegions() const {
    IoVecPair result;
    if (size_ == 0) {
        return result;
    }

    const std::size_t firstChunk = std::min(size_, capacity() - head_);

    // writev() only ever reads through these pointers; struct iovec's
    // iov_base just doesn't have a const-qualified counterpart.
    result.vec[0].iov_base = const_cast<std::byte*>(buffer_.data() + head_);
    result.vec[0].iov_len = firstChunk;
    result.count = 1;

    if (firstChunk < size_) {
        result.vec[1].iov_base = const_cast<std::byte*>(buffer_.data());
        result.vec[1].iov_len = size_ - firstChunk;
        result.count = 2;
    }
    return result;
}

IOOutcome RingBuffer::fillFrom(int fd) {
    if (freeSpace() == 0) {
        return IOOutcome{IOStatus::NothingToDo, 0, 0};
    }

    IoVecPair regions = writableRegions();

    ssize_t n = 0;
    for (;;) {
        n = ::readv(fd, regions.vec, regions.count);
        if (n >= 0 || errno != EINTR) {
            break;
        }
    }

    if (n > 0) {
        size_ += static_cast<std::size_t>(n);
        return IOOutcome{IOStatus::Ok, static_cast<std::size_t>(n), 0};
    }
    if (n == 0) {
        return IOOutcome{IOStatus::Closed, 0, 0};
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return IOOutcome{IOStatus::WouldBlock, 0, 0};
    }
    return IOOutcome{IOStatus::Error, 0, errno};
}

IOOutcome RingBuffer::drainTo(int fd) {
    if (size_ == 0) {
        return IOOutcome{IOStatus::NothingToDo, 0, 0};
    }

    IoVecPair regions = readableRegions();

    ssize_t n = 0;
    for (;;) {
        n = ::writev(fd, regions.vec, regions.count);
        if (n >= 0 || errno != EINTR) {
            break;
        }
    }

    if (n > 0) {
        consume(static_cast<std::size_t>(n));
        return IOOutcome{IOStatus::Ok, static_cast<std::size_t>(n), 0};
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return IOOutcome{IOStatus::WouldBlock, 0, 0};
    }
    return IOOutcome{IOStatus::Error, 0, errno};
}

} // namespace asyncnet
