#include "asyncnet/connection.hpp"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <string>

#include "asyncnet/logger.hpp"

namespace asyncnet {

Connection::Connection(EventLoop& loop, int fd, ClosedHandler onClosed,
                        std::size_t bufferCapacity)
    : loop_(loop),
      fd_(fd),
      readBuffer_(bufferCapacity),
      writeBuffer_(bufferCapacity),
      onClosed_(std::move(onClosed)),
      highWatermark_(bufferCapacity * 3 / 4),
      lowWatermark_(bufferCapacity / 4) {
    loop_.reactor().registerFd(fd_, IOEvent::Readable,
                                [this](int f, IOEvent e) { onEvent(f, e); });
    currentInterest_ = IOEvent::Readable;
}

Connection::~Connection() {
    // Normal teardown always goes through finalizeClose(), which already
    // closed the fd (deferred) and set state_ = Closed. This is a safety
    // net for abrupt teardown paths (e.g. an owner clearing its connection
    // map directly, without draining each one gracefully first) -- fd
    // closed exactly once, ownership never ambiguous either way.
    if (state_ != State::Closed) {
        loop_.reactor().removeFd(fd_);
        close(fd_);
    }
}

void Connection::onEvent(int fd, IOEvent events) {
    if (hasEvent(events, IOEvent::Error)) {
        ASYNCNET_LOG_WARN("connection fd=" + std::to_string(fd) +
                           ": socket error, closing");
        closeImmediately();
        return;
    }

    // Both directions are independent: handling one never blocks on or
    // depends on the other's outcome, even though both can be ready in the
    // same dispatch. HangUp also routes through onReadable() so a final
    // in-flight readv() gets a chance to observe the 0-byte EOF properly
    // rather than the connection being torn down on HangUp alone.
    if (hasEvent(events, IOEvent::Readable) || hasEvent(events, IOEvent::HangUp)) {
        onReadable();
        if (state_ == State::Closed) {
            return;
        }
    }

    if (hasEvent(events, IOEvent::Writable)) {
        onWritable();
    }
}

void Connection::onReadable() {
    for (;;) {
        const IOOutcome outcome = readBuffer_.fillFrom(fd_);

        if (outcome.status == IOStatus::Ok) {
            moveReadableBytesToWriteBuffer();
            applyBackpressureWatermarks();

            if (writeBuffer_.full() && !readBuffer_.empty()) {
                // Write buffer is completely full and there's still more
                // data that couldn't be forwarded into it: the explicit
                // overflow policy is close-on-full (architecture doc §4),
                // not silently dropping bytes and corrupting the stream.
                ASYNCNET_LOG_WARN("connection fd=" + std::to_string(fd_) +
                                   ": write buffer overflow, closing");
                closeImmediately();
                return;
            }
            if (backpressureActive_) {
                break; // HIGH watermark reached: stop reading for this wakeup
            }
            continue; // drain the socket until EAGAIN, per architecture doc §6
        }
        if (outcome.status == IOStatus::Closed) {
            closeGracefully();
            return;
        }
        if (outcome.status == IOStatus::Error) {
            ASYNCNET_LOG_WARN("connection fd=" + std::to_string(fd_) +
                               ": read error: " + std::strerror(outcome.errnoValue));
            closeImmediately();
            return;
        }
        // WouldBlock: socket drained for this wakeup.
        // NothingToDo: readBuffer_ has no free space right now.
        break;
    }
    updateInterest();
}

void Connection::onWritable() {
    for (;;) {
        const IOOutcome outcome = writeBuffer_.drainTo(fd_);

        if (outcome.status == IOStatus::Ok) {
            applyBackpressureWatermarks();
            continue; // keep draining until EAGAIN or the buffer empties
        }
        if (outcome.status == IOStatus::Error) {
            ASYNCNET_LOG_WARN("connection fd=" + std::to_string(fd_) +
                               ": write error: " + std::strerror(outcome.errnoValue));
            closeImmediately();
            return;
        }
        // WouldBlock: socket's send buffer is full for now.
        // NothingToDo: writeBuffer_ is empty, nothing queued.
        break;
    }

    if (state_ == State::Closing && writeBuffer_.empty()) {
        finalizeClose();
        return;
    }
    updateInterest();
}

void Connection::moveReadableBytesToWriteBuffer() {
    std::array<std::byte, 4096> chunk{};
    while (!readBuffer_.empty() && writeBuffer_.freeSpace() > 0) {
        const std::size_t want =
            std::min({chunk.size(), readBuffer_.size(), writeBuffer_.freeSpace()});
        const std::size_t got = readBuffer_.peek(chunk.data(), want);
        writeBuffer_.append(chunk.data(), got); // fits: got <= freeSpace() by construction
        readBuffer_.consume(got);
    }
}

void Connection::updateInterest() {
    if (state_ == State::Closed) {
        return;
    }

    IOEvent desired = IOEvent::None;
    if (readInterestEnabled_ && !backpressureActive_) {
        desired = desired | IOEvent::Readable;
    }
    if (!writeBuffer_.empty()) {
        desired = desired | IOEvent::Writable;
    }

    if (desired != currentInterest_) {
        loop_.reactor().modifyFd(fd_, desired);
        currentInterest_ = desired;
    }
}

void Connection::applyBackpressureWatermarks() {
    if (!backpressureActive_ && writeBuffer_.size() >= highWatermark_) {
        backpressureActive_ = true;
    } else if (backpressureActive_ && writeBuffer_.size() <= lowWatermark_) {
        backpressureActive_ = false;
    }
}

void Connection::closeGracefully() {
    if (state_ != State::Active) {
        return;
    }
    state_ = State::Closing;
    readInterestEnabled_ = false;

    if (writeBuffer_.empty()) {
        finalizeClose();
        return;
    }
    updateInterest();
}

void Connection::closeImmediately() {
    finalizeClose();
}

void Connection::finalizeClose() {
    if (state_ == State::Closed) {
        return;
    }
    state_ = State::Closed;
    loop_.reactor().removeFd(fd_);

    const int fd = fd_;
    ClosedHandler onClosed = onClosed_;
    loop_.post([fd, onClosed = std::move(onClosed)]() {
        // Deferred until the current dispatch batch fully finishes, so the
        // OS can't hand this fd number to a new connection while other
        // handlers in this same batch might still reference it.
        close(fd);
        if (onClosed) {
            onClosed(fd);
        }
    });
}

} // namespace asyncnet
