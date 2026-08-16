#pragma once

#include <cstddef>
#include <functional>

#include "asyncnet/event_loop.hpp"
#include "asyncnet/ring_buffer.hpp"

namespace asyncnet {

// Per-fd connection: owns the read/write RingBuffers, and drives the
// read -> (echo) -> write pipeline for one TCP connection. Readable and
// writable are handled independently per the connection model (architecture
// doc §3) -- neither path blocks on or is serialized behind the other.
//
// M1's application logic is plain echo: bytes read are copied straight into
// the write buffer, unparsed. Protocol parsing arrives in M2.
class Connection {
public:
    using ClosedHandler = std::function<void(int fd)>;
    // Invoked whenever bytes are actually read from the peer -- the signal
    // an idle-timeout mechanism cares about (architecture doc §5). Not
    // invoked on writes: us successfully flushing data says nothing about
    // whether the peer is still there.
    using ActivityHandler = std::function<void(int fd)>;

    static constexpr std::size_t kDefaultBufferCapacity = 64 * 1024;

    Connection(EventLoop& loop, int fd, ClosedHandler onClosed,
               std::size_t bufferCapacity = kDefaultBufferCapacity,
               ActivityHandler onActivity = nullptr);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    int fd() const { return fd_; }

    // Stops reading and flushes any buffered response before closing --
    // same graceful-teardown semantics as a peer EOF. Public: this is
    // triggered externally, by an idle-timeout check.
    void closeIdle();

private:
    enum class State { Active, Closing, Closed };

    void onEvent(int fd, IOEvent events);
    void onReadable();
    void onWritable();
    void moveReadableBytesToWriteBuffer();
    void updateInterest();
    // Hysteresis: flips backpressureActive_ on only at the HIGH watermark
    // and off only at LOW, so occupancy hovering near one threshold can't
    // flap EPOLLIN on and off every cycle (architecture doc §4).
    void applyBackpressureWatermarks();

    // Stops reading and, once any buffered response has been flushed,
    // closes the fd. Used for orderly teardown (peer EOF).
    void closeGracefully();
    // Closes the fd right away, discarding anything still buffered. Used
    // for real errors, where continuing to write is pointless.
    void closeImmediately();
    void finalizeClose();

    EventLoop& loop_;
    int fd_;
    RingBuffer readBuffer_;
    RingBuffer writeBuffer_;
    ClosedHandler onClosed_;
    ActivityHandler onActivity_;

    // Above HIGH: stop reading (deregister EPOLLIN) until the write buffer
    // drains back down to LOW. Based purely on write-buffer occupancy --
    // never on how fast (or slowly) the peer happens to be reading.
    std::size_t highWatermark_;
    std::size_t lowWatermark_;

    State state_ = State::Active;
    bool readInterestEnabled_ = true;  // set false once closeGracefully() has run
    bool backpressureActive_ = false;  // toggled purely by write-buffer occupancy
    IOEvent currentInterest_ = IOEvent::None;
};

} // namespace asyncnet
