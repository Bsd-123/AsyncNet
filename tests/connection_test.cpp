#include "asyncnet/connection.hpp"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "support/socket_pair.hpp"

using asyncnet::test::SocketPair;

namespace {

void setNonBlocking(int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

std::string readExactly(int fd, std::size_t n) {
    std::string out(n, '\0');
    std::size_t total = 0;
    while (total < n) {
        const ssize_t got = read(fd, out.data() + total, n - total);
        if (got <= 0) {
            throw std::runtime_error("readExactly: unexpected short read/EOF");
        }
        total += static_cast<std::size_t>(got);
    }
    return out;
}

} // namespace

TEST(Connection, EchoesDataBackToClient) {
    asyncnet::EventLoop loop;
    SocketPair sp;
    const int connFd = sp.readEnd;
    sp.readEnd = -1; // ownership transferred to Connection
    setNonBlocking(connFd);

    bool closed = false;
    asyncnet::Connection conn(loop, connFd, [&](int) { closed = true; });

    const std::string msg = "hello asyncnet";
    ASSERT_EQ(write(sp.writeEnd, msg.data(), msg.size()),
              static_cast<ssize_t>(msg.size()));

    // Cycle 1: read arrives, gets echoed into the write buffer, Writable
    // interest gets registered (takes effect on the *next* poll()).
    loop.runOnce(200);
    // Cycle 2: the now-writable fd drains the echoed bytes back out.
    loop.runOnce(200);

    EXPECT_EQ(readExactly(sp.writeEnd, msg.size()), msg);
    EXPECT_FALSE(closed);
}

TEST(Connection, SurvivesMultiplePartialReadsAndEchoesEachCorrectly) {
    asyncnet::EventLoop loop;
    SocketPair sp;
    const int connFd = sp.readEnd;
    sp.readEnd = -1;
    setNonBlocking(connFd);

    asyncnet::Connection conn(loop, connFd, [](int) {});

    const std::string part1 = "Hello, ";
    ASSERT_EQ(write(sp.writeEnd, part1.data(), part1.size()),
              static_cast<ssize_t>(part1.size()));
    loop.runOnce(200);
    loop.runOnce(200);
    EXPECT_EQ(readExactly(sp.writeEnd, part1.size()), part1);

    const std::string part2 = "World!";
    ASSERT_EQ(write(sp.writeEnd, part2.data(), part2.size()),
              static_cast<ssize_t>(part2.size()));
    loop.runOnce(200);
    loop.runOnce(200);
    EXPECT_EQ(readExactly(sp.writeEnd, part2.size()), part2);
}

TEST(Connection, PeerDisconnectIsCleanedUpWithoutCrashOrFdLeak) {
    asyncnet::EventLoop loop;
    SocketPair sp;
    const int connFd = sp.readEnd;
    sp.readEnd = -1;
    setNonBlocking(connFd);

    bool closedCalled = false;
    int closedFd = -1;
    asyncnet::Connection conn(loop, connFd, [&](int fd) {
        closedCalled = true;
        closedFd = fd;
    });

    close(sp.writeEnd); // peer disconnects mid-"transfer" (nothing pending)
    sp.writeEnd = -1;

    // runOnce() dispatches the batch (detects peer close -> closeGracefully,
    // writeBuffer_ already empty -> finalizeClose) and then flushes the
    // deferred close()+onClosed callback within the same call.
    loop.runOnce(200);

    EXPECT_TRUE(closedCalled);
    EXPECT_EQ(closedFd, connFd);

    // The fd must actually be close()'d, not just deregistered: a second
    // close() on an already-closed fd fails with EBADF.
    errno = 0;
    EXPECT_EQ(close(connFd), -1);
    EXPECT_EQ(errno, EBADF);
}

TEST(Connection, FlushesPendingWritesBeforeClosingAfterPeerEof) {
    asyncnet::EventLoop loop;
    SocketPair sp;
    const int connFd = sp.readEnd;
    sp.readEnd = -1;
    setNonBlocking(connFd);

    bool closedCalled = false;
    asyncnet::Connection conn(loop, connFd, [&](int) { closedCalled = true; });

    const std::string msg = "final message";
    ASSERT_EQ(write(sp.writeEnd, msg.data(), msg.size()),
              static_cast<ssize_t>(msg.size()));
    // Half-close: no more data will arrive, but the client can still read.
    shutdown(sp.writeEnd, SHUT_WR);

    // Cycle 1: reads "final message" AND observes EOF, echoes the message
    // into the write buffer, then closeGracefully() defers closing until
    // the buffer drains (it isn't empty yet, so no close this cycle).
    loop.runOnce(200);
    EXPECT_FALSE(closedCalled);

    // Cycle 2: the now-writable fd flushes the echoed bytes, and since the
    // connection was already Closing, finalizeClose() runs right after.
    loop.runOnce(200);

    EXPECT_EQ(readExactly(sp.writeEnd, msg.size()), msg);
    EXPECT_TRUE(closedCalled);
}

TEST(Connection, HandlesSimultaneousReadableAndWritableInOneDispatch) {
    asyncnet::EventLoop loop;
    SocketPair sp;
    const int connFd = sp.readEnd;
    sp.readEnd = -1;
    setNonBlocking(connFd);

    asyncnet::Connection conn(loop, connFd, [](int) {});

    ASSERT_EQ(write(sp.writeEnd, "AAA", 3), 3);
    // Leaves "AAA"'s echo sitting unflushed in the write buffer: Writable
    // interest was just registered, but only takes effect on the next
    // poll() call, not this one.
    loop.runOnce(200);

    ASSERT_EQ(write(sp.writeEnd, "BBB", 3), 3);
    // connFd is now both readable ("BBB" pending) and writable (the "AAA"
    // echo still unflushed, socket still writable) at the same time --
    // epoll reports both bits in a single event for this one dispatch.
    loop.runOnce(200);

    EXPECT_EQ(readExactly(sp.writeEnd, 6), "AAABBB");
}

TEST(Connection, HighWatermarkDeregistersReadingUntilDrainedBelowLow) {
    asyncnet::EventLoop loop;
    SocketPair sp;
    const int connFd = sp.readEnd;
    sp.readEnd = -1;
    setNonBlocking(connFd);

    // capacity=32 -> HIGH=24, LOW=8.
    asyncnet::Connection conn(loop, connFd, [](int) {}, 32);

    // A single read that alone crosses HIGH must stop further reading --
    // this is a pure occupancy decision, made regardless of how the peer
    // behaves (architecture doc §4: "based on write-buffer occupancy
    // only"). 30 bytes fits in the 32-byte buffers with room to spare, so
    // this isn't an overflow case.
    const std::string first(30, 'A');
    ASSERT_EQ(write(sp.writeEnd, first.data(), first.size()),
              static_cast<ssize_t>(first.size()));
    loop.runOnce(200); // reads all 30, crosses HIGH(24), deregisters Readable

    // Sent while Readable is deregistered: must NOT be picked up yet, even
    // though the kernel socket buffer and the reactor would happily
    // deliver it if interest were still registered.
    const std::string second(5, 'B');
    ASSERT_EQ(write(sp.writeEnd, second.data(), second.size()),
              static_cast<ssize_t>(second.size()));

    // Drains the first 30 bytes out (Writable was registered regardless of
    // backpressure), dropping occupancy to 0 <= LOW(8): reading resumes.
    loop.runOnce(200);
    EXPECT_EQ(readExactly(sp.writeEnd, first.size()), first);

    // Only now (Readable interest restored) does the second write get
    // read, echoed, and flushed.
    loop.runOnce(200);
    loop.runOnce(200);
    EXPECT_EQ(readExactly(sp.writeEnd, second.size()), second);
}

TEST(Connection, WriteBufferOverflowClosesWithoutAffectingOtherConnections) {
    asyncnet::EventLoop loop;

    SocketPair spA;
    const int fdA = spA.readEnd;
    spA.readEnd = -1;
    setNonBlocking(fdA);

    SocketPair spB;
    const int fdB = spB.readEnd;
    spB.readEnd = -1;
    setNonBlocking(fdB);

    bool aClosed = false;
    // capacity=8 -> HIGH=6, LOW=2.
    asyncnet::Connection connA(loop, fdA, [&](int) { aClosed = true; }, 8);
    asyncnet::Connection connB(loop, fdB, [](int) {}, 8);

    // First 3 bytes stay under HIGH(6), so Readable interest is still on
    // going into the next read.
    ASSERT_EQ(write(spA.writeEnd, "AAA", 3), 3);
    loop.runOnce(200);

    // 8 more bytes arrive before anything drains: only 5 fit in the
    // remaining free space (8-3), leaving 3 stuck in the read buffer with
    // the write buffer completely full -- genuine overflow, not just
    // "backpressure should have kicked in sooner".
    ASSERT_EQ(write(spA.writeEnd, "BBBBBBBB", 8), 8);
    loop.runOnce(200);

    EXPECT_TRUE(aClosed);

    // Connection B, sharing the same loop, must be entirely unaffected.
    // Well under its own HIGH watermark (6), so this alone doesn't trip
    // backpressure and echoes back within the usual two cycles.
    ASSERT_EQ(write(spB.writeEnd, "ok", 2), 2);
    loop.runOnce(200);
    loop.runOnce(200);
    EXPECT_EQ(readExactly(spB.writeEnd, 2), "ok");
}

TEST(Connection, ActivityCallbackFiresOnReadButNotOnWriteOnly) {
    asyncnet::EventLoop loop;
    SocketPair sp;
    const int connFd = sp.readEnd;
    sp.readEnd = -1;
    setNonBlocking(connFd);

    std::vector<int> activityFds;
    asyncnet::Connection conn(
        loop, connFd, [](int) {}, asyncnet::Connection::kDefaultBufferCapacity,
        [&](int fd) { activityFds.push_back(fd); });

    ASSERT_EQ(write(sp.writeEnd, "x", 1), 1);
    loop.runOnce(200); // triggers a read
    ASSERT_EQ(activityFds.size(), 1u);
    EXPECT_EQ(activityFds[0], connFd);

    loop.runOnce(200); // flushes the echo (a write, not a read)
    EXPECT_EQ(activityFds.size(), 1u); // unchanged: writes aren't activity

    EXPECT_EQ(readExactly(sp.writeEnd, 1), "x");
}
