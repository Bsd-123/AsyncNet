#include "asyncnet/ring_buffer.hpp"

#include <unistd.h>

#include <cstring>
#include <string>

#include <gtest/gtest.h>

#include "support/socket_pair.hpp"

using asyncnet::test::SocketPair;

TEST(RingBuffer, StartsEmptyWithFullFreeSpace) {
    asyncnet::RingBuffer rb(8);
    EXPECT_EQ(rb.size(), 0u);
    EXPECT_EQ(rb.capacity(), 8u);
    EXPECT_EQ(rb.freeSpace(), 8u);
    EXPECT_TRUE(rb.empty());
    EXPECT_FALSE(rb.full());
}

TEST(RingBuffer, AppendAndPeekRoundTripWithoutConsuming) {
    asyncnet::RingBuffer rb(8);
    ASSERT_EQ(rb.append("ABCD", 4), 4u);
    EXPECT_EQ(rb.size(), 4u);

    char out[4] = {};
    ASSERT_EQ(rb.peek(out, 4), 4u);
    EXPECT_EQ(std::string(out, 4), "ABCD");
    // peek() must not consume.
    EXPECT_EQ(rb.size(), 4u);
}

TEST(RingBuffer, ConsumeAdvancesHeadAndReducesSize) {
    asyncnet::RingBuffer rb(8);
    ASSERT_EQ(rb.append("ABCD", 4), 4u);
    rb.consume(3);
    EXPECT_EQ(rb.size(), 1u);

    char out[1] = {};
    ASSERT_EQ(rb.peek(out, 1), 1u);
    EXPECT_EQ(out[0], 'D');
}

TEST(RingBuffer, ConsumeMoreThanSizeClampsToEmpty) {
    asyncnet::RingBuffer rb(8);
    ASSERT_EQ(rb.append("AB", 2), 2u);
    rb.consume(100);
    EXPECT_TRUE(rb.empty());
    EXPECT_EQ(rb.freeSpace(), 8u);
}

TEST(RingBuffer, AppendBeyondCapacityIsBoundedAndFillsBuffer) {
    asyncnet::RingBuffer rb(4);
    EXPECT_EQ(rb.append("ABCDEFGH", 8), 4u);
    EXPECT_TRUE(rb.full());
    EXPECT_EQ(rb.freeSpace(), 0u);

    char out[4] = {};
    ASSERT_EQ(rb.peek(out, 4), 4u);
    EXPECT_EQ(std::string(out, 4), "ABCD");
}

TEST(RingBuffer, WrapAroundRoundTripPreservesData) {
    asyncnet::RingBuffer rb(8);

    ASSERT_EQ(rb.append("ABCDE", 5), 5u); // head=0 size=5
    rb.consume(3);                         // head=3 size=2 ("DE" logically remain)

    // writePos is now (3+2)%8 == 5, with only 3 contiguous slots (5,6,7)
    // before the physical end -- appending 6 bytes must wrap.
    ASSERT_EQ(rb.append("FGHIJK", 6), 6u);
    EXPECT_TRUE(rb.full());
    EXPECT_EQ(rb.size(), 8u);

    char out[8] = {};
    ASSERT_EQ(rb.peek(out, 8), 8u);
    EXPECT_EQ(std::string(out, 8), "DEFGHIJK");
}

TEST(RingBuffer, FillFromReturnsWouldBlockWhenNoDataPending) {
    SocketPair sp(/*nonBlocking=*/true);
    asyncnet::RingBuffer rb(16);

    const asyncnet::IOOutcome outcome = rb.fillFrom(sp.readEnd);
    EXPECT_EQ(outcome.status, asyncnet::IOStatus::WouldBlock);
    EXPECT_EQ(outcome.bytesTransferred, 0u);
    EXPECT_EQ(rb.size(), 0u);
}

TEST(RingBuffer, FillFromDistinguishesPeerCloseFromWouldBlock) {
    SocketPair sp(/*nonBlocking=*/true);
    close(sp.writeEnd);
    sp.writeEnd = -1;

    asyncnet::RingBuffer rb(16);
    const asyncnet::IOOutcome outcome = rb.fillFrom(sp.readEnd);
    EXPECT_EQ(outcome.status, asyncnet::IOStatus::Closed);
    EXPECT_EQ(outcome.bytesTransferred, 0u);
}

TEST(RingBuffer, FillFromWhenFullReturnsNothingToDoWithoutSyscall) {
    SocketPair sp(/*nonBlocking=*/true);
    asyncnet::RingBuffer rb(4);
    ASSERT_EQ(rb.append("ABCD", 4), 4u);

    const asyncnet::IOOutcome outcome = rb.fillFrom(sp.readEnd);
    EXPECT_EQ(outcome.status, asyncnet::IOStatus::NothingToDo);
    EXPECT_EQ(outcome.bytesTransferred, 0u);
}

TEST(RingBuffer, DrainToWhenEmptyReturnsNothingToDoWithoutSyscall) {
    SocketPair sp(/*nonBlocking=*/true);
    asyncnet::RingBuffer rb(16);

    const asyncnet::IOOutcome outcome = rb.drainTo(sp.writeEnd);
    EXPECT_EQ(outcome.status, asyncnet::IOStatus::NothingToDo);
    EXPECT_EQ(outcome.bytesTransferred, 0u);
}

TEST(RingBuffer, FillFromReadsAcrossWrapInOneVectoredCall) {
    SocketPair sp(/*nonBlocking=*/true);
    asyncnet::RingBuffer rb(8);

    // Get head into a non-zero, wrapped writable layout: head=5, size=0,
    // free=8, writePos=5 -- 3 contiguous slots before the physical end,
    // 5 after wrapping to the start.
    ASSERT_EQ(rb.append("XXXXX", 5), 5u);
    rb.consume(5);
    ASSERT_TRUE(rb.empty());

    const std::string sendData = "abcdefgh"; // exactly 8 bytes, fills free space
    ASSERT_EQ(write(sp.writeEnd, sendData.data(), sendData.size()),
              static_cast<ssize_t>(sendData.size()));

    // Give the kernel a moment to make the bytes available for reading.
    const asyncnet::IOOutcome outcome = rb.fillFrom(sp.readEnd);
    ASSERT_EQ(outcome.status, asyncnet::IOStatus::Ok);
    EXPECT_EQ(outcome.bytesTransferred, 8u);
    EXPECT_TRUE(rb.full());

    char out[8] = {};
    ASSERT_EQ(rb.peek(out, 8), 8u);
    EXPECT_EQ(std::string(out, 8), sendData);
}

TEST(RingBuffer, FillFromPartialReadSpanningBothIovecsUpdatesSizeCorrectly) {
    SocketPair sp(/*nonBlocking=*/true);
    asyncnet::RingBuffer rb(8);

    // Same wrapped layout as above: writePos=5, iov[0] covers 3 bytes
    // (positions 5,6,7), iov[1] covers 5 bytes (positions 0..4).
    ASSERT_EQ(rb.append("XXXXX", 5), 5u);
    rb.consume(5);
    ASSERT_TRUE(rb.empty());

    // Send 5 bytes: fills iov[0] (3 bytes) fully and continues 2 bytes into
    // iov[1] -- a partial completion relative to the full 8-byte capacity,
    // spanning the wrap boundary.
    const std::string sendData = "abcde";
    ASSERT_EQ(write(sp.writeEnd, sendData.data(), sendData.size()),
              static_cast<ssize_t>(sendData.size()));

    const asyncnet::IOOutcome outcome = rb.fillFrom(sp.readEnd);
    ASSERT_EQ(outcome.status, asyncnet::IOStatus::Ok);
    EXPECT_EQ(outcome.bytesTransferred, 5u);
    EXPECT_EQ(rb.size(), 5u);
    EXPECT_EQ(rb.freeSpace(), 3u);

    char out[5] = {};
    ASSERT_EQ(rb.peek(out, 5), 5u);
    EXPECT_EQ(std::string(out, 5), sendData);
}

TEST(RingBuffer, DrainToWritesAcrossWrapInOneVectoredCall) {
    SocketPair sp(/*nonBlocking=*/true);
    asyncnet::RingBuffer rb(8);

    ASSERT_EQ(rb.append("ABCDE", 5), 5u);
    rb.consume(3); // head=3 size=2, writePos=(3+2)%8=5
    ASSERT_EQ(rb.append("FGHIJK", 6), 6u); // wraps on the way in, full now
    ASSERT_TRUE(rb.full());

    const asyncnet::IOOutcome outcome = rb.drainTo(sp.writeEnd);
    ASSERT_EQ(outcome.status, asyncnet::IOStatus::Ok);
    EXPECT_EQ(outcome.bytesTransferred, 8u);
    EXPECT_TRUE(rb.empty());

    char received[8] = {};
    ssize_t total = 0;
    while (total < 8) {
        const ssize_t n = read(sp.readEnd, received + total, 8 - total);
        ASSERT_GT(n, 0);
        total += n;
    }
    EXPECT_EQ(std::string(received, 8), "DEFGHIJK");
}
