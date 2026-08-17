#include "asyncnet/idle_timeout_manager.hpp"

#include <vector>

#include <gtest/gtest.h>

using Clock = asyncnet::IdleTimeoutManager::Clock;
using namespace std::chrono_literals;

TEST(IdleTimeoutManager, ConnectionExpiresAfterTimeoutWithNoActivity) {
    asyncnet::IdleTimeoutManager mgr(100ms);
    const auto t0 = Clock::now();

    mgr.recordActivity(1, t0);

    std::vector<int> expired;
    mgr.checkExpired([&](int id) { expired.push_back(id); }, t0 + 50ms);
    EXPECT_TRUE(expired.empty()); // not yet timed out

    mgr.checkExpired([&](int id) { expired.push_back(id); }, t0 + 150ms);
    ASSERT_EQ(expired.size(), 1u);
    EXPECT_EQ(expired[0], 1);
}

TEST(IdleTimeoutManager, PeriodicActivityPreventsExpiryDespiteStaleEntries) {
    asyncnet::IdleTimeoutManager mgr(100ms);
    const auto t0 = Clock::now();

    mgr.recordActivity(1, t0);
    // Refresh before the original entry would expire -- leaves the first
    // (now-stale) entry sitting in the queue behind the fresh one.
    mgr.recordActivity(1, t0 + 60ms);

    std::vector<int> expired;
    // The STALE entry (due at t0+100ms) is popped and checked here, but
    // real last activity was t0+60ms (expires at t0+160ms), so it must not
    // fire.
    mgr.checkExpired([&](int id) { expired.push_back(id); }, t0 + 120ms);
    EXPECT_TRUE(expired.empty());

    // Past the real expiry now.
    mgr.checkExpired([&](int id) { expired.push_back(id); }, t0 + 170ms);
    ASSERT_EQ(expired.size(), 1u);
    EXPECT_EQ(expired[0], 1);
}

TEST(IdleTimeoutManager, PoppingStaleEntryNeverTriggersExpiryCallback) {
    asyncnet::IdleTimeoutManager mgr(100ms);
    const auto t0 = Clock::now();

    mgr.recordActivity(1, t0);
    mgr.recordActivity(1, t0 + 90ms); // refresh again before expiry

    int callCount = 0;
    // The original stale entry (due at t0+100ms) is popped and discarded
    // here without ever invoking the callback.
    mgr.checkExpired([&](int) { ++callCount; }, t0 + 110ms);
    EXPECT_EQ(callCount, 0);

    // The fresh entry (due at t0+190ms) fires, and only once.
    mgr.checkExpired([&](int) { ++callCount; }, t0 + 200ms);
    EXPECT_EQ(callCount, 1);
}

TEST(IdleTimeoutManager, RemoveStopsTrackingWithoutCrashingOnLaterCheck) {
    asyncnet::IdleTimeoutManager mgr(50ms);
    const auto t0 = Clock::now();

    mgr.recordActivity(1, t0);
    mgr.remove(1);

    int callCount = 0;
    EXPECT_NO_THROW(mgr.checkExpired([&](int) { ++callCount; }, t0 + 100ms));
    EXPECT_EQ(callCount, 0);
}

TEST(IdleTimeoutManager, NextPollTimeoutMsReflectsSoonestExpiry) {
    asyncnet::IdleTimeoutManager mgr(100ms);
    const auto t0 = Clock::now();

    EXPECT_EQ(mgr.nextPollTimeoutMs(t0), -1); // nothing tracked yet

    mgr.recordActivity(1, t0);
    const int timeout = mgr.nextPollTimeoutMs(t0 + 40ms);
    EXPECT_GE(timeout, 55);
    EXPECT_LE(timeout, 60);

    EXPECT_EQ(mgr.nextPollTimeoutMs(t0 + 150ms), 0); // already due
}

TEST(IdleTimeoutManager, NextPollTimeoutMsRoundsUpSubMillisecondRemainderInsteadOfTruncatingToZero) {
    asyncnet::IdleTimeoutManager mgr(100ms);
    const auto t0 = Clock::now();
    mgr.recordActivity(1, t0);

    // 500us before expiry: truncating toward zero milliseconds would
    // wrongly yield 0 (a non-blocking poll) even though the connection
    // isn't actually due yet. A caller looping on that would busy-spin
    // instead of blocking the remaining sliver of a millisecond, since
    // real time barely advances per non-blocking iteration.
    const auto almostDue = t0 + 100ms - std::chrono::microseconds(500);
    EXPECT_EQ(mgr.nextPollTimeoutMs(almostDue), 1);
}

TEST(IdleTimeoutManager, MultipleConnectionsExpireIndependently) {
    asyncnet::IdleTimeoutManager mgr(100ms);
    const auto t0 = Clock::now();

    mgr.recordActivity(1, t0);
    mgr.recordActivity(2, t0 + 50ms);

    std::vector<int> expired;
    mgr.checkExpired([&](int id) { expired.push_back(id); }, t0 + 120ms);
    ASSERT_EQ(expired.size(), 1u);
    EXPECT_EQ(expired[0], 1); // only connection 1 has timed out so far

    mgr.checkExpired([&](int id) { expired.push_back(id); }, t0 + 170ms);
    ASSERT_EQ(expired.size(), 2u);
    EXPECT_EQ(expired[1], 2);
}
