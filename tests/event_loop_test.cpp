#include "asyncnet/event_loop.hpp"

#include <unistd.h>

#include <gtest/gtest.h>

#include "support/socket_pair.hpp"

using asyncnet::test::SocketPair;

TEST(EventLoop, StopBeforeRunMakesRunReturnImmediately) {
    asyncnet::EventLoop loop;
    loop.stop();
    // Nothing is registered, so if run() called poll(-1) even once before
    // checking the stop flag, this would block forever.
    loop.run();
    SUCCEED();
}

TEST(EventLoop, StopFromCallbackExitsCleanly) {
    asyncnet::EventLoop loop;
    SocketPair sp;

    int callCount = 0;
    loop.reactor().registerFd(sp.readEnd, asyncnet::IOEvent::Readable,
                               [&](int, asyncnet::IOEvent) {
                                   ++callCount;
                                   loop.stop();
                               });

    const char byte = 'x';
    ASSERT_EQ(write(sp.writeEnd, &byte, 1), 1);

    // The fd is already readable, so poll() inside run() cannot block; if
    // stop() didn't take effect after the current dispatch pass, this call
    // would hang forever instead of returning.
    loop.run();

    EXPECT_EQ(callCount, 1);
}

TEST(EventLoop, DispatchesMultipleReadyHandlersBeforeStopping) {
    asyncnet::EventLoop loop;
    SocketPair spA;
    SocketPair spB;

    int calls = 0;
    auto handler = [&](int, asyncnet::IOEvent) {
        ++calls;
        if (calls == 2) {
            loop.stop();
        }
    };

    loop.reactor().registerFd(spA.readEnd, asyncnet::IOEvent::Readable, handler);
    loop.reactor().registerFd(spB.readEnd, asyncnet::IOEvent::Readable, handler);

    const char byte = 'x';
    ASSERT_EQ(write(spA.writeEnd, &byte, 1), 1);
    ASSERT_EQ(write(spB.writeEnd, &byte, 1), 1);

    loop.run();

    EXPECT_EQ(calls, 2);
}
