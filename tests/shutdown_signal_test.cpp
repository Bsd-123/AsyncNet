#include "asyncnet/shutdown_signal.hpp"

#include <csignal>

#include <gtest/gtest.h>

#include "asyncnet/event_loop.hpp"

// raise() delivers the signal to the calling thread synchronously (for a
// single-threaded process, the handler runs and returns before raise()
// itself returns), so by the time these tests call loop.run(), the
// eventfd write has already happened -- no real waiting/races involved.

TEST(ShutdownSignal, SigintTriggersHandlerViaEventLoop) {
    asyncnet::EventLoop loop;
    bool triggered = false;
    asyncnet::ShutdownSignal shutdown(loop, [&]() {
        triggered = true;
        loop.stop();
    });

    raise(SIGINT);

    loop.run(); // eventfd is already readable; must not block
    EXPECT_TRUE(triggered);
}

TEST(ShutdownSignal, SigtermTriggersHandlerViaEventLoop) {
    asyncnet::EventLoop loop;
    bool triggered = false;
    asyncnet::ShutdownSignal shutdown(loop, [&]() {
        triggered = true;
        loop.stop();
    });

    raise(SIGTERM);

    loop.run();
    EXPECT_TRUE(triggered);
}

TEST(ShutdownSignal, DestructorRestoresDefaultDisposition) {
    asyncnet::EventLoop loop;
    bool triggered = false;
    {
        asyncnet::ShutdownSignal shutdown(loop, [&]() { triggered = true; });
    }
    // The handler is gone; this only checks that resetting disposition
    // doesn't crash or throw -- actually verifying default-terminate
    // behavior would require a subprocess, out of scope here.
    struct sigaction current {};
    ASSERT_EQ(sigaction(SIGINT, nullptr, &current), 0);
    EXPECT_EQ(current.sa_handler, SIG_DFL);
    EXPECT_FALSE(triggered);
}
