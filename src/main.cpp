#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>

#include "asyncnet/echo_server.hpp"
#include "asyncnet/logger.hpp"

namespace {
constexpr std::uint16_t kDefaultPort = 9000;
constexpr std::chrono::milliseconds kDefaultIdleTimeout{60000};
}

// Plain TCP echo server: no parsing yet (M2), no graceful shutdown yet
// (a later M1 step) -- just the Acceptor/Connection/idle-timeout pipeline
// wired together end to end.
int main(int argc, char** argv) {
    const std::uint16_t port =
        argc > 1 ? static_cast<std::uint16_t>(std::stoi(argv[1])) : kDefaultPort;

    asyncnet::EchoServer server(port, std::optional(kDefaultIdleTimeout));

    ASYNCNET_LOG_INFO("AsyncNet echo server listening on port " +
                       std::to_string(server.boundPort()));

    server.run();

    return 0;
}
