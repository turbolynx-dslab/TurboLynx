#pragma once

#include <sys/socket.h>
#include <cstdint>
#include <netinet/in.h>
#include <iostream>
#include <thread>
#include <vector>
#include <unistd.h>
#include "nlohmann/json.hpp"

using json = nlohmann::json;

#define BUFFER_SIZE 8192
#define PORT 8080

class TurboLynxSocketServer {
public:
    TurboLynxSocketServer(std::string workspace, int port) : workspace(workspace), port(port), server_fd(-1) {}

    ~TurboLynxSocketServer() {
        stop();
    }

    void start();
    void stop();

    static bool ReadLengthPrefixedRequest(int socket, std::string &payload);
    static bool SendAll(int socket, const std::string &payload);

private:
    void handleClient(int socket);
    json processRequest(const json& request);
    json handleQuery(const std::string& query);
    json handleLoad(const std::string& filePath, const std::string& label);

    int port;
    int server_fd;
    int64_t conn_id_ = -1;
    std::string workspace;
};
