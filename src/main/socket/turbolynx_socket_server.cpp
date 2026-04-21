#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>
#include "main/socket/turbolynx_socket_server.hpp"
#include "main/capi/turbolynx.h"
#include "common/logger.hpp"

namespace {

constexpr uint32_t MAX_SOCKET_REQUEST_BYTES = 64 * 1024 * 1024;

bool ContainsNulByte(const std::string &value) {
    return value.find('\0') != std::string::npos;
}

bool ReadExact(int socket, void *buffer, size_t bytes_to_read) {
    auto *ptr = static_cast<char *>(buffer);
    size_t total_read = 0;
    while (total_read < bytes_to_read) {
        ssize_t bytes_read = ::read(socket, ptr + total_read, bytes_to_read - total_read);
        if (bytes_read == 0) {
            return false;
        }
        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        total_read += static_cast<size_t>(bytes_read);
    }
    return true;
}

int RunArgvProcess(const std::vector<std::string> &args) {
    if (args.empty()) {
        throw std::invalid_argument("Cannot execute an empty command");
    }

    std::vector<char *> argv;
    argv.reserve(args.size() + 1);
    for (const auto &arg : args) {
        if (ContainsNulByte(arg)) {
            throw std::invalid_argument("Command argument contains NUL byte");
        }
        argv.push_back(const_cast<char *>(arg.c_str()));
    }
    argv.push_back(nullptr);

    pid_t pid = ::fork();
    if (pid < 0) {
        throw std::runtime_error(std::string("fork failed: ") + std::strerror(errno));
    }
    if (pid == 0) {
        ::execv(argv[0], argv.data());
        _exit(127);
    }

    int status = 0;
    while (::waitpid(pid, &status, 0) < 0) {
        if (errno == EINTR) {
            continue;
        }
        throw std::runtime_error(std::string("waitpid failed: ") + std::strerror(errno));
    }

    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
        return 128 + WTERMSIG(status);
    }
    return 1;
}

} // namespace

bool TurboLynxSocketServer::ReadLengthPrefixedRequest(int socket, std::string &payload) {
    uint32_t frame_size_network = 0;
    if (!ReadExact(socket, &frame_size_network, sizeof(frame_size_network))) {
        return false;
    }

    uint32_t frame_size = ntohl(frame_size_network);
    if (frame_size > MAX_SOCKET_REQUEST_BYTES) {
        throw std::runtime_error("Socket request exceeds maximum frame size");
    }

    payload.resize(frame_size);
    if (frame_size == 0) {
        return true;
    }
    return ReadExact(socket, payload.data(), frame_size);
}

bool TurboLynxSocketServer::SendAll(int socket, const std::string &payload) {
    size_t total_sent = 0;
    while (total_sent < payload.size()) {
        ssize_t bytes_sent = ::send(socket, payload.data() + total_sent,
                                    payload.size() - total_sent, 0);
        if (bytes_sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        total_sent += static_cast<size_t>(bytes_sent);
    }
    return true;
}

void TurboLynxSocketServer::start() {
    conn_id_ = turbolynx_connect(workspace.c_str());
    if (conn_id_ < 0) {
        spdlog::error("[start] Failed to connect to workspace: {}", workspace);
        exit(EXIT_FAILURE);
    }

    sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);

    if ((server_fd = ::socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        spdlog::error("[start] Socket creation failed");
        exit(EXIT_FAILURE);
    }

    if (::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        spdlog::error("[start] Set socket options failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (::bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        spdlog::error("[start] Socket bind failed");
        exit(EXIT_FAILURE);
    }

    if (::listen(server_fd, 10) < 0) {
        spdlog::error("[start] Socket listen failed");
        exit(EXIT_FAILURE);
    }

    spdlog::info("[start] TurboLynx Database Server listening on port {}", port);

    while (true) {
        int new_socket = ::accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen);
        if (new_socket < 0) {
            perror("[start] Socket accept failed");
            continue;
        }
        handleClient(new_socket);
    }
}

void TurboLynxSocketServer::stop() {
    if (server_fd != -1) {
        ::close(server_fd);
    }
    turbolynx_disconnect(conn_id_);
}

void TurboLynxSocketServer::handleClient(int socket) {
    std::string payload;
    if (!ReadLengthPrefixedRequest(socket, payload)) {
        ::close(socket);
        return;
    }

    try {
        json request = json::parse(payload);
        json response = processRequest(request);
        std::string responseStr = response.dump();

        SendAll(socket, responseStr);
    } catch (const std::exception& e) {
        json errorResponse = {{"status", "error"}, {"error", e.what()}};
        std::string errorStr = errorResponse.dump();
        SendAll(socket, errorStr);
    }

    ::close(socket);
}

json TurboLynxSocketServer::processRequest(const json& request) {
    json response;

    try {
        if (request.contains("query")) {
            std::string query = request.at("query");
            response = handleQuery(query);
        } 
        else if (request.contains("load") && request.contains("label")) {
            std::string filePath = request.at("load");
            std::string label = request.at("label");
            response = handleLoad(filePath, label);
        } 
        else {
            response = {{"status", "error"}, {"error", "Invalid request: Missing required fields"}};
        }
    } catch (const std::exception& e) {
        response = {{"status", "error"}, {"error", e.what()}};
    }

    return response;
}

json TurboLynxSocketServer::handleQuery(const std::string& query) {
    spdlog::info("[handleQuery] Query: {}", query);

    turbolynx_prepared_statement* prep_stmt = turbolynx_prepare(conn_id_, const_cast<char*>(query.c_str()));
    if (prep_stmt == nullptr) {
        spdlog::error("Failed to prepare query: {}", query);
        json response;
        response["status"] = "error";
        response["error"] = "Failed to prepare query";
        return response;
    }

    json response;
    response["status"] = "success";
    response["error"] = nullptr;

    std::vector<std::string> columns;
    std::vector<turbolynx_type> types;
    turbolynx_property* property = prep_stmt->property;
    while (property) {
        columns.push_back(property->property_name);
        types.push_back(property->property_type);
        property = property->next;
    }
    spdlog::info("[handleQuery] Columns: {}", join_vector(columns));
    response["columns"] = columns;

    turbolynx_resultset_wrapper* result_set_wrp;
    turbolynx_num_rows rows = turbolynx_execute(conn_id_, prep_stmt, &result_set_wrp);

    turbolynx_num_properties num_columns = result_set_wrp->result_set->num_properties;
    turbolynx_result* result = result_set_wrp->result_set->result;

    json data;
    while (turbolynx_fetch_next(result_set_wrp) != TURBOLYNX_END_OF_RESULT) {
        json row;
        for (idx_t i = 0; i < num_columns; ++i) {
            switch (types[i]) {
                case TURBOLYNX_TYPE_BOOLEAN:
                    row.push_back(turbolynx_get_bool(result_set_wrp, i));
                    break;
                case TURBOLYNX_TYPE_INTEGER:
                    row.push_back(turbolynx_get_int32(result_set_wrp, i));
                    break;
                case TURBOLYNX_TYPE_BIGINT:
                    row.push_back(turbolynx_get_int64(result_set_wrp, i));
                    break;
                case TURBOLYNX_TYPE_UINTEGER:
                    row.push_back(turbolynx_get_uint32(result_set_wrp, i));
                    break;
                case TURBOLYNX_TYPE_UBIGINT:
                    row.push_back(turbolynx_get_uint64(result_set_wrp, i));
                    break;
                case TURBOLYNX_TYPE_FLOAT:
                    row.push_back(turbolynx_get_float(result_set_wrp, i));
                    break;
                case TURBOLYNX_TYPE_DOUBLE:
                    row.push_back(turbolynx_get_double(result_set_wrp, i));
                    break;
                case TURBOLYNX_TYPE_VARCHAR:
                    row.push_back(std::string(turbolynx_get_varchar(result_set_wrp, i).data));
                    break;
                case TURBOLYNX_TYPE_DECIMAL:
                    row.push_back(std::string(turbolynx_decimal_to_string(turbolynx_get_decimal(result_set_wrp, i)).data));
                    break;
                case TURBOLYNX_TYPE_ID:
                    row.push_back(turbolynx_get_id(result_set_wrp, i));
                    break;
                default:
                    row.push_back(nullptr);
                    break;
            }
        }
        data.push_back(row);
    }
    response["data"] = data;

    turbolynx_close_resultset(result_set_wrp);
    turbolynx_close_prepared_statement(prep_stmt);
    return response;
}

json TurboLynxSocketServer::handleLoad(const std::string& filePath, const std::string& label) {
    json response;

    try {
        std::vector<std::string> args = {
            "./bulkload",
            "--log-level", "debug",
            "--incremental", "true",
            "--skip-histogram", "true",
            "--output_dir", workspace,
            "--relationships", label,
            filePath,
        };

        int ret_code = RunArgvProcess(args);

        if (ret_code == 0) {
            response["status"] = "success";
            response["message"] = "File loaded successfully";
        } else {
            response["status"] = "error";
            response["error"] = "Bulk load failed with exit code: " + std::to_string(ret_code);
        }
    } catch (const std::exception& e) {
        response["status"] = "error";
        response["error"] = e.what();
    }

    return response;
}
