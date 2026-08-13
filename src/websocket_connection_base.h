#pragma once
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio.hpp>
#include <memory>
#include "extension.h"
#include <map>
#include <atomic>
#include <mutex>

#if defined WIN32
#include <sdkddkver.h>
#endif

namespace websocket = boost::beast::websocket;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;

class websocket_connection_base
{
public:
    websocket_connection_base(std::string address, std::string endpoint, uint16_t port);
    virtual ~websocket_connection_base();
    void set_write_callback(std::function<void(std::size_t)> callback);
    void set_read_callback(std::function<void(uint8_t *, std::size_t)> callback);
    void set_connect_callback(std::function<void()> callback);
    void set_disconnect_callback(std::function<void()> callback);
    void set_header(std::string key, std::string value);
    void add_headers(websocket::request_type &req);
    void destroy();
    static void wait_for_all_destroyed();
    bool ws_open();

    virtual void close() = 0;
    virtual void connect() = 0;
    virtual void write(std::string message) = 0;
    virtual bool socket_open() = 0;
    virtual void cancel() = 0;

protected:
    void begin_async();
    void end_async();
    void notify_disconnect();
    void maybe_delete();

    std::unique_ptr<std::function<void(uint8_t *, std::size_t)>> read_callback;
    std::unique_ptr<std::function<void(std::size_t)>> write_callback;
    std::unique_ptr<std::function<void()>> connect_callback;
    std::unique_ptr<std::function<void()>> disconnect_callback;
    std::map<std::string, std::string> headers;
    std::mutex header_mutex;
    beast::flat_buffer buffer;
    std::string address;
    std::string endpoint;
    uint16_t port;
    std::atomic<bool> pending_delete{false};
    std::atomic<bool> ws_connect{false};
    std::atomic<bool> disconnect_notified{false};
    std::atomic<uint32_t> async_ops{0};
    std::atomic<bool> deleted{false};
    std::atomic<bool> close_in_progress{false};
};
