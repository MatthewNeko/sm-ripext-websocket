#include "websocket_connection_base.h"
#include <condition_variable>
#include <set>

namespace
{
std::mutex g_connection_mutex;
std::set<websocket_connection_base *> g_connections;
std::condition_variable g_connection_condition;
}

websocket_connection_base::websocket_connection_base(std::string address, std::string endpoint, uint16_t port)
    : address(std::move(address)), endpoint(std::move(endpoint)), port(port)
{
    std::lock_guard<std::mutex> guard(g_connection_mutex);
    g_connections.insert(this);
}

websocket_connection_base::~websocket_connection_base()
{
    {
        std::lock_guard<std::mutex> guard(g_connection_mutex);
        g_connections.erase(this);
    }
    g_connection_condition.notify_all();
}

void websocket_connection_base::set_write_callback(std::function<void(size_t)> callback)
{
    this->write_callback = std::make_unique<std::function<void(size_t)>>(callback);
}

void websocket_connection_base::set_read_callback(std::function<void(uint8_t *, size_t)> callback)
{
    this->read_callback = std::make_unique<std::function<void(uint8_t *, size_t)>>(callback);
}

void websocket_connection_base::set_connect_callback(std::function<void()> callback)
{
    this->connect_callback = std::make_unique<std::function<void()>>(callback);
}

void websocket_connection_base::set_disconnect_callback(std::function<void()> callback)
{
    this->disconnect_callback = std::make_unique<std::function<void()>>(callback);
}

void websocket_connection_base::set_header(std::string header, std::string value)
{
    std::lock_guard<std::mutex> guard(this->header_mutex);
    this->headers.insert_or_assign(header, value);
}

void websocket_connection_base::add_headers(websocket::request_type &req)
{
    req.set(beast::http::field::user_agent, std::string(BOOST_BEAST_VERSION_STRING) + " SourceMod-WebSockets v" + SMEXT_CONF_VERSION);
    std::lock_guard<std::mutex> guard(this->header_mutex);
    for (std::pair<std::string, std::string> elem : this->headers)
    {
        req.set(elem.first, elem.second);
    }
}

void websocket_connection_base::destroy()
{
    this->pending_delete.store(true, std::memory_order_release);
    this->cancel();
    this->maybe_delete();
}

void websocket_connection_base::wait_for_all_destroyed()
{
    std::unique_lock<std::mutex> lock(g_connection_mutex);
    g_connection_condition.wait(lock, []()
                                { return g_connections.empty(); });
}

bool websocket_connection_base::ws_open()
{
    return this->ws_connect.load(std::memory_order_acquire) && !this->deleted.load(std::memory_order_acquire);
}

void websocket_connection_base::begin_async()
{
    this->async_ops.fetch_add(1, std::memory_order_acq_rel);
}

void websocket_connection_base::end_async()
{
    if (this->async_ops.fetch_sub(1, std::memory_order_acq_rel) == 1)
    {
        this->maybe_delete();
    }
}

void websocket_connection_base::notify_disconnect()
{
    if (!this->disconnect_callback || this->disconnect_notified.exchange(true))
    {
        return;
    }

    this->disconnect_callback->operator()();
}

void websocket_connection_base::maybe_delete()
{
    // Use compare_exchange to ensure only one thread ever executes delete this
    // This prevents the double-free race condition between destroy() and end_async()
    if (this->pending_delete.load(std::memory_order_acquire) &&
        this->async_ops.load(std::memory_order_acquire) == 0)
    {
        // Atomically mark as deleted - only one thread will succeed
        bool expected = false;
        if (this->deleted.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        {
            delete this;
        }
    }
}
