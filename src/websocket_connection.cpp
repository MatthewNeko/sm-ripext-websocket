#include "websocket_connection.h"
#include "websocket_eventloop.h"
#include <boost/asio/strand.hpp>

websocket_connection::websocket_connection(std::string address, std::string endpoint, uint16_t port) : websocket_connection_base(address, endpoint, port)
{
    this->ws = std::make_unique<websocket::stream<beast::tcp_stream>>(boost::asio::make_strand(event_loop.get_context()));
    this->work = std::make_unique<boost::asio::io_context::work>(event_loop.get_context());
    this->resolver = std::make_shared<tcp::resolver>(event_loop.get_context());
}

void websocket_connection::connect()
{
    bool resolve_started = false;

    try
    {
        // Check if already being destroyed
        if (this->pending_delete.load(std::memory_order_acquire))
        {
            g_RipExt.LogError("WebSocket connect called on destroyed connection");
            return;
        }

        this->disconnect_notified.store(false, std::memory_order_release);
        this->ws_connect.store(false, std::memory_order_release);
        this->close_in_progress.store(false, std::memory_order_release);

        char s_port[8];
        std::snprintf(s_port, sizeof(s_port), "%hu", this->port);
        tcp::resolver::query query(this->address.c_str(), s_port);

        this->begin_async();
        resolve_started = true;
        this->resolver->async_resolve(query, beast::bind_front_handler(&websocket_connection::on_resolve, this));
        g_RipExt.LogMessage("Init Connect %s:%d", address.c_str(), this->port);
    }
    catch (const std::exception &ex)
    {
        if (resolve_started)
        {
            this->end_async();
        }
        g_RipExt.LogError("WebSocket connect setup error for %s:%d: %s", this->address.c_str(), this->port, ex.what());
        this->notify_disconnect();
    }
}

void websocket_connection::on_resolve(beast::error_code ec, tcp::resolver::results_type results)
{
    const auto guard = std::unique_ptr<void, std::function<void(void *)>>(nullptr, [this](void *)
                                                                          { this->end_async(); });

    // Check if connection is being destroyed
    if (this->pending_delete.load(std::memory_order_acquire))
    {
        return;
    }

    if (ec)
    {
        g_RipExt.LogError("Error resolving %s: %d %s", this->address.c_str(), ec.value(), ec.message().c_str());
        this->notify_disconnect();
        this->ws_connect.store(false, std::memory_order_release);
        return;
    }

    beast::get_lowest_layer(*this->ws).expires_after(std::chrono::seconds(30));
    this->begin_async();
    beast::get_lowest_layer(*this->ws).async_connect(results, beast::bind_front_handler(&websocket_connection::on_connect, this));
}

void websocket_connection::on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type ep)
{
    const auto guard = std::unique_ptr<void, std::function<void(void *)>>(nullptr, [this](void *)
                                                                          { this->end_async(); });

    if (this->pending_delete.load(std::memory_order_acquire))
    {
        return;
    }

    if (ec)
    {
        g_RipExt.LogError("Error connecting to %s: %d %s", this->address.c_str(), ec.value(), ec.message().c_str());
        this->notify_disconnect();
        this->ws_connect.store(false, std::memory_order_release);
        return;
    }
    beast::get_lowest_layer(*this->ws).expires_never();

    auto timeout = websocket::stream_base::timeout::suggested(beast::role_type::client);
    timeout.keep_alive_pings = true;
    this->ws->set_option(websocket::stream_base::decorator([this](websocket::request_type &req)
                                                           { this->add_headers(req); }));

    this->begin_async();
    this->ws->async_handshake(this->address, this->endpoint.c_str(), beast::bind_front_handler(&websocket_connection::on_handshake, this));
}

void websocket_connection::on_handshake(beast::error_code ec)
{
    const auto guard = std::unique_ptr<void, std::function<void(void *)>>(nullptr, [this](void *)
                                                                          { this->end_async(); });

    if (this->pending_delete.load(std::memory_order_acquire))
    {
        return;
    }

    if (ec)
    {
        g_RipExt.LogError("WebSocket Handshake Error: %d %s", ec.value(), ec.message().c_str());
        this->notify_disconnect();
        this->ws_connect.store(false, std::memory_order_release);
        return;
    }

    this->buffer.clear();
    if (this->connect_callback)
    {
        this->connect_callback->operator()();
    }

    this->begin_async();
    this->ws->async_read(this->buffer, beast::bind_front_handler(&websocket_connection::on_read, this));
    this->ws_connect.store(true, std::memory_order_release);
    g_RipExt.LogMessage("On Handshaked %s:%d", address.c_str(), this->port);
}

void websocket_connection::on_write(beast::error_code ec, size_t bytes_transferred)
{
    const auto guard = std::unique_ptr<void, std::function<void(void *)>>(nullptr, [this](void *)
                                                                          { this->end_async(); });

    if (this->pending_delete.load(std::memory_order_acquire))
    {
        return;
    }

    if (ec)
    {
        g_RipExt.LogError("WebSocket write error: %d %s", ec.value(), ec.message().c_str());
        // FIX: Notify disconnect on write error to prevent state inconsistency
        this->notify_disconnect();
        this->ws_connect.store(false, std::memory_order_release);
        return;
    }
}

void websocket_connection::on_read(beast::error_code ec, size_t bytes_transferred)
{
    const auto guard = std::unique_ptr<void, std::function<void(void *)>>(nullptr, [this](void *)
                                                                          { this->end_async(); });

    if (this->pending_delete.load(std::memory_order_acquire))
    {
        return;
    }

    if (ec)
    {
        if (!this->pending_delete.load(std::memory_order_acquire) &&
            ec != boost::asio::error::operation_aborted &&
            ec.value() != 995 &&
            ec.value() != 1236)
        {
            g_RipExt.LogError("WebSocket read error: %d %s", ec.value(), ec.message().c_str());
        }
        this->notify_disconnect();
        this->ws_connect.store(false, std::memory_order_release);
        return;
    }

    if (this->read_callback)
    {
        // FIX: Check malloc return value
        auto buffer = reinterpret_cast<uint8_t *>(malloc(bytes_transferred));
        if (!buffer)
        {
            g_RipExt.LogError("WebSocket read: failed to allocate buffer of size %zu", bytes_transferred);
            this->notify_disconnect();
            this->ws_connect.store(false, std::memory_order_release);
            return;
        }
        memcpy(buffer, reinterpret_cast<const uint8_t *>(this->buffer.data().data()), bytes_transferred);

        this->read_callback->operator()(buffer, bytes_transferred);
    }
    this->buffer.consume(bytes_transferred);

    this->begin_async();
    this->ws->async_read(this->buffer, beast::bind_front_handler(&websocket_connection::on_read, this));
}

void websocket_connection::on_close(beast::error_code ec)
{
    const auto guard = std::unique_ptr<void, std::function<void(void *)>>(nullptr, [this](void *)
                                                                          { this->end_async(); });

    if (ec)
    {
        if (!this->pending_delete.load(std::memory_order_acquire) &&
            ec != boost::asio::error::operation_aborted &&
            ec.value() != 995 &&
            ec.value() != 1236)
        {
            g_RipExt.LogError("WebSocket close error: %d %s", ec.value(), ec.message().c_str());
        }
    }
    this->ws_connect.store(false, std::memory_order_release);
    this->close_in_progress.store(false, std::memory_order_release);
}

void websocket_connection::write(std::string message)
{
    try
    {
        // FIX: Check connection state before writing
        if (this->pending_delete.load(std::memory_order_acquire))
        {
            g_RipExt.LogError("WebSocket write: connection is being destroyed");
            return;
        }
        if (!this->ws_connect.load(std::memory_order_acquire))
        {
            g_RipExt.LogError("WebSocket write: connection not established");
            return;
        }

        auto payload = std::make_shared<std::string>(std::move(message));
        this->begin_async();
        this->ws->async_write(boost::asio::buffer(*payload), [this, payload](beast::error_code ec, size_t bytes_transferred)
                              { this->on_write(ec, bytes_transferred); });
    }
    catch (const std::exception &ex)
    {
        this->end_async();
        g_RipExt.LogError("WebSocket write setup error for %s:%d: %s", this->address.c_str(), this->port, ex.what());
    }
}

void websocket_connection::close()
{
    this->cancel();
}

bool websocket_connection::socket_open()
{
    return this->ws->is_open();
}

void websocket_connection::cancel()
{
    try
    {
        beast::error_code ec;
        this->resolver->cancel();

        auto &stream = beast::get_lowest_layer(*this->ws);

        // FIX: Use close_in_progress to prevent double-close race
        if (this->close_in_progress.exchange(true, std::memory_order_acq_rel))
        {
            // Close already in progress, just cancel remaining operations
            stream.cancel();
            return;
        }

        // Cancel all pending async operations first
        stream.cancel();

        if (this->pending_delete.load(std::memory_order_acquire))
        {
            if (stream.socket().is_open())
            {
                stream.socket().shutdown(tcp::socket::shutdown_both, ec);
                stream.socket().close(ec);
            }
            this->ws_connect.store(false, std::memory_order_release);
            this->close_in_progress.store(false, std::memory_order_release);
            return;
        }

        if (this->ws->is_open())
        {
            this->begin_async();
            this->ws->async_close(websocket::close_code::normal, beast::bind_front_handler(&websocket_connection::on_close, this));
            return;
        }

        if (stream.socket().is_open())
        {
            stream.socket().shutdown(tcp::socket::shutdown_both, ec);
            stream.socket().close(ec);
        }
    }
    catch (const std::exception &ex)
    {
        g_RipExt.LogError("WebSocket cancel error for %s:%d: %s", this->address.c_str(), this->port, ex.what());
    }
}
