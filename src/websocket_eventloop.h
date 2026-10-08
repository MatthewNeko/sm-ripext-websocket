#pragma once
#include "extension.h"
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <thread>
#include <atomic>

class websocket_eventloop
{
public:
    void OnExtLoad();
    void OnExtUnload();
    void run();

    // WebSocket SSL context does not know about the cURL ca-bundle by default.
    // Call this after caBundlePath is resolved (before OnExtLoad) so WebSocket
    // and HTTP share the same CA trust source.
    void load_ca_bundle(const char *path)
    {
        try
        {
            this->ssl_ctx.load_verify_file(path);
        }
        catch (...)
        {
            // File missing or unreadable; fall back to compiled-in OpenSSL paths.
            this->ssl_ctx.set_default_verify_paths();
        }
    }

    boost::asio::io_context &get_context();
    boost::asio::ssl::context &get_ssl_context();

    websocket_eventloop() : work(context), ssl_ctx(boost::asio::ssl::context::tlsv12_client), running(false)
    {
        this->ssl_ctx.set_verify_mode(boost::asio::ssl::verify_peer);
        this->ssl_ctx.set_default_verify_paths();
    }

private:
    boost::asio::io_context context;
    boost::asio::io_context::work work;
    boost::asio::ssl::context ssl_ctx;
    std::thread event_thread;
    std::atomic<bool> running;
};

extern websocket_eventloop event_loop;
