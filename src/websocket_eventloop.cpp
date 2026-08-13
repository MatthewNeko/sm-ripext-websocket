#include "websocket_eventloop.h"
#include "websocket_connection_base.h"

websocket_eventloop event_loop;

void websocket_eventloop::OnExtLoad()
{
    // FIX: Store thread handle for proper join on unload
    running.store(true);
    event_thread = std::thread(&websocket_eventloop::run, this);
}

void websocket_eventloop::OnExtUnload()
{
    // Handles are removed before this method. Wait for cancelled completion
    // handlers while the event loop is still running.
    websocket_connection_base::wait_for_all_destroyed();
    running.store(false);
    this->context.stop();
    
    if (event_thread.joinable())
    {
        event_thread.join();
    }
    
    this->context.reset();
}

void websocket_eventloop::run()
{
    while (running.load())
    {
        try
        {
            // Run the io_context until stopped
            this->context.run();
            
            // If stopped but still running, restart
            if (running.load())
            {
                this->context.restart();
            }
        }
        catch (const std::exception &ex)
        {
            g_RipExt.LogError("WebSocket event loop crashed: %s", ex.what());
        }
        catch (...)
        {
            g_RipExt.LogError("WebSocket event loop crashed with unknown exception");
        }
    }
}

boost::asio::io_context &websocket_eventloop::get_context()
{
    return this->context;
}

boost::asio::ssl::context &websocket_eventloop::get_ssl_context()
{
    return this->ssl_ctx;
}
