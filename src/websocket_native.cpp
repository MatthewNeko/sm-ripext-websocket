#include "websocket_connection_base.h"
#include "websocket_connection_ssl.h"
#include "websocket_connection.h"
#include "url.hpp"

enum
{
    WebSocket_JSON,
    Websocket_STRING,
};

HandleError websocket_read_handle(Handle_t hndl, IPluginContext *p_context, websocket_connection_base **obj)
{
    HandleSecurity sec;

    sec.pOwner = p_context->GetIdentity();
    sec.pIdentity = myself->GetIdentity();
    HandleError herr;
    if ((herr = handlesys->ReadHandle(hndl, htWebSocket, &sec, reinterpret_cast<void **>(obj))) != HandleError_None)
    {
        p_context->ReportError("Invalid WebSocket handle (error %d)", herr);
        return herr;
    }

    return HandleError_None;
}

static json_t *GetJSONFromHandle(IPluginContext *pContext, Handle_t hndl)
{
    HandleError err;
    HandleSecurity sec(pContext->GetIdentity(), myself->GetIdentity());

    json_t *json;
    if ((err = handlesys->ReadHandle(hndl, htJSON, &sec, (void **)&json)) != HandleError_None)
    {
        pContext->ReportError("Invalid JSON handle %x (error %d)", hndl, err);
        return nullptr;
    }

    return json;
}

static cell_t native_Connect(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    if (websocket_read_handle(params[1], p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    connection->connect();
    return 1;
}

static cell_t native_Close(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    if (websocket_read_handle(params[1], p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    connection->close();
    return 1;
}

static cell_t native_SetReadCallback(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    Handle_t hndl_websocket = params[1];
    if (websocket_read_handle(hndl_websocket, p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    auto callback_type = params[2];
    IPluginFunction *callback = p_context->GetFunctionById((funcid_t)params[3]);
    if (!callback)
    {
        p_context->ReportError("Invalid handler callback provided");
        return 0;
    }

    cell_t data = params[4];

    connection->set_read_callback([connection, callback, hndl_websocket, p_context, data, callback_type](auto buffer, auto size)
                                  {
        std::string message(reinterpret_cast<const char*>(buffer), size);
        free(buffer);

		if (!g_RipExt.Defer([callback, hndl_websocket, message, p_context, data,callback_type]() {
			    callback->PushCell(hndl_websocket);
                if(callback_type == WebSocket_JSON)
                {
                    json_error_t error;
                    json_t *object = json_loads(message.data(), 0, &error);
                    if(!object)
                    {
                        g_RipExt.LogError("WebSocket JSON parse failed: %s", error.text);
                        callback->PushCell(BAD_HANDLE);
                    }
                    else
                    {
			            Handle_t handle = handlesys->CreateHandle(htJSON, object, p_context->GetIdentity(), myself->GetIdentity(), nullptr);
                        if (handle == BAD_HANDLE)
                        {
                            json_decref(object);
                            g_RipExt.LogError("WebSocket JSON handle creation failed.");
                        }
                        callback->PushCell(handle);
                    }
                }
                else if(callback_type == Websocket_STRING)
                {
                    callback->PushString(message.data());
                }
			    callback->PushCell(data);
			    callback->Execute(nullptr);
            }))
        {
            g_RipExt.LogError("WebSocket read callback queue overflow; closing connection.");
            connection->close();
        } });
    return 1;
}

static cell_t native_SetDisconnectCallback(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    Handle_t hndl_websocket = params[1];
    if (websocket_read_handle(hndl_websocket, p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    IPluginFunction *callback = p_context->GetFunctionById((funcid_t)params[2]);
    if (!callback)
    {
        p_context->ReportError("Invalid handler callback provided");
        return 0;
    }

    cell_t data = params[3];

    connection->set_disconnect_callback([callback, hndl_websocket, p_context, data]()
                                        { if (!g_RipExt.Defer([callback, hndl_websocket, p_context, data]()
                                                         {
            callback->PushCell(hndl_websocket);
            callback->PushCell(data);
            callback->Execute(nullptr); }))
        {
            g_RipExt.LogError("WebSocket disconnect callback dropped due to deferred queue overflow.");
        } });

    return 1;
}

static cell_t native_SetConnectCallback(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    Handle_t hndl_websocket = params[1];
    if (websocket_read_handle(hndl_websocket, p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    auto callback = p_context->GetFunctionById((funcid_t)params[2]);
    if (!callback)
    {
        p_context->ReportError("Invalid handler callback provided");
        return 0;
    }

    cell_t data = params[3];

    connection->set_connect_callback([connection, callback, hndl_websocket, p_context, data]()
                                     { if (!g_RipExt.Defer([callback, hndl_websocket, p_context, data]()
                                                      {
            callback->PushCell(hndl_websocket);
            callback->PushCell(data);
            callback->Execute(nullptr); }))
        {
            g_RipExt.LogError("WebSocket connect callback queue overflow; closing connection.");
            connection->close();
        } });

    return 1;
}

static cell_t native_Write(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    if (websocket_read_handle(params[1], p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    json_t *object = GetJSONFromHandle(p_context, params[2]);

    if (object == nullptr)
    {
        return 0;
    }

    char *result;

    result = json_dumps(object, 0);

    std::string payload(result);
    free(result);

    connection->write(std::move(payload));
    return 1;
}

static cell_t native_WriteString(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    if (websocket_read_handle(params[1], p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    char *result;

    p_context->LocalToString(params[2], &result);

    connection->write(std::string(result));
    return 1;
}

static cell_t native_SetHeader(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    if (websocket_read_handle(params[1], p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    char *header, *value;
    p_context->LocalToString(params[2], &header);
    p_context->LocalToString(params[3], &value);
    connection->set_header(std::string(header), std::string(value));
    return 1;
}

static cell_t native_WebSocket(IPluginContext *p_context, const cell_t *params)
{
    char *s_url;
    p_context->LocalToString(params[1], &s_url);
    try
    {
        Url url(s_url);
        // FIX: Initialize pointer to nullptr to avoid uninitialized use
        websocket_connection_base *connection = nullptr;
        if (url.path().empty())
        {
            url.path("/");
        }

        std::string path(url.path());

        for (unsigned int i = 0; i < url.query().size(); i++)
        {
            if (i == 0)
            {
                path.append("?");
            }
            else
            {
                path.append("&");
            }
            Url::KeyVal q = url.query(i);
            path.append(q.key());
            path.append("=");
            path.append(q.val());
        }

        std::string host(url.host());
        if (url.scheme() == "wss")
        {
            if (url.port().empty())
            {
                url.port("443");
            }
            connection = new websocket_connection_ssl(host, path, stoi(url.port()));
        }
        else if (url.scheme() == "ws")
        {
            if (url.port().empty())
            {
                url.port("80");
            }
            connection = new websocket_connection(host, path, stoi(url.port()));
        }
        else
        {
            // FIX: Handle invalid scheme
            p_context->ReportError("Invalid WebSocket URL scheme: %s (must be ws or wss)", url.scheme().c_str());
            return 0;
        }

        // FIX: Double-check connection was created
        if (!connection)
        {
            p_context->ReportError("Failed to create WebSocket connection");
            return 0;
        }
        Handle_t handle = handlesys->CreateHandle(htWebSocket, connection, p_context->GetIdentity(), myself->GetIdentity(), nullptr);
        if (handle == BAD_HANDLE)
        {
            delete connection;
            p_context->ReportError("Failed to create WebSocket handle");
            return 0;
        }

        return handle;
    }
    catch (...)
    {
        p_context->ReportError("Invalid websocket URL: %s", s_url);
        return 0;
    }
}

static cell_t native_SocketOpen(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    if (websocket_read_handle(params[1], p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    return connection->socket_open();
}

static cell_t native_WsOpen(IPluginContext *p_context, const cell_t *params)
{
    websocket_connection_base *connection;
    if (websocket_read_handle(params[1], p_context, &connection) != HandleError_None)
    {
        return 0;
    }

    return connection->ws_open();
}

const sp_nativeinfo_t websocket_natives[] = {
    {"WebSocket.WebSocket", native_WebSocket},
    {"WebSocket.Connect", native_Connect},
    {"WebSocket.SetHeader", native_SetHeader},
    {"WebSocket.Close", native_Close},
    {"WebSocket.SetReadCallback", native_SetReadCallback},
    {"WebSocket.SetDisconnectCallback", native_SetDisconnectCallback},
    {"WebSocket.SetConnectCallback", native_SetConnectCallback},
    {"WebSocket.Write", native_Write},
    {"WebSocket.WriteString", native_WriteString},
    {"WebSocket.SocketOpen", native_SocketOpen},
    {"WebSocket.WsOpen", native_WsOpen},
    {nullptr, nullptr}};
