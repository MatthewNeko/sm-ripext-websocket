/**
 * vim: set ts=4 :
 * =============================================================================
 * SourceMod REST in Pawn Extension
 * Copyright 2017-2022 Erik Minekus
 * =============================================================================
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "extension.h"
#include "httprequest.h"
#include "queue.h"
#include "websocket_connection_base.h"
#include "websocket_eventloop.h"
#include <atomic>
#include <mutex>
#include <set>

// SourcePawn and SourceMod handles/forwards must be touched from the game thread.
// Keep all per-frame work bounded so network callbacks cannot stall the server tick.
#define MAX_PROCESS 10
#define MAX_COMPLETED_PROCESS 4
#define MAX_DEFERRED_CALLBACKS_PER_FRAME 64
#define MAX_DEFERRED_CALLBACKS_QUEUED 4096

RipExt g_RipExt; /**< Global singleton for extension's main interface */

SMEXT_LINK(&g_RipExt);

LockedQueue<IHTTPContext *> g_RequestQueue;
LockedQueue<IHTTPContext *> g_CompletedRequestQueue;
LockedQueue<IHTTPContext *> g_DeleteRequestQueue;
LockedQueue<std::function<void()> *> g_DeferredCallbackQueue;

std::mutex g_ActiveRequestMutex;
std::set<IHTTPContext *> g_ActiveRequestContexts;
std::mutex g_CurlContextMutex;
std::set<CurlContext *> g_CurlContexts;

std::atomic<unsigned int> g_DeferredCallbackCount{0};
std::atomic<unsigned int> g_DroppedDeferredCallbacks{0};

CURLM *g_Curl;
uv_loop_t *g_Loop;
uv_thread_t g_Thread;
uv_timer_t g_Timeout;

uv_async_t g_AsyncPerformRequests;
uv_async_t g_AsyncStopLoop;

HTTPRequestHandler g_HTTPRequestHandler;
HandleType_t htHTTPRequest;

HTTPResponseHandler g_HTTPResponseHandler;
HandleType_t htHTTPResponse;

JSONHandler g_JSONHandler;
HandleType_t htJSON;

JSONObjectKeysHandler g_JSONObjectKeysHandler;
HandleType_t htJSONObjectKeys;

WebSocketHandler g_WebSocketHandler;
HandleType_t htWebSocket;

std::atomic<bool> unloaded;

static void TrackActiveRequest(IHTTPContext *context)
{
	std::lock_guard<std::mutex> guard(g_ActiveRequestMutex);
	g_ActiveRequestContexts.insert(context);
}

static void UntrackActiveRequest(IHTTPContext *context)
{
	std::lock_guard<std::mutex> guard(g_ActiveRequestMutex);
	g_ActiveRequestContexts.erase(context);
}

static void TrackCurlContext(CurlContext *context)
{
	std::lock_guard<std::mutex> guard(g_CurlContextMutex);
	g_CurlContexts.insert(context);
}

static void UntrackCurlContext(CurlContext *context)
{
	std::lock_guard<std::mutex> guard(g_CurlContextMutex);
	g_CurlContexts.erase(context);
}

static void CheckCompletedRequests()
{
	CURLMsg *message;
	int pending;

	while ((message = curl_multi_info_read(g_Curl, &pending)))
	{
		if (message->msg != CURLMSG_DONE)
		{
			continue;
		}

		CURL *curl = message->easy_handle;
		IHTTPContext *context = nullptr;
		curl_easy_getinfo(curl, CURLINFO_PRIVATE, &context);
		curl_multi_remove_handle(g_Curl, curl);

		if (context == nullptr)
		{
			continue;
		}
		UntrackActiveRequest(context);

		g_CompletedRequestQueue.Lock();
		g_CompletedRequestQueue.Push(context);
		g_CompletedRequestQueue.Unlock();
	}
}

static bool ReserveDeferredCallbackSlot()
{
	unsigned int current = g_DeferredCallbackCount.load(std::memory_order_relaxed);
	while (current < MAX_DEFERRED_CALLBACKS_QUEUED)
	{
		if (g_DeferredCallbackCount.compare_exchange_weak(current, current + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
		{
			return true;
		}
	}

	return false;
}

static void RunDeferredCallbacks()
{
	unsigned int dropped = g_DroppedDeferredCallbacks.exchange(0, std::memory_order_acq_rel);
	if (dropped != 0 && !unloaded.load())
	{
		smutils->LogError(myself, "Dropped %u deferred callbacks to protect the server frame time.", dropped);
	}

	int count = 0;
	while (count < MAX_DEFERRED_CALLBACKS_PER_FRAME)
	{
		g_DeferredCallbackQueue.Lock();
		if (g_DeferredCallbackQueue.Empty())
		{
			g_DeferredCallbackQueue.Unlock();
			break;
		}

		std::unique_ptr<std::function<void()>> callback(g_DeferredCallbackQueue.Pop());
		g_DeferredCallbackCount.fetch_sub(1, std::memory_order_acq_rel);
		g_DeferredCallbackQueue.Unlock();

		if (!unloaded.load())
		{
			callback->operator()();
		}

		count++;
	}
}

static void DeleteFailedRequests()
{
	int count = 0;
	while (count < MAX_COMPLETED_PROCESS)
	{
		g_DeleteRequestQueue.Lock();
		if (g_DeleteRequestQueue.Empty())
		{
			g_DeleteRequestQueue.Unlock();
			break;
		}

		IHTTPContext *context = g_DeleteRequestQueue.Pop();
		g_DeleteRequestQueue.Unlock();

		if (context->HasPendingCallbacks())
		{
			g_DeleteRequestQueue.Lock();
			g_DeleteRequestQueue.Push(context);
			g_DeleteRequestQueue.Unlock();
			break;
		}

		delete context;
		count++;
	}
}

static void RunCompletedRequests()
{
	int count = 0;
	while (count < MAX_COMPLETED_PROCESS)
	{
		g_CompletedRequestQueue.Lock();
		if (g_CompletedRequestQueue.Empty())
		{
			g_CompletedRequestQueue.Unlock();
			break;
		}

		IHTTPContext *context = g_CompletedRequestQueue.Pop();
		g_CompletedRequestQueue.Unlock();

		context->OnCompleted();

		if (context->HasPendingCallbacks())
		{
			g_DeleteRequestQueue.Lock();
			g_DeleteRequestQueue.Push(context);
			g_DeleteRequestQueue.Unlock();
		}
		else
		{
			delete context;
		}
		count++;
	}
}

static void DrainDeferredCallbacks()
{
	while (true)
	{
		g_DeferredCallbackQueue.Lock();
		if (g_DeferredCallbackQueue.Empty())
		{
			g_DeferredCallbackQueue.Unlock();
			break;
		}

		std::unique_ptr<std::function<void()>> callback(g_DeferredCallbackQueue.Pop());
		g_DeferredCallbackCount.fetch_sub(1, std::memory_order_acq_rel);
		g_DeferredCallbackQueue.Unlock();
	}
}

static void DrainRequestQueue(LockedQueue<IHTTPContext *> &queue)
{
	while (true)
	{
		queue.Lock();
		if (queue.Empty())
		{
			queue.Unlock();
			break;
		}

		IHTTPContext *context = queue.Pop();
		queue.Unlock();
		delete context;
	}
}

static void CleanupActiveRequests()
{
	while (true)
	{
		g_ActiveRequestMutex.lock();
		if (g_ActiveRequestContexts.empty())
		{
			g_ActiveRequestMutex.unlock();
			break;
		}

		IHTTPContext *context = *g_ActiveRequestContexts.begin();
		g_ActiveRequestContexts.erase(g_ActiveRequestContexts.begin());
		g_ActiveRequestMutex.unlock();

		if (context->curl)
		{
			curl_multi_remove_handle(g_Curl, context->curl);
		}
		delete context;
	}
}

static void CleanupCurlContexts()
{
	while (true)
	{
		g_CurlContextMutex.lock();
		if (g_CurlContexts.empty())
		{
			g_CurlContextMutex.unlock();
			break;
		}

		CurlContext *context = *g_CurlContexts.begin();
		g_CurlContexts.erase(g_CurlContexts.begin());
		g_CurlContextMutex.unlock();

		context->Destroy();
	}
}

static void CloseLibuvHandle(uv_handle_t *handle)
{
	if (!uv_is_closing(handle))
	{
		uv_close(handle, nullptr);
	}
}

static void CloseLibuvHandles()
{
	uv_timer_stop(&g_Timeout);
	CloseLibuvHandle(reinterpret_cast<uv_handle_t *>(&g_Timeout));
	CloseLibuvHandle(reinterpret_cast<uv_handle_t *>(&g_AsyncPerformRequests));
	CloseLibuvHandle(reinterpret_cast<uv_handle_t *>(&g_AsyncStopLoop));
	uv_run(g_Loop, UV_RUN_DEFAULT);
}

static void PerformRequests(uv_timer_t *handle)
{
	int running;
	curl_multi_socket_action(g_Curl, CURL_SOCKET_TIMEOUT, 0, &running);

	CheckCompletedRequests();
}

static void CurlSocketActivity(uv_poll_t *handle, int status, int events)
{
	CurlContext *context = (CurlContext *)handle->data;
	int flags = 0;

	if (events & UV_READABLE)
	{
		flags |= CURL_CSELECT_IN;
	}
	if (events & UV_WRITABLE)
	{
		flags |= CURL_CSELECT_OUT;
	}

	int running;
	curl_multi_socket_action(g_Curl, context->socket, flags, &running);

	CheckCompletedRequests();
}

static int CurlSocketCallback(CURL *curl, curl_socket_t socket, int action, void *userdata, void *socketdata)
{
	CurlContext *context;
	int events = 0;

	switch (action)
	{
	case CURL_POLL_IN:
	case CURL_POLL_OUT:
	case CURL_POLL_INOUT:
		context = socketdata ? (CurlContext *)socketdata : new CurlContext(socket);
		if (!socketdata)
		{
			TrackCurlContext(context);
		}
		curl_multi_assign(g_Curl, socket, context);

		if (action != CURL_POLL_IN)
		{
			events |= UV_WRITABLE;
		}
		if (action != CURL_POLL_OUT)
		{
			events |= UV_READABLE;
		}

		uv_poll_start(&context->poll_handle, events, &CurlSocketActivity);
		break;
	case CURL_POLL_REMOVE:
		if (socketdata)
		{
			context = (CurlContext *)socketdata;
			UntrackCurlContext(context);
			context->Destroy();

			curl_multi_assign(g_Curl, socket, nullptr);
		}
		break;
	}

	return 0;
}

static int CurlTimeoutCallback(CURLM *multi, long timeout_ms, void *userdata)
{
	if (timeout_ms < 0)
	{
		uv_timer_stop(&g_Timeout);
	}
	else
	{
		if (timeout_ms == 0)
		{
			timeout_ms = 1; /* 0 means directly call socket_action, but we will do it in a bit */
		}
		uv_timer_start(&g_Timeout, &PerformRequests, timeout_ms, 0);
	}
	return 0;
}

static void EventLoop(void *data)
{
	uv_run(g_Loop, UV_RUN_DEFAULT);
}

static void AsyncPerformRequests(uv_async_t *handle)
{
	int count = 0;

	while (count < MAX_PROCESS)
	{
		g_RequestQueue.Lock();
		if (g_RequestQueue.Empty())
		{
			g_RequestQueue.Unlock();
			break;
		}

		IHTTPContext *context = g_RequestQueue.Pop();
		g_RequestQueue.Unlock();
		count++;

		if (!context->InitCurl())
		{
			g_DeleteRequestQueue.Lock();
			g_DeleteRequestQueue.Push(context);
			g_DeleteRequestQueue.Unlock();
			continue;
		}

		CURLMcode code = curl_multi_add_handle(g_Curl, context->curl);
		if (code != CURLM_OK)
		{
			g_RipExt.LogError("Could not add cURL handle: %s", curl_multi_strerror(code));
			g_DeleteRequestQueue.Lock();
			g_DeleteRequestQueue.Push(context);
			g_DeleteRequestQueue.Unlock();
			continue;
		}

		TrackActiveRequest(context);
	}

	g_RequestQueue.Lock();
	bool hasMoreRequests = !g_RequestQueue.Empty();
	g_RequestQueue.Unlock();

	if (hasMoreRequests && !unloaded.load())
	{
		uv_async_send(&g_AsyncPerformRequests);
	}
}

static void AsyncStopLoop(uv_async_t *handle)
{
	uv_stop(g_Loop);
}

static void FrameHook(bool simulating)
{
	g_RequestQueue.Lock();
	bool hasRequests = !g_RequestQueue.Empty();
	g_RequestQueue.Unlock();

	if (hasRequests)
	{
		uv_async_send(&g_AsyncPerformRequests);
	}

	RunDeferredCallbacks();
	DeleteFailedRequests();
	RunCompletedRequests();
}

bool RipExt::SDK_OnLoad(char *error, size_t maxlength, bool late)
{
	sharesys->AddNatives(myself, http_natives);
	sharesys->AddNatives(myself, json_natives);
	sharesys->AddNatives(myself, websocket_natives);
	sharesys->AddNatives(myself, crypto_native);
	sharesys->RegisterLibrary(myself, "ripext");

	/* Initialize cURL */
	CURLcode res = curl_global_init(CURL_GLOBAL_ALL);
	if (res != CURLE_OK)
	{
		smutils->Format(error, maxlength, "%s", curl_easy_strerror(res));
		return false;
	}

	g_Curl = curl_multi_init();
	if (g_Curl == nullptr)
	{
		curl_global_cleanup();
		smutils->Format(error, maxlength, "%s", "Could not initialize cURL multi session.");
		return false;
	}
	curl_multi_setopt(g_Curl, CURLMOPT_SOCKETFUNCTION, &CurlSocketCallback);
	curl_multi_setopt(g_Curl, CURLMOPT_TIMERFUNCTION, &CurlTimeoutCallback);

	/* Initialize libuv */
	g_Loop = uv_default_loop();
	uv_timer_init(g_Loop, &g_Timeout);
	uv_async_init(g_Loop, &g_AsyncPerformRequests, &AsyncPerformRequests);
	uv_async_init(g_Loop, &g_AsyncStopLoop, &AsyncStopLoop);
	uv_thread_create(&g_Thread, &EventLoop, nullptr);

	/* Set up access rights for the 'HTTPRequest' handle type */
	HandleAccess haHTTPRequest;
	handlesys->InitAccessDefaults(nullptr, &haHTTPRequest);
	haHTTPRequest.access[HandleAccess_Delete] = 0;

	/* Set up access rights for the 'HTTPResponse' handle type */
	HandleAccess haHTTPResponse;
	handlesys->InitAccessDefaults(nullptr, &haHTTPResponse);
	haHTTPResponse.access[HandleAccess_Clone] = HANDLE_RESTRICT_IDENTITY;

	/* Set up access rights for the 'JSON' handle type */
	HandleAccess haJSON;
	handlesys->InitAccessDefaults(nullptr, &haJSON);
	haJSON.access[HandleAccess_Delete] = 0;

	/* Set up access rights for the 'WebSocket' handle type */
	HandleAccess haWS;
	TypeAccess taWS;

	handlesys->InitAccessDefaults(&taWS, &haWS);
	taWS.ident = myself->GetIdentity();
	haWS.access[HandleAccess_Read] = HANDLE_RESTRICT_OWNER;
	taWS.access[HTypeAccess_Create] = true;
	taWS.access[HTypeAccess_Inherit] = true;

	htHTTPRequest = handlesys->CreateType("HTTPRequest", &g_HTTPRequestHandler, 0, nullptr, &haHTTPRequest, myself->GetIdentity(), nullptr);
	htHTTPResponse = handlesys->CreateType("HTTPResponse", &g_HTTPResponseHandler, 0, nullptr, &haHTTPResponse, myself->GetIdentity(), nullptr);
	htJSON = handlesys->CreateType("JSON", &g_JSONHandler, 0, nullptr, &haJSON, myself->GetIdentity(), nullptr);
	htJSONObjectKeys = handlesys->CreateType("JSONObjectKeys", &g_JSONObjectKeysHandler, 0, nullptr, nullptr, myself->GetIdentity(), nullptr);
	htWebSocket = handlesys->CreateType("WebSocket", &g_WebSocketHandler, 0, &taWS, &haWS, myself->GetIdentity(), nullptr);

	smutils->AddGameFrameHook(&FrameHook);
	smutils->BuildPath(Path_SM, caBundlePath, sizeof(caBundlePath), SM_RIPEXT_CA_BUNDLE_PATH);

	event_loop.OnExtLoad();

	unloaded.store(false);

	return true;
}

void RipExt::SDK_OnUnload()
{
	unloaded.store(true);
	// Destroy WebSocket handles while the event loop is still alive so cancel()
	// can dispatch completion handlers. OnExtUnload waits for those handlers before
	// stopping the event loop.
	handlesys->RemoveType(htWebSocket, myself->GetIdentity());
	event_loop.OnExtUnload();

	uv_async_send(&g_AsyncStopLoop);
	uv_thread_join(&g_Thread);

	DrainDeferredCallbacks();
	CleanupActiveRequests();
	DrainRequestQueue(g_RequestQueue);
	DrainRequestQueue(g_CompletedRequestQueue);
	DrainRequestQueue(g_DeleteRequestQueue);
	CleanupCurlContexts();

	curl_multi_cleanup(g_Curl);
	curl_global_cleanup();

	CloseLibuvHandles();
	uv_loop_close(g_Loop);

	handlesys->RemoveType(htHTTPRequest, myself->GetIdentity());
	handlesys->RemoveType(htHTTPResponse, myself->GetIdentity());
	handlesys->RemoveType(htJSON, myself->GetIdentity());
	handlesys->RemoveType(htJSONObjectKeys, myself->GetIdentity());

	smutils->RemoveGameFrameHook(&FrameHook);
}

void RipExt::AddRequestToQueue(IHTTPContext *context)
{
	g_RequestQueue.Lock();
	g_RequestQueue.Push(context);
	g_RequestQueue.Unlock();
}

static bool DeferLog(bool error, char *buffer)
{
	if (!buffer)
	{
		return false;
	}

	std::shared_ptr<char> msg(buffer, &free);
	if (!g_RipExt.Defer([error, msg]() {
		if (unloaded.load())
		{
			return;
		}

		if (error)
		{
			smutils->LogError(myself, msg.get());
		}
		else
		{
			smutils->LogMessage(myself, msg.get());
		}
	}))
	{
		return false;
	}

	return true;
}

void RipExt::LogMessage(const char *msg, ...)
{
	if (unloaded.load())
	{
		return;
	}

	char *buffer = reinterpret_cast<char *>(malloc(3072));
	if (!buffer)
	{
		return;
	}

	va_list vp;
	va_start(vp, msg);
	vsnprintf(buffer, 3072, msg, vp);
	va_end(vp);

	DeferLog(false, buffer);
}

void RipExt::LogError(const char *msg, ...)
{
	if (unloaded.load())
	{
		return;
	}

	char *buffer = reinterpret_cast<char *>(malloc(3072));
	if (!buffer)
	{
		return;
	}

	va_list vp;
	va_start(vp, msg);
	vsnprintf(buffer, 3072, msg, vp);
	va_end(vp);

	DeferLog(true, buffer);
}

bool RipExt::Defer(std::function<void()> callback)
{
	if (unloaded.load())
	{
		return false;
	}

	if (!ReserveDeferredCallbackSlot())
	{
		g_DroppedDeferredCallbacks.fetch_add(1, std::memory_order_acq_rel);
		return false;
	}

	std::unique_ptr<std::function<void()>> cb = std::make_unique<std::function<void()>>(callback);
	g_DeferredCallbackQueue.Lock();
	g_DeferredCallbackQueue.Push(cb.release());
	g_DeferredCallbackQueue.Unlock();
	return true;
}

void HTTPRequestHandler::OnHandleDestroy(HandleType_t type, void *object)
{
	delete (HTTPRequest *)object;
}

void HTTPResponseHandler::OnHandleDestroy(HandleType_t type, void *object)
{
	/* Response objects are automatically cleaned up */
}

void JSONHandler::OnHandleDestroy(HandleType_t type, void *object)
{
	json_decref((json_t *)object);
}

void JSONObjectKeysHandler::OnHandleDestroy(HandleType_t type, void *object)
{
	delete (struct JSONObjectKeys *)object;
}

void WebSocketHandler::OnHandleDestroy(HandleType_t type, void *object)
{
	// Safely destroy the WebSocket connection.
	// destroy() sets pending_delete and calls cancel().
	// The actual deletion happens via maybe_delete() when async_ops reaches 0,
	// using compare_exchange to ensure only one thread executes delete.
	auto *conn = reinterpret_cast<websocket_connection_base *>(object);
	if (conn)
	{
		conn->destroy();
	}
}

bool WebSocketHandler::GetHandleApproxSize(HandleType_t type, void *object, unsigned int *size)
{
	*size = sizeof(websocket_connection_base);
	return true;
}
