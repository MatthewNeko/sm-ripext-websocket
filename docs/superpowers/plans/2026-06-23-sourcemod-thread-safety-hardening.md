# SourceMod Thread Safety Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the remaining SourceMod 1.12 main-thread stall, logging flood, callback overload, and lifetime risks introduced or found during review.

**Architecture:** Keep every SourcePawn/SourceMod handle and forward operation on the game thread. Network/libuv/Boost.Asio threads may enqueue work only through bounded queues; queue overload must preserve plugin-visible lifecycle by disconnecting or dropping low-value progress/log callbacks explicitly. Avoid broad rewrites and keep existing extension structure.

**Tech Stack:** SourceMod 1.12 extension SDK, C++11-style codebase, libuv, libcurl multi, Boost.Asio/Beast WebSocket, Jansson JSON, AMBuild.

---

## File Structure

- Modify `src/extension.cpp`
  - Narrow `g_RequestQueue` lock scope in `AsyncPerformRequests()`.
  - Route extension logs through the existing bounded deferred callback queue instead of direct `AddFrameAction()` flood.
  - Add queue-drain helpers for unload safety.
  - Optionally reduce per-frame completed request default.

- Modify `src/extension.h`
  - Keep `RipExt::Defer()` returning `bool`.
  - No further public API expansion unless a task explicitly requires it.

- Modify `src/websocket_native.cpp`
  - Check `g_RipExt.Defer()` return values.
  - For read callback overload, close the WebSocket connection to preserve lifecycle instead of silently dropping reads.
  - Validate `json_loads()` failure before creating `htJSON` handle.

- Modify `src/httpfilecontext.cpp`
  - Remove unnecessary shared progress members from `setProgressData()`.
  - Keep pending progress callback lifetime tracking.

- Modify `src/httpfilecontext.h`
  - Remove unused `dltotal`, `dlnow`, `ultotal`, `ulnow` members.

- No new test framework is currently present. Verification uses static diagnostics, `git diff --check`, and full AMBuild compile when `ambuild2` and SourceMod SDK paths are available.

---

### Task 1: Narrow request queue lock scope

**Files:**
- Modify: `src/extension.cpp:289-316`

- [ ] **Step 1: Inspect current locking pattern**

Confirm `AsyncPerformRequests()` currently keeps `g_RequestQueue` locked while executing `context->InitCurl()` and `curl_multi_add_handle()`.

Expected current shape:

```cpp
static void AsyncPerformRequests(uv_async_t *handle)
{
	g_RequestQueue.Lock();
	IHTTPContext *context;
	// Limiter
	int count = 0;

	while (!g_RequestQueue.Empty() && count < MAX_PROCESS)
	{
		context = g_RequestQueue.Pop();

		if (!context->InitCurl())
		{
			g_DeleteRequestQueue.Lock();
			g_DeleteRequestQueue.Push(context);
			g_DeleteRequestQueue.Unlock();
			continue;
		}

		curl_multi_add_handle(g_Curl, context->curl);
		count++;
	}

	g_RequestQueue.Unlock();
}
```

- [ ] **Step 2: Replace with pop-only lock scope**

Change the function to this exact structure:

```cpp
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

		if (!context->InitCurl())
		{
			g_DeleteRequestQueue.Lock();
			g_DeleteRequestQueue.Push(context);
			g_DeleteRequestQueue.Unlock();
			continue;
		}

		curl_multi_add_handle(g_Curl, context->curl);
		count++;
	}
}
```

Rationale: `AddRequestToQueue()` can be called from SourceMod native paths on the game thread; it must not wait for file I/O or curl setup in the libuv thread.

- [ ] **Step 3: Run targeted diagnostics**

Run:

```powershell
git diff -- src/extension.cpp
```

Expected: only `AsyncPerformRequests()` lock scope changes in this task.

- [ ] **Step 4: Commit**

```powershell
git add src/extension.cpp
git commit -m "fix: narrow request queue lock scope"
```

---

### Task 2: Route logs through bounded queue

**Files:**
- Modify: `src/extension.cpp:450-484`

- [ ] **Step 1: Remove stale direct frame-action callback helpers**

Remove these helper functions only if no other code references them after the new log implementation:

```cpp
void log_msg(void *msg)
{
	if (!unloaded.load())
	{
		smutils->LogMessage(myself, reinterpret_cast<char *>(msg));
	}
	free(msg);
}

void log_err(void *msg)
{
	if (!unloaded.load())
	{
		smutils->LogError(myself, reinterpret_cast<char *>(msg));
	}
	free(msg);
}
```

- [ ] **Step 2: Add bounded log helper**

Insert this helper before `RipExt::LogMessage()`:

```cpp
static bool DeferLog(bool error, char *buffer)
{
	if (!buffer)
	{
		return false;
	}

	if (!g_RipExt.Defer([error, buffer]() {
		std::unique_ptr<char, decltype(&free)> msg(buffer, &free);
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
		free(buffer);
		return false;
	}

	return true;
}
```

Rationale: logs from network callbacks must respect the same bounded per-frame queue as other main-thread work.

- [ ] **Step 3: Update `RipExt::LogMessage()`**

Replace the tail of `RipExt::LogMessage()` with:

```cpp
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
```

Full function should be:

```cpp
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
```

- [ ] **Step 4: Update `RipExt::LogError()`**

Full function should be:

```cpp
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
```

- [ ] **Step 5: Check for remaining log frame actions**

Run:

```powershell
Select-String -Path src\extension.cpp -Pattern "AddFrameAction|log_msg|log_err"
```

Expected: only `execute_cb` may remain if it has not yet been removed; there should be no log path using `AddFrameAction`.

- [ ] **Step 6: Commit**

```powershell
git add src/extension.cpp
git commit -m "fix: bound deferred extension logs"
```

---

### Task 3: Drain deferred queues during unload

**Files:**
- Modify: `src/extension.cpp:404-420`

- [ ] **Step 1: Add queue drain helpers**

Insert these helpers near `RunCompletedRequests()`:

```cpp
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
```

Note: These helpers must only run after `unloaded.store(true)` and after worker threads have stopped posting new work.

- [ ] **Step 2: Call drains in `SDK_OnUnload()`**

Change unload flow to this structure:

```cpp
void RipExt::SDK_OnUnload()
{
	unloaded.store(true);
	event_loop.OnExtUnload();

	uv_async_send(&g_AsyncStopLoop);
	uv_thread_join(&g_Thread);
	uv_loop_close(g_Loop);

	DrainDeferredCallbacks();
	DrainRequestQueue(g_RequestQueue);
	DrainRequestQueue(g_CompletedRequestQueue);
	DrainRequestQueue(g_DeleteRequestQueue);

	curl_multi_cleanup(&g_Curl);
	curl_global_cleanup();

	handlesys->RemoveType(htHTTPRequest, myself->GetIdentity());
	handlesys->RemoveType(htHTTPResponse, myself->GetIdentity());
	handlesys->RemoveType(htJSON, myself->GetIdentity());
	handlesys->RemoveType(htJSONObjectKeys, myself->GetIdentity());
	handlesys->RemoveType(htWebSocket, myself->GetIdentity());

	smutils->RemoveGameFrameHook(&FrameHook);
}
```

- [ ] **Step 3: Validate lifetime ordering**

Manual checklist:

- `unloaded` set before drains.
- WebSocket event loop stopped before draining deferred callbacks.
- libuv thread joined before draining HTTP request queues.
- SourceMod handle types removed only after queues are drained.

- [ ] **Step 4: Commit**

```powershell
git add src/extension.cpp
git commit -m "fix: drain deferred queues on unload"
```

---

### Task 4: Fix WebSocket deferred overload semantics and JSON parse failure

**Files:**
- Modify: `src/websocket_native.cpp:86-111`
- Modify: `src/websocket_native.cpp:128-160`

- [ ] **Step 1: Update read callback to close on overload**

Replace current `connection->set_read_callback(...)` body with:

```cpp
	connection->set_read_callback([connection, callback, hndl_websocket, p_context, data, callback_type](auto buffer, auto size)
								  {
		std::string message(reinterpret_cast<const char*>(buffer), size);
		free(buffer);

		if (!g_RipExt.Defer([callback, hndl_websocket, message, p_context, data, callback_type]() {
			callback->PushCell(hndl_websocket);
			if (callback_type == WebSocket_JSON)
			{
				json_error_t error;
				json_t *object = json_loads(message.data(), 0, &error);
				if (!object)
				{
					g_RipExt.LogError("WebSocket JSON parse failed: %s", error.text);
					callback->PushCell(BAD_HANDLE);
				}
				else
				{
					Handle_t handle = handlesys->CreateHandle(htJSON, object, p_context->GetIdentity(), myself->GetIdentity(), nullptr);
					callback->PushCell(handle);
				}
			}
			else if (callback_type == Websocket_STRING)
			{
				callback->PushString(message.data());
			}
			callback->PushCell(data);
			callback->Execute(nullptr);
		}))
		{
			g_RipExt.LogError("WebSocket read callback queue overflow; closing connection.");
			connection->close();
		}
	});
```

Rationale: read events are ordered stream semantics. If the extension cannot deliver one, the connection state is no longer reliable; close it and let disconnect callback synchronize plugin state.

- [ ] **Step 2: Update disconnect callback to log overload**

Change disconnect setter body to:

```cpp
	connection->set_disconnect_callback([callback, hndl_websocket, p_context, data]()
									{
		if (!g_RipExt.Defer([callback, hndl_websocket, p_context, data]()
						 {
			callback->PushCell(hndl_websocket);
			callback->PushCell(data);
			callback->Execute(nullptr);
		}))
		{
			g_RipExt.LogError("WebSocket disconnect callback dropped due to deferred queue overflow.");
		}
	});
```

- [ ] **Step 3: Update connect callback to close on overload**

Change connect setter body to:

```cpp
	connection->set_connect_callback([connection, callback, hndl_websocket, p_context, data]()
							   {
		if (!g_RipExt.Defer([callback, hndl_websocket, p_context, data]()
						 {
			callback->PushCell(hndl_websocket);
			callback->PushCell(data);
			callback->Execute(nullptr);
		}))
		{
			g_RipExt.LogError("WebSocket connect callback queue overflow; closing connection.");
			connection->close();
		}
	});
```

Rationale: if connect event cannot be delivered, plugin state cannot safely use the connection.

- [ ] **Step 4: Verify JSON failure behavior compiles against existing includes**

`websocket_native.cpp` already includes `websocket_connection_base.h`, `websocket_connection_ssl.h`, `websocket_connection.h`, and `url.hpp`; `BAD_HANDLE` is available through SourceMod SDK headers included by `extension.h`.

Run:

```powershell
Select-String -Path src\websocket_native.cpp -Pattern "json_loads|BAD_HANDLE|queue overflow|CreateHandle"
```

Expected:

- `json_loads` uses `json_error_t`.
- `CreateHandle(htJSON, object...)` is only called when `object` is non-null.
- overload paths log and close for read/connect.

- [ ] **Step 5: Commit**

```powershell
git add src/websocket_native.cpp
git commit -m "fix: handle websocket callback overload"
```

---

### Task 5: Remove unused shared progress fields

**Files:**
- Modify: `src/httpfilecontext.h:47-50`
- Modify: `src/httpfilecontext.cpp:160-165`

- [ ] **Step 1: Remove fields from header**

Delete these fields from `HTTPFileContext`:

```cpp
	curl_off_t dltotal;
	curl_off_t dlnow;
	curl_off_t ultotal;
	curl_off_t ulnow;
```

Keep:

```cpp
	std::atomic<unsigned int> pendingCallbacks{0};
	FILE *file = nullptr;
```

- [ ] **Step 2: Remove writes in `setProgressData()`**

Change function start from:

```cpp
void HTTPFileContext::setProgressData(curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow)
{
	this->dltotal = dltotal;
	this->dlnow = dlnow;
	this->ultotal = ultotal;
	this->ulnow = ulnow;
	if (dltotal != 0 || ultotal != 0)
```

To:

```cpp
void HTTPFileContext::setProgressData(curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow)
{
	if (dltotal != 0 || ultotal != 0)
```

- [ ] **Step 3: Verify no old fields remain**

Run:

```powershell
Select-String -Path src\httpfilecontext.* -Pattern "this->dltotal|this->dlnow|this->ultotal|this->ulnow|curl_off_t dltotal;|curl_off_t dlnow;|curl_off_t ultotal;|curl_off_t ulnow;"
```

Expected: no matches.

- [ ] **Step 4: Commit**

```powershell
git add src/httpfilecontext.cpp src/httpfilecontext.h
git commit -m "refactor: remove shared progress fields"
```

---

### Task 6: Tune per-frame request callback budget

**Files:**
- Modify: `src/extension.cpp:30-33`

- [ ] **Step 1: Decide conservative default**

For SourceMod game frames, prefer lower completion callback budget than HTTP request ingestion. Change:

```cpp
#define MAX_COMPLETED_PROCESS 10
```

To:

```cpp
#define MAX_COMPLETED_PROCESS 4
```

Keep:

```cpp
#define MAX_PROCESS 10
#define MAX_DEFERRED_CALLBACKS_PER_FRAME 64
#define MAX_DEFERRED_CALLBACKS_QUEUED 4096
```

Rationale: `OnCompleted()` executes SourcePawn forwards and may run plugin code; a lower default reduces per-frame worst-case stall while still draining faster than the old one-per-frame behavior.

- [ ] **Step 2: Verify macro values**

Run:

```powershell
Select-String -Path src\extension.cpp -Pattern "MAX_PROCESS|MAX_COMPLETED_PROCESS|MAX_DEFERRED_CALLBACKS"
```

Expected:

```text
MAX_PROCESS 10
MAX_COMPLETED_PROCESS 4
MAX_DEFERRED_CALLBACKS_PER_FRAME 64
MAX_DEFERRED_CALLBACKS_QUEUED 4096
```

- [ ] **Step 3: Commit**

```powershell
git add src/extension.cpp
git commit -m "chore: lower completed callback frame budget"
```

---

### Task 7: Final verification

**Files:**
- Check all modified files.

- [ ] **Step 1: Check whitespace and patch format**

Run:

```powershell
git diff --check
```

Expected: no whitespace errors. Existing line-ending warnings may appear and are acceptable if no `trailing whitespace` or `space before tab` errors appear.

- [ ] **Step 2: Check source diagnostics**

Run VS Code diagnostics for:

- `src/extension.cpp`
- `src/extension.h`
- `src/websocket_native.cpp`
- `src/httpfilecontext.cpp`
- `src/httpfilecontext.h`

Expected: no new diagnostics caused by this change. Existing includePath errors for SourceMod SDK headers, if still present, are environment configuration issues and must be reported separately.

- [ ] **Step 3: Attempt AMBuild configure**

Run from repo root:

```powershell
& "f:/CSGO SCRIPT/sm-ripext-websocket-master/.venv/Scripts/python.exe" configure.py --help
```

Expected if environment is incomplete:

```text
ModuleNotFoundError: No module named 'ambuild2'
```

If `ambuild2` is installed, continue to the next step.

- [ ] **Step 4: Full compile when dependencies exist**

If AMBuild and SDK paths are configured, run the project’s normal configure/build command. Example shape:

```powershell
& "f:/CSGO SCRIPT/sm-ripext-websocket-master/.venv/Scripts/python.exe" configure.py --sm-path "<path-to-sourcemod-1.12>" --targets x86
ambuild
```

Expected: compile succeeds with no errors.

- [ ] **Step 5: Manual runtime smoke test on SourceMod 1.12 server**

Test scenario:

1. Load extension on a SourceMod 1.12 test server.
2. Create one WebSocket connection.
3. Send valid JSON and raw string messages.
4. Send malformed JSON when callback type is JSON.
5. Flood WebSocket messages until queue pressure is observable.
6. Start several HTTP file transfers with progress callback.
7. Unload extension during idle and during active connection.

Expected:

- Valid messages invoke callbacks on game thread.
- Malformed JSON passes `BAD_HANDLE` and logs parse error, no crash.
- Queue overload closes WebSocket instead of silently losing read state.
- Server tick does not freeze during callback bursts.
- Extension unload does not crash or use freed SourceMod forwards.

- [ ] **Step 6: Final diff review**

Run:

```powershell
git diff --stat
git diff -- src/extension.cpp src/extension.h src/websocket_native.cpp src/httpfilecontext.cpp src/httpfilecontext.h
```

Expected:

- No unrelated refactors.
- SourceMod API calls remain on game thread.
- libuv/Asio threads only enqueue bounded work or close sockets.

---

## Self-Review Checklist

- Spec coverage:
  - Request queue lock stall: Task 1.
  - Log flood through `AddFrameAction`: Task 2.
  - Unload queue cleanup: Task 3.
  - WebSocket overload semantics and JSON parse failure: Task 4.
  - Unused progress fields: Task 5.
  - Per-frame callback budget: Task 6.
  - Verification: Task 7.

- Placeholder scan:
  - No `TBD`, `TODO`, or unspecified implementation steps.
  - Commands and expected results are included.

- Type consistency:
  - `RipExt::Defer(std::function<void()>)` returns `bool`.
  - `LockedQueue<IHTTPContext *>` is used by `DrainRequestQueue()`.
  - `BAD_HANDLE` is used only in game-thread deferred WebSocket callback.

- SourceMod 1.12 constraint:
  - `IPluginFunction::Execute`, `IChangeableForward::Execute`, `handlesys->CreateHandle`, and `forwards->ReleaseForward` remain on the game thread or unload path after worker threads stop.
