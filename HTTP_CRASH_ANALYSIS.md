# HTTP 超时/失败崩溃风险分析报告

## 执行日期
2026-07-28

## 分析概述
对 sm-ripext-websocket 扩展进行了全面的错误处理审计，重点关注 HTTP 超时和失败场景下的崩溃风险。

---

## ✅ 安全的机制

### 1. **超时配置正确**
```cpp
curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, connectTimeout);
curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout);
```
- 连接超时和总超时均已配置
- cURL 会在超时时中止请求

### 2. **错误信息捕获**
```cpp
curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, error);
```
- 错误缓冲区正确设置
- 错误信息会传递给 Pawn 回调的第三个参数

### 3. **失败请求清理流程**
```cpp
// AsyncPerformRequests 中
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
```
- 失败的请求正确进入删除队列
- 析构函数释放所有资源

### 4. **realloc 失败处理**
```cpp
static size_t WriteResponseBody(void *body, size_t size, size_t nmemb, void *userdata)
{
    size_t total = size * nmemb;
    struct HTTPResponse *response = (struct HTTPResponse *)userdata;

    char *temp = (char *)realloc(response->body, response->size + total + 1);
    if (temp == nullptr)
    {
        return 0;  // 告诉 cURL 停止传输
    }
    // ...
}
```
- realloc 失败时返回 0，cURL 会中止传输
- 不会造成内存泄漏

### 5. **插件卸载保护**
```cpp
void HTTPRequestContext::OnCompleted()
{
    if (forward->GetFunctionCount() == 0)
    {
        return;  // 插件已卸载，直接返回
    }
    // ...
}
```

---

## ⚠️ **已发现的崩溃风险（已修复）**

### 🔴 **严重问题 #1: `GetResponseStr` 空指针解引用**

**问题描述**：
当 HTTP 请求超时、连接失败、DNS 解析失败时，`WriteResponseBody` 回调从未被调用，导致 `response->body` 保持为 `nullptr`。如果 Pawn 代码调用 `HTTPResponse.GetString()`，会直接崩溃。

**触发条件**：
- 连接超时（CURLOPT_CONNECTTIMEOUT）
- 请求超时（CURLOPT_TIMEOUT）
- DNS 解析失败
- 网络不可达
- SSL 握手失败

**原始代码**：
```cpp
static cell_t GetResponseStr(IPluginContext *pContext, const cell_t *params)
{
    // ... 句柄验证 ...
    
    // ⚠️ response->body 可能是 nullptr！
    pContext->StringToLocalUTF8(params[2], params[3], response->body, nullptr);
    return 1;
}
```

**修复方案**：
```cpp
// 如果没有响应体，返回空字符串而不是崩溃
const char *bodyStr = response->body ? response->body : "";
pContext->StringToLocalUTF8(params[2], params[3], bodyStr, nullptr);
```

---

### 🔴 **严重问题 #2: `GetResponseData` 空指针传递给 json_loads**

**问题描述**：
类似问题，当 `response->body` 为 `nullptr` 时，直接传递给 `json_loads()` 会导致崩溃或未定义行为。

**原始代码**：
```cpp
static cell_t GetResponseData(IPluginContext *pContext, const cell_t *params)
{
    // ...
    
    json_error_t error;
    // ⚠️ response->body 可能是 nullptr！
    response->data = json_loads(response->body, 0, &error);
    // ...
}
```

**修复方案**：
```cpp
// 检查响应体是否存在
if (response->body == nullptr || response->size == 0)
{
    pContext->ReportError("No response body received (request may have failed or timed out)");
    return BAD_HANDLE;
}

json_error_t error;
response->data = json_loads(response->body, 0, &error);
```

---

## 📊 **崩溃场景测试矩阵**

| 场景 | response.body 状态 | response.status | error 内容 | 修复前 | 修复后 |
|------|-------------------|----------------|-----------|--------|--------|
| 连接超时 | `nullptr` | 0 | "Timeout was reached" | 💥 崩溃 | ✅ 返回空字符串 |
| DNS 失败 | `nullptr` | 0 | "Could not resolve host" | 💥 崩溃 | ✅ 返回空字符串 |
| 连接拒绝 | `nullptr` | 0 | "Connection refused" | 💥 崩溃 | ✅ 返回空字符串 |
| SSL 错误 | `nullptr` | 0 | "SSL certificate problem" | 💥 崩溃 | ✅ 返回空字符串 |
| 200 OK 空响应 | `""` (空字符串) | 200 | "" | ⚠️ JSON 解析失败 | ✅ 报错但不崩溃 |
| 200 OK 有数据 | 有效指针 | 200 | "" | ✅ 正常 | ✅ 正常 |

---

## 🛡️ **建议的额外增强**

### 1. **日志增强**（可选）
在 `OnCompleted` 中记录失败详情：
```cpp
if (response.body == nullptr)
{
    smutils->LogMessage(myself, "HTTP request failed: %s (URL: %s)", error, url.c_str());
}
```

### 2. **Pawn 侧防御性编程**
```pawn
public void OnHTTPResponse(HTTPResponse response, any value, const char[] error)
{
    // 总是先检查错误
    if (error[0] != '\0')
    {
        LogError("HTTP request failed: %s", error);
        return;
    }
    
    // 检查状态码
    if (response.Status != 200)
    {
        LogError("HTTP request returned status %d", response.Status);
        return;
    }
    
    // 然后再尝试获取数据
    JSONObject data = view_as<JSONObject>(response.Data);
    // ...
}
```

---

## 📝 **修复文件清单**

- ✅ `src/http_natives.cpp` - `GetResponseStr()` 函数
- ✅ `src/http_natives.cpp` - `GetResponseData()` 函数

---

## 🎯 **结论**

**原始代码存在严重崩溃风险**，在以下常见场景中会导致服务器崩溃：
- 网络不稳定导致超时
- 目标服务器宕机
- DNS 解析问题
- 防火墙阻止连接

**修复后的代码**：
- ✅ 不会因为超时/失败而崩溃
- ✅ 正确返回错误信息给 Pawn 回调
- ✅ 通过 `error` 参数提供详细错误原因
- ✅ 允许 Pawn 代码进行适当的错误处理

**风险等级**：
- 修复前：🔴 **严重** - 生产环境不应使用
- 修复后：🟢 **低** - 可安全部署
