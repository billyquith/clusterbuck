using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Clusterbuck.Worker;

/// <summary>
/// Installs and removes model artifacts on this node.
///
/// **This is the one place clusterbuck necessarily leaves the generic OpenAI wire protocol**
/// (see decisions.md ADR 25). "Pull a model" has no OpenAI-standard endpoint: Ollama has
/// `POST /api/pull`, llama.cpp has no concept of it (you hand it a GGUF path), and vLLM
/// fetches at launch. So installation lives behind this small pluggable adapter, kept
/// strictly separate from the inference path — which stays vendor-neutral (protocols.md §3).
///
/// The worker never *decides* to install anything; it executes an action the coordinator
/// issued for an approved proposal (fleet-management.md → three gates).
/// </summary>
public sealed class ModelManager
{
    private readonly HttpClient _http;
    private readonly string _nativeBase;
    private readonly string _manager;

    public ModelManager(HttpClient http, string nativeBase, string manager)
    {
        _http = http;
        _nativeBase = nativeBase.TrimEnd('/');
        _manager = manager;
    }

    /// <summary>Whether this node can install/remove models itself.</summary>
    public bool CanManage => _manager is "ollama" or "auto";

    /// <summary>Pull an artifact. Returns (ok, error).</summary>
    public async Task<(bool ok, string? error)> InstallAsync(
        string registryRef, CancellationToken ct = default)
    {
        if (!CanManage)
            return (false, $"model manager '{_manager}' cannot install; install {registryRef} manually");
        try
        {
            // Ollama streams NDJSON progress; `stream:false` returns a single final object.
            // JsonNode serializes itself — no reflection serializer, so this stays AOT-safe.
            var body = new JsonObject { ["model"] = registryRef, ["stream"] = false }.ToJsonString();
            using var content = new StringContent(body, Encoding.UTF8, "application/json");
            using var resp = await _http.PostAsync($"{_nativeBase}/api/pull", content, ct);
            var text = await resp.Content.ReadAsStringAsync(ct);
            if (!resp.IsSuccessStatusCode)
                return (false, $"pull failed: HTTP {(int)resp.StatusCode} {Trim(text)}");
            // Ollama reports a terminal {"status":"success"} — or an {"error":...}.
            using var doc = JsonDocument.Parse(text);
            if (doc.RootElement.TryGetProperty("error", out var err))
                return (false, $"pull failed: {err.GetString()}");
            return (true, null);
        }
        catch (Exception e)
        {
            return (false, $"pull failed: {e.Message}");
        }
    }

    /// <summary>Remove an artifact, returning disk to the owner.</summary>
    public async Task<(bool ok, string? error)> RemoveAsync(
        string artifact, CancellationToken ct = default)
    {
        if (!CanManage)
            return (false, $"model manager '{_manager}' cannot remove; remove {artifact} manually");
        try
        {
            var body = new JsonObject { ["model"] = artifact }.ToJsonString();
            using var msg = new HttpRequestMessage(HttpMethod.Delete, $"{_nativeBase}/api/delete")
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json"),
            };
            using var resp = await _http.SendAsync(msg, ct);
            if (!resp.IsSuccessStatusCode)
            {
                var text = await resp.Content.ReadAsStringAsync(ct);
                return (false, $"remove failed: HTTP {(int)resp.StatusCode} {Trim(text)}");
            }
            return (true, null);
        }
        catch (Exception e)
        {
            return (false, $"remove failed: {e.Message}");
        }
    }

    private static string Trim(string s) => s.Length > 160 ? s[..160] : s;
}
