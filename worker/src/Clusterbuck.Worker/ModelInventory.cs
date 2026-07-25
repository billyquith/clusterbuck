using System.Text.Json;

namespace Clusterbuck.Worker;

/// <summary>
/// Discovers what models this node's model server has (fleet-management.md → Dynamic
/// registry). Replaces hand-maintained `fleet.yaml` model lists with observed reality.
///
/// **Installed** comes from the generic OpenAI-compatible `GET /v1/models`, which Ollama,
/// LM Studio, vLLM and llama.cpp-server all serve — so one portable code path, no vendor
/// SDK, consistent with protocols.md §3.
///
/// **Loaded** (warm right now) and **digests** are NOT in the OpenAI standard, so they need
/// a small per-server adapter (Ollama: `/api/ps`, `/api/tags`). Anything the adapter can't
/// answer degrades to "unknown" (empty) rather than failing the heartbeat — we never let
/// inventory reporting take a worker down. Scanning vendor model directories on disk is
/// deliberately NOT the primary mechanism: those layouts are undocumented internals, and a
/// blob on disk the server hasn't registered isn't servable anyway.
/// </summary>
public sealed class ModelInventory
{
    private readonly HttpClient _http;
    private readonly string _modelServerUrl;
    private readonly string _manager;

    public ModelInventory(HttpClient http, string modelServerUrl, string manager)
    {
        _http = http;
        _modelServerUrl = modelServerUrl.TrimEnd('/');
        _manager = manager;
    }

    /// <summary>Native (non-OpenAI) base URL: strip the trailing /v1 the OpenAI path adds.</summary>
    public string NativeBase =>
        _modelServerUrl.EndsWith("/v1", StringComparison.Ordinal)
            ? _modelServerUrl[..^3]
            : _modelServerUrl;

    /// <summary>Models the server can serve, via the portable OpenAI endpoint.</summary>
    public async Task<List<string>> InstalledAsync(CancellationToken ct = default)
    {
        var ids = new List<string>();
        try
        {
            using var doc = await GetJsonAsync($"{_modelServerUrl}/models", ct);
            if (doc is not null && doc.RootElement.TryGetProperty("data", out var data)
                && data.ValueKind == JsonValueKind.Array)
            {
                foreach (var m in data.EnumerateArray())
                    if (m.TryGetProperty("id", out var id) && id.GetString() is string s)
                        ids.Add(s);
            }
        }
        catch
        {
            // Server down or non-conforming: report nothing rather than crash the beat.
        }
        ids.Sort(StringComparer.Ordinal);
        return ids;
    }

    /// <summary>Models warm in memory right now (vendor-specific; empty = unknown).</summary>
    public async Task<List<string>> LoadedAsync(CancellationToken ct = default)
    {
        var loaded = new List<string>();
        if (!IsOllama) return loaded;
        try
        {
            using var doc = await GetJsonAsync($"{NativeBase}/api/ps", ct);
            if (doc is not null && doc.RootElement.TryGetProperty("models", out var models)
                && models.ValueKind == JsonValueKind.Array)
            {
                foreach (var m in models.EnumerateArray())
                    if (m.TryGetProperty("name", out var n) && n.GetString() is string s)
                        loaded.Add(s);
            }
        }
        catch
        {
        }
        loaded.Sort(StringComparer.Ordinal);
        return loaded;
    }

    /// <summary>
    /// artifact → content digest, where the server exposes it. Digests are what make
    /// "this model was updated upstream" detectable — and, because ability is pinned to an
    /// artifact (ADR 15), what forces re-measurement when one changes.
    /// </summary>
    public async Task<Dictionary<string, string>> DigestsAsync(CancellationToken ct = default)
    {
        var digests = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!IsOllama) return digests;
        try
        {
            using var doc = await GetJsonAsync($"{NativeBase}/api/tags", ct);
            if (doc is not null && doc.RootElement.TryGetProperty("models", out var models)
                && models.ValueKind == JsonValueKind.Array)
            {
                foreach (var m in models.EnumerateArray())
                {
                    if (m.TryGetProperty("name", out var n) && n.GetString() is string name
                        && m.TryGetProperty("digest", out var d) && d.GetString() is string dig)
                        digests[name] = dig;
                }
            }
        }
        catch
        {
        }
        return digests;
    }

    private bool IsOllama => _manager is "ollama" or "auto";

    private async Task<JsonDocument?> GetJsonAsync(string url, CancellationToken ct)
    {
        using var resp = await _http.GetAsync(url, ct);
        if (!resp.IsSuccessStatusCode) return null;
        return JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
    }
}
