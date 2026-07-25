using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Clusterbuck.Worker;

/// <summary>
/// Speaks the OpenAI-compatible wire protocol (POST /v1/chat/completions) to whatever
/// model server runs on this node — Ollama, llama.cpp, vLLM, LM Studio (protocols.md §3).
/// No vendor SDK: the request/response are plain JSON, so the servers are interchangeable.
/// </summary>
public sealed class ModelClient
{
    private readonly HttpClient _http;
    private readonly WorkerConfig _cfg;

    public ModelClient(HttpClient http, WorkerConfig cfg)
    {
        _http = http;
        _cfg = cfg;
    }

    /// <summary>Run one completion. Returns the completion element and its usage (if any).</summary>
    public async Task<(JsonElement completion, JsonElement? usage)> CompleteAsync(
        Job job, CancellationToken ct)
    {
        var messages = new JsonArray();
        if (job.Messages is { Count: > 0 })
        {
            foreach (var m in job.Messages)
                messages.Add((JsonNode)new JsonObject { ["role"] = m.Role, ["content"] = m.Content });
        }
        else
        {
            messages.Add((JsonNode)new JsonObject { ["role"] = "user", ["content"] = job.Prompt ?? "" });
        }

        var request = new JsonObject
        {
            ["model"] = _cfg.ModelName,
            ["messages"] = messages,
            ["stream"] = false,
        };

        // Pass inference params straight through (temperature, max_tokens, …).
        if (job.Params.ValueKind == JsonValueKind.Object)
        {
            foreach (var p in job.Params.EnumerateObject())
            {
                if (p.NameEquals("response_format")) continue; // hint only (protocols.md §3)
                request[p.Name] = JsonNode.Parse(p.Value.GetRawText());
            }
        }

        var url = _cfg.ModelServerUrl.TrimEnd('/') + "/chat/completions";
        // JsonNode serializes itself (no reflection serializer → AOT-safe).
        using var content = new StringContent(
            request.ToJsonString(), Encoding.UTF8, "application/json");
        using var resp = await _http.PostAsync(url, content, ct);
        resp.EnsureSuccessStatusCode();

        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
        var root = doc.RootElement;
        JsonElement? usage = root.TryGetProperty("usage", out var u) ? u.Clone() : null;
        return (root.Clone(), usage);
    }
}
