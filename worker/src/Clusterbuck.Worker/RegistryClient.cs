using System.Text;
using System.Text.Json;

namespace Clusterbuck.Worker;

/// <summary>Enrollment + heartbeat over HTTP to the coordinator (protocols.md §6).</summary>
public sealed class RegistryClient
{
    private readonly HttpClient _http;
    private readonly string _server;

    public RegistryClient(HttpClient http, string server)
    {
        _http = http;
        _server = server.TrimEnd('/');
    }

    public async Task<EnrollResponse> EnrollAsync(EnrollRequest req, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(req, CbkJsonContext.Default.EnrollRequest);
        using var resp = await _http.PostAsync(
            $"{_server}/nodes/enroll", Json(json), ct);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize(body, CbkJsonContext.Default.EnrollResponse)!;
    }

    public async Task<HeartbeatResponse> HeartbeatAsync(
        string nodeId, string nodeKey, HeartbeatRequest req, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(req, CbkJsonContext.Default.HeartbeatRequest);
        using var msg = new HttpRequestMessage(HttpMethod.Post, $"{_server}/nodes/{nodeId}/heartbeat")
        {
            Content = Json(json),
        };
        msg.Headers.Add("X-CBK-Node-Key", nodeKey);
        using var resp = await _http.SendAsync(msg, ct);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize(body, CbkJsonContext.Default.HeartbeatResponse)!;
    }

    private static StringContent Json(string s) => new(s, Encoding.UTF8, "application/json");
}

/// <summary>Loads/saves the persisted node identity + mode (survives restarts).</summary>
public static class NodeStateStore
{
    public static string DefaultPath =>
        Environment.GetEnvironmentVariable("CBK_NODE_STATE")
        ?? Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
            ".clusterbuck", "node.json");

    public static NodeState? Load(string path)
    {
        if (!File.Exists(path)) return null;
        return JsonSerializer.Deserialize(File.ReadAllText(path), CbkJsonContext.Default.NodeState);
    }

    public static void Save(string path, NodeState state)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path) ?? ".");
        File.WriteAllText(path, JsonSerializer.Serialize(state, CbkJsonContext.Default.NodeState));
    }
}
