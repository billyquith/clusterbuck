using System.Text.Json.Nodes;

namespace Clusterbuck.Worker.Commands;

/// <summary>`cbk fleet` — list the coordinator's capability/node registry (GET /fleet).</summary>
public static class FleetCommand
{
    public static async Task<int> RunAsync(CliArgs cli)
    {
        var baseUrl = cli.OptOrEnv("--server", "CBK_SERVER_URL", "http://localhost:8000");

        using var http = WorkerConfig.AdminHttp();
        using var resp = await http.GetAsync($"{baseUrl.TrimEnd('/')}/fleet");
        var text = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            Out.Error($"{(int)resp.StatusCode} {text}");
            return 1;
        }

        var root = JsonNode.Parse(text)!;
        var caps = root["capabilities"]?.AsObject();
        if (caps is null || caps.Count == 0)
        {
            Out.Warn("no fleet configured (no fleet.yaml on the coordinator)");
            return 0;
        }

        var rows = caps.Select(kv => new[]
        {
            kv.Key,
            kv.Value?["queue"]?.ToString() ?? "",
            kv.Value?["model"]?.ToString() ?? "",
            kv.Value?["model_server"]?.ToString() ?? "",
        }).ToList();
        Out.Table(new[] { "capability", "queue", "model", "model_server" }, rows);

        var nodes = root["nodes"]?.AsArray();
        if (nodes is { Count: > 0 })
        {
            Out.Line("");
            var nodeRows = nodes.Select(n => new[]
            {
                n?["id"]?.ToString() ?? "",
                n?["wake"]?.ToString() ?? "",
                string.Join(", ", n?["capabilities"]?.AsArray().Select(x => x?.ToString()) ?? []),
            }).ToList();
            Out.Table(new[] { "node", "wake", "capabilities" }, nodeRows);
        }
        return 0;
    }
}
