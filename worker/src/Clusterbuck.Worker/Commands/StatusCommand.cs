using System.Text.Json;
using System.Text.Json.Nodes;

namespace Clusterbuck.Worker.Commands;

/// <summary>`cbk status &lt;job_id&gt;` — poll a job via GET /jobs/{id}.</summary>
public static class StatusCommand
{
    public static async Task<int> RunAsync(CliArgs cli)
    {
        var jobId = cli.Arg(0);
        if (string.IsNullOrWhiteSpace(jobId))
        {
            Out.Error("usage: cbk status <job_id>");
            return 1;
        }
        var baseUrl = cli.OptOrEnv("--server", "CBK_SERVER_URL", "http://localhost:8000");

        using var http = WorkerConfig.AdminHttp();
        using var resp = await http.GetAsync($"{baseUrl.TrimEnd('/')}/jobs/{jobId}");
        var text = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            Out.Error($"{(int)resp.StatusCode} {text}");
            return 1;
        }

        var node = JsonNode.Parse(text)!;
        // urgency reflects the escalation trajectory (ADR 18), so show it alongside status.
        Out.Line($"status={node["status"]}  urgency={node["urgency"]}  attempts={node["attempts"]}");
        if (node["result"] is JsonNode result &&
            result["choices"]?[0]?["message"]?["content"]?.ToString() is { } content)
            Out.Line(content);
        if (node["error"] is JsonNode err && err.GetValueKind() != JsonValueKind.Null)
            Out.Error($"error: {err}");
        return 0;
    }
}
