using System.Text;
using System.Text.Json.Nodes;

namespace Clusterbuck.Worker.Commands;

/// <summary>`cbk submit` — a thin client over POST /jobs (protocols.md §1b).</summary>
public static class SubmitCommand
{
    public static async Task<int> RunAsync(CliArgs cli)
    {
        var prompt = cli.Opt("--prompt");
        if (string.IsNullOrWhiteSpace(prompt))
        {
            Out.Error("--prompt is required");
            return 1;
        }

        var body = new JsonObject
        {
            ["messages"] = new JsonArray(
                (JsonNode)new JsonObject { ["role"] = "user", ["content"] = prompt }),
            ["urgency"] = cli.Opt("--urgency") ?? "waitable",
            ["privacy"] = cli.Opt("--privacy") ?? "local_only",
        };
        if (cli.Opt("--capability") is { Length: > 0 } cap) body["capability"] = cap;
        if (cli.Opt("--task-class") is { Length: > 0 } tc) body["task_class"] = tc;
        if (cli.OptInt("--min-ability") is int ma) body["min_ability"] = ma;

        var baseUrl = cli.OptOrEnv("--server", "CBK_SERVER_URL", "http://localhost:8000");

        using var http = WorkerConfig.AdminHttp();
        using var content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        using var resp = await http.PostAsync(baseUrl.TrimEnd('/') + "/jobs", content);
        var text = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            // A 422 here is often the router refusing to under-serve min_ability (ADR 16).
            Out.Error($"{(int)resp.StatusCode} {text}");
            return 1;
        }

        var node = JsonNode.Parse(text)!;
        Out.Good($"queued id={node["id"]} result_key={node["result_key"]}");
        return 0;
    }
}
