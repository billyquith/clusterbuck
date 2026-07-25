using System.ComponentModel;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Clusterbuck.Worker.Commands;

/// <summary>`cbk submit` — a thin client over the server's POST /jobs (protocols.md §1b).</summary>
public sealed class SubmitCommand : AsyncCommand<SubmitCommand.Settings>
{
    public sealed class Settings : CommandSettings
    {
        [CommandOption("--server <URL>")]
        [Description("Server base URL (default CBK_SERVER_URL or http://localhost:8000).")]
        public string? Server { get; init; }

        [CommandOption("-p|--prompt <TEXT>")]
        [Description("Prompt text.")]
        public string? Prompt { get; init; }

        [CommandOption("--capability <TIER>")]
        [Description("Explicit capability tier (advanced addressing).")]
        public string? Capability { get; init; }

        [CommandOption("--task-class <NAME>")]
        [Description("Task class (need-shaped addressing; pair with --min-ability).")]
        public string? TaskClass { get; init; }

        [CommandOption("--min-ability <N>")]
        [Description("Minimum ability 1-10 (need-shaped addressing).")]
        public int? MinAbility { get; init; }

        [CommandOption("--urgency <CLASS>")]
        [Description("urgent | necessary | waitable (default waitable).")]
        public string Urgency { get; init; } = "waitable";

        [CommandOption("--privacy <CLASS>")]
        [Description("local_only | cloud_ok (default local_only).")]
        public string Privacy { get; init; } = "local_only";
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Prompt))
        {
            AnsiConsole.MarkupLine("[red]--prompt is required[/]");
            return 1;
        }

        var body = new JsonObject
        {
            ["messages"] = new JsonArray(new JsonObject { ["role"] = "user", ["content"] = settings.Prompt }),
            ["urgency"] = settings.Urgency,
            ["privacy"] = settings.Privacy,
        };
        if (settings.Capability is { Length: > 0 }) body["capability"] = settings.Capability;
        if (settings.TaskClass is { Length: > 0 }) body["task_class"] = settings.TaskClass;
        if (settings.MinAbility is int a) body["min_ability"] = a;

        var baseUrl = settings.Server
            ?? Environment.GetEnvironmentVariable("CBK_SERVER_URL")
            ?? "http://localhost:8000";

        using var http = new HttpClient();
        using var content = new StringContent(body.ToJsonString(), Encoding.UTF8, "application/json");
        using var resp = await http.PostAsync(baseUrl.TrimEnd('/') + "/jobs", content);
        var text = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            AnsiConsole.MarkupLineInterpolated($"[red]{(int)resp.StatusCode}[/] {text}");
            return 1;
        }

        var node = JsonNode.Parse(text)!;
        AnsiConsole.MarkupLineInterpolated($"[green]queued[/] id=[bold]{node["id"]}[/] result_key={node["result_key"]}");
        return 0;
    }
}
