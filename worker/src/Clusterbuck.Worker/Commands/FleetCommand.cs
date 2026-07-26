using System.ComponentModel;
using System.Text.Json.Nodes;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Clusterbuck.Worker.Commands;

/// <summary>`cbk fleet` — list the server's capability/node registry (GET /fleet).</summary>
public sealed class FleetCommand : AsyncCommand<FleetCommand.Settings>
{
    public sealed class Settings : CommandSettings
    {
        [CommandOption("--server <URL>")]
        [Description("Server base URL (default CBK_SERVER_URL or http://localhost:8000).")]
        public string? Server { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
    {
        var baseUrl = settings.Server
            ?? Environment.GetEnvironmentVariable("CBK_SERVER_URL")
            ?? "http://localhost:8000";

        using var http = WorkerConfig.AdminHttp();
        using var resp = await http.GetAsync($"{baseUrl.TrimEnd('/')}/fleet");
        var text = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            AnsiConsole.MarkupLineInterpolated($"[red]{(int)resp.StatusCode}[/] {text}");
            return 1;
        }

        var root = JsonNode.Parse(text)!;
        var caps = root["capabilities"]?.AsObject();
        if (caps is null || caps.Count == 0)
        {
            AnsiConsole.MarkupLine("[yellow]no fleet configured (no fleet.yaml on the server)[/]");
            return 0;
        }

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("capability");
        table.AddColumn("queue");
        table.AddColumn("model");
        table.AddColumn("model_server");
        foreach (var (name, spec) in caps)
        {
            table.AddRow(
                name,
                spec?["queue"]?.ToString() ?? "",
                spec?["model"]?.ToString() ?? "",
                spec?["model_server"]?.ToString() ?? "");
        }
        AnsiConsole.Write(table);

        var nodes = root["nodes"]?.AsArray();
        if (nodes is { Count: > 0 })
        {
            var nt = new Table().Border(TableBorder.Rounded);
            nt.AddColumn("node");
            nt.AddColumn("wake");
            nt.AddColumn("capabilities");
            foreach (var n in nodes)
            {
                var capList = n?["capabilities"]?.AsArray();
                var caps2 = capList is null ? "" : string.Join(", ", capList.Select(x => x?.ToString()));
                nt.AddRow(n?["id"]?.ToString() ?? "", n?["wake"]?.ToString() ?? "", caps2);
            }
            AnsiConsole.Write(nt);
        }
        return 0;
    }
}
