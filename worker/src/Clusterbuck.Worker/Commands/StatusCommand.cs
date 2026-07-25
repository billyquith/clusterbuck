using System.ComponentModel;
using System.Text.Json.Nodes;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Clusterbuck.Worker.Commands;

/// <summary>`cbk status &lt;id&gt;` — poll a job via the server's GET /jobs/{id}.</summary>
public sealed class StatusCommand : AsyncCommand<StatusCommand.Settings>
{
    public sealed class Settings : CommandSettings
    {
        [CommandArgument(0, "<job_id>")]
        [Description("The job id returned by submit.")]
        public string JobId { get; init; } = "";

        [CommandOption("--server <URL>")]
        [Description("Server base URL (default CBK_SERVER_URL or http://localhost:8000).")]
        public string? Server { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
    {
        var baseUrl = settings.Server
            ?? Environment.GetEnvironmentVariable("CBK_SERVER_URL")
            ?? "http://localhost:8000";

        using var http = new HttpClient();
        using var resp = await http.GetAsync($"{baseUrl.TrimEnd('/')}/jobs/{settings.JobId}");
        var text = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            AnsiConsole.MarkupLineInterpolated($"[red]{(int)resp.StatusCode}[/] {text}");
            return 1;
        }

        var node = JsonNode.Parse(text)!;
        var status = node["status"]?.ToString();
        AnsiConsole.MarkupLineInterpolated($"status=[bold]{status}[/]");
        var result = node["result"];
        if (result is not null)
        {
            var content = result["choices"]?[0]?["message"]?["content"]?.ToString();
            if (content is not null)
                AnsiConsole.WriteLine(content);
        }
        if (node["error"] is JsonNode err && err.GetValueKind() != System.Text.Json.JsonValueKind.Null)
            AnsiConsole.MarkupLineInterpolated($"[red]error:[/] {err}");
        return 0;
    }
}
