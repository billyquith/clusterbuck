using System.ComponentModel;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Clusterbuck.Worker.Commands;

/// <summary>`cbk enroll` — probe the hardware, join with a one-time token, persist identity.</summary>
public sealed class EnrollCommand : AsyncCommand<EnrollCommand.Settings>
{
    public sealed class Settings : CommandSettings
    {
        [CommandOption("-t|--token <TOKEN>")]
        [Description("One-time join token minted by the coordinator admin.")]
        public string? Token { get; init; }

        [CommandOption("--server <URL>")]
        [Description("Coordinator base URL (default CBK_SERVER_URL or http://localhost:8000).")]
        public string? Server { get; init; }

        [CommandOption("--profile <PROFILE>")]
        [Description("dedicated | shared | background (default shared).")]
        public string Profile { get; init; } = "shared";

        [CommandOption("--state <PATH>")]
        [Description("Where to persist node identity (default CBK_NODE_STATE or ~/.clusterbuck/node.json).")]
        public string? StatePath { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Token))
        {
            AnsiConsole.MarkupLine("[red]--token is required[/]");
            return 1;
        }
        var server = settings.Server
            ?? Environment.GetEnvironmentVariable("CBK_SERVER_URL") ?? "http://localhost:8000";
        var statePath = settings.StatePath ?? NodeStateStore.DefaultPath;

        var req = HardwareProbe.Build(settings.Token, settings.Profile);
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]probed[/] ram={req.Hw.RamGb}GB accel={req.Hw.Accelerator} disk={req.Hw.DiskFreeGb}GB arch={req.Arch}");

        using var http = new HttpClient();
        var registry = new RegistryClient(http, server);
        EnrollResponse resp;
        try
        {
            resp = await registry.EnrollAsync(req);
        }
        catch (Exception e)
        {
            AnsiConsole.MarkupLineInterpolated($"[red]enroll failed:[/] {e.Message}");
            return 1;
        }

        NodeStateStore.Save(statePath, new NodeState
        {
            NodeId = resp.NodeId,
            NodeKey = resp.NodeKey,
            Server = server,
            Capabilities = resp.Proposed.Capabilities,
            Ladder = resp.Proposed.Ladder,
            Mode = "active",
        });
        AnsiConsole.MarkupLineInterpolated(
            $"[green]enrolled[/] as [bold]{resp.NodeId}[/] · capabilities: {string.Join(", ", resp.Proposed.Capabilities)}");
        AnsiConsole.MarkupLineInterpolated($"[grey]identity saved to {statePath}[/]");
        return 0;
    }
}
