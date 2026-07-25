using System.ComponentModel;
using Spectre.Console;
using Spectre.Console.Cli;

namespace Clusterbuck.Worker.Commands;

/// <summary>
/// `cbk pause` / `cbk resume` — the owner's fast-eviction control (ADR 10). Flips the
/// persisted presence mode; a running `cbk work` reads it each heartbeat and stops/starts
/// pulling within seconds. `cbk pause` sets `paused`; `cbk resume` sets `active`.
/// </summary>
public abstract class ModeCommandBase : AsyncCommand<ModeCommandBase.Settings>
{
    public sealed class Settings : CommandSettings
    {
        [CommandOption("--state <PATH>")]
        [Description("Node state path (default CBK_NODE_STATE or ~/.clusterbuck/node.json).")]
        public string? StatePath { get; init; }
    }

    protected abstract string Mode { get; }

    public override Task<int> ExecuteAsync(CommandContext context, Settings settings)
    {
        var path = settings.StatePath ?? NodeStateStore.DefaultPath;
        var state = NodeStateStore.Load(path);
        if (state is null)
        {
            AnsiConsole.MarkupLine("[red]not enrolled[/] — run `cbk enroll` first");
            return Task.FromResult(1);
        }
        NodeStateStore.Save(path, state with { Mode = Mode });
        AnsiConsole.MarkupLineInterpolated($"[green]mode → {Mode}[/] for {state.NodeId}");
        return Task.FromResult(0);
    }
}

public sealed class PauseCommand : ModeCommandBase
{
    protected override string Mode => "paused";
}

public sealed class ResumeCommand : ModeCommandBase
{
    protected override string Mode => "active";
}
