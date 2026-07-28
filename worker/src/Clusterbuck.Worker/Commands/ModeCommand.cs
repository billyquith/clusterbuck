namespace Clusterbuck.Worker.Commands;

/// <summary>
/// `cbk pause` / `cbk resume` — the owner's fast-eviction control (ADR 10). Flips the
/// persisted presence mode; a running `cbk work` re-reads it each heartbeat and stops or
/// starts claiming within seconds.
/// </summary>
public static class ModeCommand
{
    public static int Run(CliArgs cli, string mode)
    {
        var path = cli.Opt("--state") ?? NodeStateStore.DefaultPath;
        var state = NodeStateStore.Load(path);
        if (state is null)
        {
            Out.Error("not enrolled — run `cbk enroll --token …` first");
            return 1;
        }
        NodeStateStore.Save(path, state with { Mode = mode });
        Out.Good($"mode → {mode} for {state.NodeId}");
        return 0;
    }
}
