using System.ComponentModel;
using Spectre.Console;
using Spectre.Console.Cli;
using StackExchange.Redis;

namespace Clusterbuck.Worker.Commands;

/// <summary>`cbk work` — the resident worker loop.</summary>
public sealed class WorkCommand : AsyncCommand<WorkCommand.Settings>
{
    public sealed class Settings : CommandSettings
    {
        [CommandOption("-c|--capabilities <LIST>")]
        [Description("Comma-separated capability tiers to serve (overrides CBK_CAPABILITIES).")]
        public string? Capabilities { get; init; }

        [CommandOption("--model <NAME>")]
        [Description("Model name to pass to the local model server (overrides CBK_MODEL).")]
        public string? Model { get; init; }
    }

    public override async Task<int> ExecuteAsync(CommandContext context, Settings settings)
    {
        var cfg = WorkerConfig.FromEnvironment();
        if (settings.Capabilities is { Length: > 0 })
            cfg = cfg with { Capabilities = settings.Capabilities.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries) };
        if (settings.Model is { Length: > 0 })
            cfg = cfg with { ModelName = settings.Model };

        // Enrolled mode (M4b): if node identity exists, take the id + ladder from it and
        // heartbeat. Otherwise the worker stays purely env-configured (M0-M3 behaviour).
        var statePath = NodeStateStore.DefaultPath;
        var state = NodeStateStore.Load(statePath);
        PresenceLadder? ladder = null;
        if (state is not null)
        {
            ladder = new PresenceLadder(state.Ladder, state.Capabilities, cfg.LadderHysteresisS);
            ladder.Update(state.Mode);
            cfg = cfg with
            {
                WorkerId = state.NodeId,
                Capabilities = ladder.Capabilities().ToArray(),
            };
        }

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        var mux = await ConnectionMultiplexer.ConnectAsync(cfg.RedisConfig);
        var db = mux.GetDatabase(cfg.Database);
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(10) };
        using var hbHttp = new HttpClient();
        var model = new ModelClient(http, cfg);
        var loop = new WorkLoop(db, model, cfg, s => AnsiConsole.MarkupLineInterpolated($"[grey]{s}[/]"));

        var inventory = new ModelInventory(hbHttp, cfg.ModelServerUrl, cfg.ModelManager);
        // Model pulls can take minutes; give them their own generous client.
        using var mgrHttp = new HttpClient { Timeout = TimeSpan.FromHours(1) };
        var manager = new ModelManager(mgrHttp, inventory.NativeBase, cfg.ModelManager);

        Task? heartbeat = null;
        if (state is not null && ladder is not null)
            heartbeat = HeartbeatLoop(new RegistryClient(hbHttp, state.Server), loop, ladder,
                                      inventory, manager, statePath, cfg, cts.Token);

        try
        {
            await loop.RunAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            if (heartbeat is not null) { try { await heartbeat; } catch { } }
            await mux.CloseAsync();
        }
        return 0;
    }

    /// <summary>
    /// Apply the coordinator's fitness verdict. Returns true when this worker must not claim
    /// jobs. Enforcement is COOPERATIVE: workers claim straight from Redis, so the
    /// coordinator cannot hard-block one that ignores this — see ADR 27.
    /// </summary>
    private static bool ApplyFitness(Fitness? fitness, Action<string> log)
    {
        if (fitness is null) return false;
        switch (fitness.Status)
        {
            case "quarantine":
                log($"QUARANTINED by coordinator — not claiming jobs: {fitness.Reason}. " +
                    $"Update to {fitness.CurrentVersion ?? "the current release"} " +
                    $"(this agent is {WorkerConfig.AgentVersion}).");
                return true;
            case "stale":
                log($"version {WorkerConfig.AgentVersion} is behind " +
                    $"{fitness.CurrentVersion ?? "current"}: {fitness.Reason}. Still serving.");
                return false;
            default:
                return false;
        }
    }

    /// <summary>Periodic heartbeat: re-read the persisted mode, drive the ladder (which
    /// resubscribes capabilities and toggles pause), take a model inventory, and report to
    /// the coordinator.</summary>
    private static async Task HeartbeatLoop(
        RegistryClient registry, WorkLoop loop, PresenceLadder ladder,
        ModelInventory inventory, ModelManager manager, string statePath, WorkerConfig cfg,
        CancellationToken ct)
    {
        ActionResult? pendingResult = null;
        var quarantined = false;   // reported on the next beat, then cleared
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var s = NodeStateStore.Load(statePath);
                if (s is not null)
                {
                    var effective = ladder.Update(s.Mode);
                    loop.Paused = effective == "paused";
                    await loop.SetCapabilitiesAsync(ladder.Capabilities());
                    var caps = ladder.Capabilities();
                    // Observed reality, not configuration: what this node's model server
                    // actually has, and what is warm right now.
                    var installed = await inventory.InstalledAsync(ct);
                    var loaded = await inventory.LoadedAsync(ct);
                    var digests = await inventory.DigestsAsync(ct);
                    var resp = await registry.HeartbeatAsync(s.NodeId, s.NodeKey,
                        new HeartbeatRequest
                        {
                            Mode = effective,
                            Installed = installed,
                            Loaded = loaded,
                            Digests = digests.Count > 0 ? digests : null,
                            Queues = caps.Select(c => WorkLoop.StreamKey(c)).ToList(),
                            ProtocolVersion = UpdateVerifier.ProtocolVersion,
                            AgentVersion = WorkerConfig.AgentVersion,
                            ActionResult = pendingResult,
                        }, ct);
                    pendingResult = null;   // reported; don't repeat it

                    // The coordinator judges whether this build is fit to run jobs. A
                    // quarantined worker stops claiming: a version with known-bad behaviour
                    // producing plausible-looking wrong results is worse than an idle node.
                    quarantined = ApplyFitness(resp.Fitness, s2 =>
                        AnsiConsole.MarkupLineInterpolated($"[yellow]{s2}[/]"));
                    loop.Paused = quarantined || effective == "paused";

                    // Execute an approved model-management action, if one was issued. The
                    // worker never decides this — it only carries out an approved proposal.
                    // A quarantined worker installs nothing.
                    if (!quarantined && resp.Action is { } action)
                        pendingResult = await ExecuteActionAsync(manager, action, ct);
                }
            }
            catch (OperationCanceledException) { break; }
            catch { /* transient; retry next beat */ }

            try { await Task.Delay(cfg.HeartbeatMs, ct); }
            catch (OperationCanceledException) { break; }
        }
    }

    /// <summary>Carry out one approved install/remove and report the outcome.</summary>
    private static async Task<ActionResult> ExecuteActionAsync(
        ModelManager manager, ModelAction action, CancellationToken ct)
    {
        AnsiConsole.MarkupLineInterpolated(
            $"[grey]model {action.Kind}: {action.Artifact} (proposal {action.ProposalId})[/]");
        var (ok, error) = action.Kind switch
        {
            "install" => await manager.InstallAsync(action.RegistryRef ?? action.Artifact, ct),
            "remove" => await manager.RemoveAsync(action.Artifact, ct),
            _ => (false, $"unknown action kind '{action.Kind}'"),
        };
        if (ok)
            AnsiConsole.MarkupLineInterpolated($"[green]model {action.Kind} ok:[/] {action.Artifact}");
        else
            AnsiConsole.MarkupLineInterpolated($"[red]model {action.Kind} failed:[/] {error}");
        return new ActionResult { ProposalId = action.ProposalId, Ok = ok, Error = error };
    }
}
