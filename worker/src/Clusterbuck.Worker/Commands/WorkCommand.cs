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

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        var mux = await ConnectionMultiplexer.ConnectAsync(cfg.RedisConfig);
        var db = mux.GetDatabase(cfg.Database);
        using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(10) };
        var model = new ModelClient(http, cfg);
        var loop = new WorkLoop(db, model, cfg, s => AnsiConsole.MarkupLineInterpolated($"[grey]{s}[/]"));

        try
        {
            await loop.RunAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            await mux.CloseAsync();
        }
        return 0;
    }
}
