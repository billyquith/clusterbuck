using System.Text.Json;
using StackExchange.Redis;

namespace Clusterbuck.Worker;

/// <summary>
/// The pull-based worker loop (protocols.md §2, ADR 2/20). Reads jobs from each
/// capability stream under the shared consumer group, runs them on the local model
/// server, writes the terminal result to the result store, and acknowledges.
///
/// StackExchange.Redis does not expose blocking reads, so the loop polls with a short
/// delay; a truly idle-cheap block (implementation.md footprint note) is a later refinement.
/// A job abandoned mid-run (worker died before XACK) is recovered by the coordinator's
/// XAUTOCLAIM reaper (server/clusterbuck/reaper.py); the worker acks on success and failure.
/// </summary>
public sealed class WorkLoop
{
    private readonly IDatabase _db;
    private readonly ModelClient _model;
    private readonly WorkerConfig _cfg;
    private readonly Action<string> _log;
    // Mutable so the presence ladder can resubscribe live (active ⇄ away).
    private volatile IReadOnlyList<string> _capabilities;

    public WorkLoop(IDatabase db, ModelClient model, WorkerConfig cfg, Action<string>? log = null)
    {
        _db = db;
        _model = model;
        _cfg = cfg;
        _log = log ?? Console.WriteLine;
        _capabilities = cfg.Capabilities;
    }

    public IReadOnlyList<string> Capabilities => _capabilities;

    /// <summary>Swap the served capability set (ensuring groups for any new ones).</summary>
    public async Task SetCapabilitiesAsync(IReadOnlyList<string> caps)
    {
        foreach (var cap in caps)
            await EnsureGroupAsync(cap);
        _capabilities = caps;
    }

    /// <summary>When true the loop stops pulling (owner eviction / paused presence mode).
    /// In-flight jobs already claimed still finish; nothing new is claimed.</summary>
    public volatile bool Paused;

    public static string StreamKey(string capability) => $"q:{capability}";

    /// <summary>Create the consumer group (and stream) for each capability. Idempotent.</summary>
    public async Task EnsureGroupsAsync()
    {
        foreach (var cap in _capabilities)
            await EnsureGroupAsync(cap);
    }

    private async Task EnsureGroupAsync(string cap)
    {
        try
        {
            await _db.StreamCreateConsumerGroupAsync(
                StreamKey(cap), _cfg.ConsumerGroup, StreamPosition.NewMessages,
                createStream: true);
        }
        catch (RedisServerException e) when (e.Message.Contains("BUSYGROUP"))
        {
            // group already exists — fine
        }
    }

    /// <summary>Read at most one job per capability and process it. Returns true if it did work.</summary>
    public async Task<bool> PollOnceAsync(CancellationToken ct)
    {
        if (Paused) return false;
        var didWork = false;
        foreach (var cap in _capabilities)
        {
            if (ct.IsCancellationRequested) break;
            var entries = await _db.StreamReadGroupAsync(
                StreamKey(cap), _cfg.ConsumerGroup, _cfg.WorkerId,
                position: ">", count: 1);
            foreach (var entry in entries)
            {
                didWork = true;
                await ProcessAsync(cap, entry, ct);
            }
        }
        return didWork;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        await EnsureGroupsAsync();
        _log($"cbk worker {_cfg.WorkerId} serving [{string.Join(", ", _capabilities)}] " +
             $"→ model {_cfg.ModelName} @ {_cfg.ModelServerUrl}");
        while (!ct.IsCancellationRequested)
        {
            bool didWork;
            try
            {
                didWork = await PollOnceAsync(ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            if (!didWork)
                await Task.Delay(_cfg.PollMs, ct).ContinueWith(_ => { }, TaskScheduler.Default);
        }
    }

    private async Task ProcessAsync(string capability, StreamEntry entry, CancellationToken ct)
    {
        var raw = (string?)entry["job"];
        if (raw is null)
        {
            await _db.StreamAcknowledgeAsync(StreamKey(capability), _cfg.ConsumerGroup, entry.Id);
            return;
        }

        var job = JsonSerializer.Deserialize(raw, CbkJsonContext.Default.Job)!;
        var now = DateTime.UtcNow.ToString("o");

        Result result;
        try
        {
            var (completion, usage) = await _model.CompleteAsync(job, ct);
            result = new Result
            {
                JobId = job.Id,
                Status = "done",
                Worker = _cfg.WorkerId,
                CompletedAt = now,
                Completion = completion,
                Usage = usage,
            };
            _log($"done  {job.Id} [{capability}]");
        }
        catch (Exception e)
        {
            result = new Result
            {
                JobId = job.Id,
                Status = "failed",
                Worker = _cfg.WorkerId,
                CompletedAt = now,
                Error = e.Message,
            };
            _log($"fail  {job.Id} [{capability}]: {e.Message}");
        }

        var json = JsonSerializer.Serialize(result, CbkJsonContext.Default.Result);
        await _db.StringSetAsync(job.ResultKey, json, TimeSpan.FromSeconds(_cfg.ResultTtlSeconds));
        await _db.StreamAcknowledgeAsync(StreamKey(capability), _cfg.ConsumerGroup, entry.Id);
    }
}
