using System.Reflection;

namespace Clusterbuck.Worker;

/// <summary>Worker configuration, from environment with LAN-dev defaults.</summary>
public sealed record WorkerConfig
{
    public string RedisConfig { get; init; } = "localhost:6379";
    public int Database { get; init; } = 0;
    public string ConsumerGroup { get; init; } = "cbk-workers";
    public string WorkerId { get; init; } = "node-" + Guid.NewGuid().ToString("N")[..6];
    public string ModelServerUrl { get; init; } = "http://localhost:11434/v1";
    public string ModelName { get; init; } = "llama3.2:3b";
    public IReadOnlyList<string> Capabilities { get; init; } = new[] { "8b-extract" };
    public int PollMs { get; init; } = 1000;
    public int ResultTtlSeconds { get; init; } = 86400;
    public int HeartbeatMs { get; init; } = 10000;
    /// <summary>Which model-manager adapter to use for the non-OpenAI bits (loaded state,
    /// digests, installs): auto | ollama | none. `none` = discovery via /v1/models only.</summary>
    public string ModelManager { get; init; } = "auto";
    /// <summary>How long the owner must be stably away before the ladder climbs to the big
    /// models (ADR 10). Cold loads are expensive, so this is damped by default; tune it per
    /// node (a dedicated box wants it near zero, a laptop wants minutes).</summary>
    public double LadderHysteresisS { get; init; } = 120;

    /// <summary>
    /// This agent's version, read from the assembly's build-time stamp (csproj `&lt;Version&gt;`).
    /// The coordinator uses it to decide whether this worker is fit to run jobs — a version can
    /// be protocol-compatible yet carry bugs that produce wrong results, so "suitable" is a
    /// stronger question than "can it speak the contract".
    /// </summary>
    public static string AgentVersion =>
        System.Reflection.Assembly.GetExecutingAssembly()
            .GetCustomAttribute<System.Reflection.AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion.Split('+')[0]          // strip any +buildmetadata
        ?? System.Reflection.Assembly.GetExecutingAssembly().GetName().Version?.ToString(3)
        ?? "0.0.0";

    /// <summary>Operator shared secret for the coordinator API (CBK_API_KEY). Needed by the
    /// admin CLI verbs; the worker loop itself does not use it, because enroll is
    /// join-token authenticated and heartbeat is node-key authenticated.</summary>
    public static string? ApiKey => Environment.GetEnvironmentVariable("CBK_API_KEY");

    /// <summary>An HttpClient that presents the operator key when one is configured.</summary>
    public static HttpClient AdminHttp()
    {
        var http = new HttpClient();
        if (ApiKey is { Length: > 0 } key)
            http.DefaultRequestHeaders.Add("X-CBK-Api-Key", key);
        return http;
    }

    public static WorkerConfig FromEnvironment()
    {
        string? Env(string k) => Environment.GetEnvironmentVariable(k);

        var caps = Env("CBK_CAPABILITIES");
        var (redisConfig, db) = ParseRedis(Env("CBK_REDIS_URL") ?? "localhost:6379");
        return new WorkerConfig
        {
            RedisConfig = redisConfig,
            Database = db,
            ConsumerGroup = Env("CBK_CONSUMER_GROUP") ?? "cbk-workers",
            WorkerId = Env("CBK_WORKER_ID") ?? "node-" + Guid.NewGuid().ToString("N")[..6],
            ModelServerUrl = Env("CBK_MODEL_SERVER_URL") ?? "http://localhost:11434/v1",
            ModelName = Env("CBK_MODEL") ?? "llama3.2:3b",
            Capabilities = string.IsNullOrWhiteSpace(caps)
                ? new[] { "8b-extract" }
                : caps.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries),
            PollMs = int.TryParse(Env("CBK_POLL_MS"), out var p) ? p : 1000,
            ResultTtlSeconds = int.TryParse(Env("CBK_RESULT_TTL_S"), out var t) ? t : 86400,
            HeartbeatMs = int.TryParse(Env("CBK_HEARTBEAT_MS"), out var h) ? h : 10000,
            ModelManager = Env("CBK_MODEL_MANAGER") ?? "auto",
            LadderHysteresisS = double.TryParse(Env("CBK_LADDER_HYSTERESIS_S"), out var lh) ? lh : 120,
        };
    }

    /// <summary>
    /// Accept both a bare host:port and a redis:// URL (the Python side's norm), since
    /// StackExchange.Redis parses host:port and selects the db at GetDatabase(n), not via
    /// the config string. Returns (host:port, dbIndex).
    /// </summary>
    public static (string config, int db) ParseRedis(string value)
    {
        if (!value.Contains("://")) return (value, 0);
        var uri = new Uri(value);
        var port = uri.Port > 0 ? uri.Port : 6379;
        var path = uri.AbsolutePath.Trim('/');
        var db = int.TryParse(path, out var n) ? n : 0;
        var config = $"{uri.Host}:{port}";
        // Carry a password through (redis://:secret@host or redis://user:secret@host).
        // Dropping it silently would make an authenticated broker look simply unreachable.
        if (!string.IsNullOrEmpty(uri.UserInfo))
        {
            var parts = uri.UserInfo.Split(':', 2);
            var password = Uri.UnescapeDataString(parts.Length == 2 ? parts[1] : parts[0]);
            if (password.Length > 0) config += $",password={password}";
            if (parts.Length == 2 && parts[0].Length > 0)
                config += $",user={Uri.UnescapeDataString(parts[0])}";
        }
        return (config, db);
    }
}
