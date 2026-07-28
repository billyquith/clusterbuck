using System.Text.Json;
using System.Text.Json.Serialization;

namespace Clusterbuck.Worker;

/// <summary>An OpenAI-style chat message (contract/job.schema.json).</summary>
public sealed record Message
{
    [JsonPropertyName("role")] public string Role { get; init; } = "";
    [JsonPropertyName("content")] public string Content { get; init; } = "";
}

/// <summary>
/// The enqueued job record the worker consumes from a capability stream
/// (contract/job.schema.json). Deserialize-only on the worker side.
/// </summary>
public sealed record Job
{
    [JsonPropertyName("id")] public string Id { get; init; } = "";
    [JsonPropertyName("created_at")] public string CreatedAt { get; init; } = "";
    [JsonPropertyName("capability")] public string Capability { get; init; } = "";
    [JsonPropertyName("messages")] public List<Message>? Messages { get; init; }
    [JsonPropertyName("prompt")] public string? Prompt { get; init; }
    [JsonPropertyName("params")] public JsonElement Params { get; init; }
    [JsonPropertyName("urgency")] public string Urgency { get; init; } = "";
    [JsonPropertyName("escalate_after_min")] public int? EscalateAfterMin { get; init; }
    [JsonPropertyName("privacy")] public string Privacy { get; init; } = "";
    [JsonPropertyName("deadline")] public string? Deadline { get; init; }
    [JsonPropertyName("result_key")] public string ResultKey { get; init; } = "";
    [JsonPropertyName("attempts")] public int Attempts { get; init; }
    [JsonPropertyName("max_attempts")] public int MaxAttempts { get; init; }
}

/// <summary>
/// The terminal result record the worker writes to the result store
/// (contract/result.schema.json). Serialize-only on the worker side.
/// </summary>
public sealed record Result
{
    [JsonPropertyName("job_id")] public string JobId { get; init; } = "";
    [JsonPropertyName("status")] public string Status { get; init; } = "";
    [JsonPropertyName("worker")] public string Worker { get; init; } = "";
    [JsonPropertyName("completed_at")] public string CompletedAt { get; init; } = "";
    [JsonPropertyName("completion")] public JsonElement? Completion { get; init; }
    [JsonPropertyName("error")] public string? Error { get; init; }
    [JsonPropertyName("usage")] public JsonElement? Usage { get; init; }
}

/// <summary>
/// Source-generated JSON (AOT-friendly, per implementation.md). Nulls are omitted so a
/// "done" result carries a real completion object and no stray nulls; the shared schema
/// permits absent optionals.
/// </summary>
[JsonSourceGenerationOptions(DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(Job))]
[JsonSerializable(typeof(Result))]
[JsonSerializable(typeof(Message))]
[JsonSerializable(typeof(EnrollRequest))]
[JsonSerializable(typeof(EnrollResponse))]
[JsonSerializable(typeof(HeartbeatRequest))]
[JsonSerializable(typeof(HeartbeatResponse))]
[JsonSerializable(typeof(NodeState))]
[JsonSerializable(typeof(UpdateManifest))]
[JsonSerializable(typeof(Fitness))]
public partial class CbkJsonContext : JsonSerializerContext
{
}
