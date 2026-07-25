using System.Text.Json;
using System.Text.Json.Serialization;

namespace Clusterbuck.Worker;

/// <summary>Hardware probe (contract/enroll-request.schema.json → hw).</summary>
public sealed record HwProbe
{
    [JsonPropertyName("ram_gb")] public double RamGb { get; init; }
    [JsonPropertyName("accelerator")] public string Accelerator { get; init; } = "cpu";
    [JsonPropertyName("vram_gb")] public double? VramGb { get; init; }
    [JsonPropertyName("disk_free_gb")] public double DiskFreeGb { get; init; }
    [JsonPropertyName("bench_tps_small")] public double? BenchTpsSmall { get; init; }
}

/// <summary>Enrollment request the worker produces (contract/enroll-request.schema.json).</summary>
public sealed record EnrollRequest
{
    [JsonPropertyName("join_token")] public string JoinToken { get; init; } = "";
    [JsonPropertyName("hostname")] public string Hostname { get; init; } = "";
    [JsonPropertyName("os")] public string Os { get; init; } = "";
    [JsonPropertyName("arch")] public string Arch { get; init; } = "";
    [JsonPropertyName("hw")] public HwProbe Hw { get; init; } = new();
    [JsonPropertyName("profile")] public string Profile { get; init; } = "shared";
}

public sealed record Proposed
{
    [JsonPropertyName("capabilities")] public List<string> Capabilities { get; init; } = new();
    [JsonPropertyName("ladder")] public Dictionary<string, List<string>>? Ladder { get; init; }
}

/// <summary>Enrollment response the worker parses (contract/enroll-response.schema.json).</summary>
public sealed record EnrollResponse
{
    [JsonPropertyName("node_id")] public string NodeId { get; init; } = "";
    [JsonPropertyName("node_key")] public string NodeKey { get; init; } = "";
    [JsonPropertyName("proposed")] public Proposed Proposed { get; init; } = new();
}

public sealed record HeartbeatStats
{
    [JsonPropertyName("jobs_done")] public int JobsDone { get; init; }
    [JsonPropertyName("tps")] public double Tps { get; init; }
}

/// <summary>Heartbeat the worker produces (contract/heartbeat-request.schema.json).</summary>
public sealed record HeartbeatRequest
{
    [JsonPropertyName("mode")] public string Mode { get; init; } = "active";
    [JsonPropertyName("installed")] public List<string> Installed { get; init; } = new();
    [JsonPropertyName("loaded")] public List<string> Loaded { get; init; } = new();
    [JsonPropertyName("digests")] public Dictionary<string, string>? Digests { get; init; }
    [JsonPropertyName("queues")] public List<string> Queues { get; init; } = new();
    [JsonPropertyName("stats")] public HeartbeatStats Stats { get; init; } = new();
    [JsonPropertyName("protocol_version")] public int? ProtocolVersion { get; init; }
    [JsonPropertyName("action_result")] public ActionResult? ActionResult { get; init; }
}

/// <summary>An approved model-management action issued by the coordinator.</summary>
public sealed record ModelAction
{
    [JsonPropertyName("proposal_id")] public string ProposalId { get; init; } = "";
    [JsonPropertyName("kind")] public string Kind { get; init; } = "";
    [JsonPropertyName("artifact")] public string Artifact { get; init; } = "";
    [JsonPropertyName("registry_ref")] public string? RegistryRef { get; init; }
    [JsonPropertyName("source")] public string Source { get; init; } = "";
}

/// <summary>Outcome the worker reports back on the next heartbeat.</summary>
public sealed record ActionResult
{
    [JsonPropertyName("proposal_id")] public string ProposalId { get; init; } = "";
    [JsonPropertyName("ok")] public bool Ok { get; init; }
    [JsonPropertyName("error")] public string? Error { get; init; }
}

/// <summary>Heartbeat response the worker parses (contract/heartbeat-response.schema.json).</summary>
public sealed record HeartbeatResponse
{
    [JsonPropertyName("update")] public JsonElement? Update { get; init; }
    [JsonPropertyName("action")] public ModelAction? Action { get; init; }
    [JsonPropertyName("planner_notes")] public List<string> PlannerNotes { get; init; } = new();
}

/// <summary>Persisted node identity + mode (survives restarts; written by enroll/pause).</summary>
public sealed record NodeState
{
    [JsonPropertyName("node_id")] public string NodeId { get; init; } = "";
    [JsonPropertyName("node_key")] public string NodeKey { get; init; } = "";
    [JsonPropertyName("server")] public string Server { get; init; } = "";
    [JsonPropertyName("capabilities")] public List<string> Capabilities { get; init; } = new();
    [JsonPropertyName("ladder")] public Dictionary<string, List<string>>? Ladder { get; init; }
    [JsonPropertyName("mode")] public string Mode { get; init; } = "active";
}
