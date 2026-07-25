using System.Text.Json;
using System.Text.Json.Nodes;
using Clusterbuck.Worker;
using Json.Schema;
using Xunit;

namespace Clusterbuck.Worker.Tests;

/// <summary>
/// C#-side contract conformance (ADR 22), the mirror of the Python test_contract.py: the
/// same shared JSON Schema in contract/ must accept the valid fixtures, reject the invalid
/// ones, and accept what the worker's own source-generated types serialize. If either
/// side's types drift from the schema, that side's conformance test fails.
///
/// The full worker↔Redis↔model integration is proven by the end-to-end script
/// (deploy/e2e), which needs live infra; these tests stay infra-free and fast.
/// </summary>
public sealed class ContractConformanceTests
{
    private static readonly string ContractDir = FindContractDir();

    private static string FindContractDir()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "contract", "job.schema.json");
            if (File.Exists(candidate)) return Path.Combine(dir.FullName, "contract");
            dir = dir.Parent;
        }
        throw new DirectoryNotFoundException("could not locate contract/ above the test assembly");
    }

    private static JsonSchema Schema(string name) =>
        JsonSchema.FromText(File.ReadAllText(Path.Combine(ContractDir, name)));

    private static JsonNode Fixture(string name) =>
        JsonNode.Parse(File.ReadAllText(Path.Combine(ContractDir, "examples", name)))!;

    [Fact]
    public void JobSchema_Accepts_Valid() =>
        Assert.True(Schema("job.schema.json").Evaluate(Fixture("job.valid.json")).IsValid);

    [Fact]
    public void JobSchema_Rejects_Invalid() =>
        Assert.False(Schema("job.schema.json").Evaluate(Fixture("job.invalid.json")).IsValid);

    [Fact]
    public void ResultSchema_Accepts_Valid() =>
        Assert.True(Schema("result.schema.json").Evaluate(Fixture("result.valid.json")).IsValid);

    [Fact]
    public void ResultSchema_Rejects_Invalid() =>
        Assert.False(Schema("result.schema.json").Evaluate(Fixture("result.invalid.json")).IsValid);

    [Fact]
    public void UpdateManifestSchema_Accepts_Fixture() =>
        Assert.True(Schema("update-manifest.schema.json").Evaluate(Fixture("update-manifest.valid.json")).IsValid);

    [Fact]
    public void Worker_Job_RoundTrips_Against_Schema()
    {
        // Deserialize the shared fixture into the worker's own type, re-serialize, and it
        // must still satisfy the schema — proving the C# Job type agrees with the contract.
        var job = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(ContractDir, "examples", "job.valid.json")),
            CbkJsonContext.Default.Job)!;
        var wire = JsonSerializer.Serialize(job, CbkJsonContext.Default.Job);
        Assert.True(Schema("job.schema.json").Evaluate(JsonNode.Parse(wire)).IsValid);
    }

    [Fact]
    public void Worker_EnrollRequest_Serializes_Schema_Valid()
    {
        var req = new EnrollRequest
        {
            JoinToken = "jt_x", Hostname = "h", Os = "darwin", Arch = "arm64",
            Hw = new HwProbe { RamGb = 64, Accelerator = "metal", DiskFreeGb = 512 },
            Profile = "shared",
        };
        var wire = JsonSerializer.Serialize(req, CbkJsonContext.Default.EnrollRequest);
        Assert.True(Schema("enroll-request.schema.json").Evaluate(JsonNode.Parse(wire)).IsValid);
    }

    [Fact]
    public void Worker_Parses_EnrollResponse_Fixture()
    {
        var resp = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(ContractDir, "examples", "enroll-response.valid.json")),
            CbkJsonContext.Default.EnrollResponse)!;
        Assert.Equal("node-a7f3", resp.NodeId);
        Assert.Contains("70b-reason", resp.Proposed.Capabilities);
        Assert.NotNull(resp.Proposed.Ladder);
    }

    [Fact]
    public void Worker_HeartbeatRequest_Serializes_Schema_Valid()
    {
        var hb = new HeartbeatRequest
        {
            Mode = "away", Installed = new() { "m" }, Loaded = new() { "m" },
            Digests = new() { ["m"] = "sha256:abc" },
            Queues = new() { "q:8b-extract" },
            Stats = new HeartbeatStats { JobsDone = 1, Tps = 10 }, ProtocolVersion = 1,
            ActionResult = new ActionResult { ProposalId = "prop_1", Ok = true },
        };
        var wire = JsonSerializer.Serialize(hb, CbkJsonContext.Default.HeartbeatRequest);
        Assert.True(Schema("heartbeat-request.schema.json").Evaluate(JsonNode.Parse(wire)).IsValid);
    }

    [Fact]
    public void Worker_Parses_HeartbeatResponse_Action()
    {
        // The coordinator's approved-action instruction must round-trip into the worker's
        // type — this is what drives M6c's install/remove execution.
        var resp = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(ContractDir, "examples", "heartbeat-response.valid.json")),
            CbkJsonContext.Default.HeartbeatResponse)!;
        Assert.NotNull(resp.Action);
        Assert.Equal("install", resp.Action!.Kind);
        Assert.Equal("qwen2.5:14b", resp.Action.Artifact);
        Assert.Equal("ollama", resp.Action.Source);
        Assert.NotEmpty(resp.PlannerNotes);
    }

    [Fact]
    public void Worker_Result_Serializes_Schema_Valid()
    {
        // A "done" Result the worker would write must satisfy the schema.
        using var completion = JsonDocument.Parse(
            """{"choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hi"}}]}""");
        var result = new Result
        {
            JobId = "job_abc",
            Status = "done",
            Worker = "node-test",
            CompletedAt = "2026-07-24T18:30:04Z",
            Completion = completion.RootElement.Clone(),
        };
        var wire = JsonSerializer.Serialize(result, CbkJsonContext.Default.Result);
        Assert.True(Schema("result.schema.json").Evaluate(JsonNode.Parse(wire)).IsValid);
    }
}
