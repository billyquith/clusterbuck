using System.Diagnostics;
using System.Net.Sockets;
using System.Text.Json;
using Clusterbuck.Worker;
using Xunit;

namespace Clusterbuck.Worker.Tests;

/// <summary>
/// The worker↔model-server call (protocols.md §3), including the **artifact pin**: a job may
/// carry `params.model` to override the worker's configured model, and the worker must
/// forward it. The eval harness depends on this — without it a measurement would be
/// attributed to whatever model the worker defaults to rather than the artifact under test.
/// Guarded here so it can't silently regress into an accident.
///
/// Verified through the real HTTP path against the repo's fake model server, which echoes
/// the model name it was asked for.
/// </summary>
public sealed class ModelClientTests : IAsyncLifetime
{
    private Process? _server;
    private int _port;
    private readonly HttpClient _http = new();

    private static int FreePort()
    {
        var l = new TcpListener(System.Net.IPAddress.Loopback, 0);
        l.Start();
        var p = ((System.Net.IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return p;
    }

    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "contract", "job.schema.json")))
                return dir.FullName;
            dir = dir.Parent;
        }
        throw new DirectoryNotFoundException("repo root not found");
    }

    public async Task InitializeAsync()
    {
        _port = FreePort();
        var script = Path.Combine(RepoRoot(), "server", "tools", "fake_model_server.py");
        _server = Process.Start(new ProcessStartInfo("python3", $"{script} --port {_port}")
        {
            RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false,
        });
        for (var i = 0; i < 50; i++)
        {
            try { await _http.GetAsync($"http://127.0.0.1:{_port}/healthz"); return; }
            catch (HttpRequestException) { await Task.Delay(100); }
        }
        throw new InvalidOperationException("fake model server did not start");
    }

    public Task DisposeAsync()
    {
        try { _server?.Kill(entireProcessTree: true); } catch { }
        _http.Dispose();
        return Task.CompletedTask;
    }

    private ModelClient Client(string configuredModel) => new(_http, new WorkerConfig
    {
        ModelServerUrl = $"http://127.0.0.1:{_port}/v1",
        ModelName = configuredModel,
    });

    private static Job JobWith(string paramsJson) => new()
    {
        Id = "job_x", CreatedAt = "t", Capability = "8b-extract",
        Messages = new() { new Message { Role = "user", Content = "hello" } },
        Params = JsonDocument.Parse(paramsJson).RootElement.Clone(),
        Urgency = "waitable", Privacy = "local_only", ResultKey = "res_x", MaxAttempts = 3,
    };

    [Fact]
    public async Task Uses_Configured_Model_By_Default()
    {
        var (completion, _) = await Client("configured:1b").CompleteAsync(JobWith("{}"), default);
        var text = completion.GetProperty("choices")[0].GetProperty("message")
            .GetProperty("content").GetString();
        Assert.Contains("configured:1b", text);
    }

    [Fact]
    public async Task Job_Params_Model_Pins_The_Artifact()
    {
        // The eval harness's correctness hinges on this override.
        var job = JobWith("""{"model": "under-test:7b", "temperature": 0.0}""");
        var (completion, _) = await Client("configured:1b").CompleteAsync(job, default);
        var text = completion.GetProperty("choices")[0].GetProperty("message")
            .GetProperty("content").GetString();
        Assert.Contains("under-test:7b", text);
        Assert.DoesNotContain("configured:1b", text);
    }

    [Fact]
    public async Task Usage_Is_Returned_When_Reported()
    {
        var (_, usage) = await Client("m").CompleteAsync(JobWith("{}"), default);
        Assert.NotNull(usage);
        Assert.True(usage!.Value.GetProperty("total_tokens").GetInt32() > 0);
    }

    [Fact]
    public async Task Prompt_Form_Is_Supported()
    {
        var job = new Job
        {
            Id = "job_p", CreatedAt = "t", Capability = "8b-extract",
            Prompt = "plain prompt form",
            Params = JsonDocument.Parse("{}").RootElement.Clone(),
            Urgency = "waitable", Privacy = "local_only", ResultKey = "res_p", MaxAttempts = 3,
        };
        var (completion, _) = await Client("m").CompleteAsync(job, default);
        var text = completion.GetProperty("choices")[0].GetProperty("message")
            .GetProperty("content").GetString();
        Assert.Contains("plain prompt form", text);
    }
}
