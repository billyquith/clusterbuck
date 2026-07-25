using System.Diagnostics;
using System.Net.Sockets;
using Clusterbuck.Worker;
using Xunit;

namespace Clusterbuck.Worker.Tests;

/// <summary>
/// The model-manager adapter (M6c / ADR 25): the one place clusterbuck leaves the generic
/// OpenAI wire protocol. Driven against the fake model server's Ollama-native /api/pull and
/// /api/delete, including a simulated pull failure — so both success and failure paths are
/// proven without downloading gigabytes.
/// </summary>
public sealed class ModelManagerTests : IAsyncLifetime
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
        _server = Process.Start(new ProcessStartInfo("python3",
            $"{script} --port {_port} --models existing:1b --fail-pulls cursed:9b")
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

    private string Base => $"http://127.0.0.1:{_port}";
    private ModelManager Manager(string mgr = "ollama") => new(_http, Base, mgr);
    private ModelInventory Inv() => new(_http, $"{Base}/v1", "ollama");

    [Fact]
    public async Task Install_Adds_The_Artifact()
    {
        var (ok, error) = await Manager().InstallAsync("fresh:3b");
        Assert.True(ok, error);
        Assert.Null(error);
        Assert.Contains("fresh:3b", await Inv().InstalledAsync());
    }

    [Fact]
    public async Task Install_Failure_Is_Reported_Not_Thrown()
    {
        var (ok, error) = await Manager().InstallAsync("cursed:9b");
        Assert.False(ok);
        Assert.Contains("simulated pull failure", error);
        Assert.DoesNotContain("cursed:9b", await Inv().InstalledAsync());
    }

    [Fact]
    public async Task Remove_Frees_The_Artifact()
    {
        Assert.Contains("existing:1b", await Inv().InstalledAsync());
        var (ok, error) = await Manager().RemoveAsync("existing:1b");
        Assert.True(ok, error);
        Assert.DoesNotContain("existing:1b", await Inv().InstalledAsync());
    }

    [Fact]
    public async Task Remove_Of_Absent_Artifact_Fails_Cleanly()
    {
        var (ok, error) = await Manager().RemoveAsync("never:1b");
        Assert.False(ok);
        Assert.Contains("remove failed", error);
    }

    [Fact]
    public async Task Manager_None_Refuses_And_Says_So()
    {
        // A model server we can't drive (llama.cpp etc.): report honestly, don't pretend.
        var mgr = Manager("none");
        Assert.False(mgr.CanManage);
        var (ok, error) = await mgr.InstallAsync("x:1b");
        Assert.False(ok);
        Assert.Contains("manually", error);
        var (ok2, error2) = await mgr.RemoveAsync("existing:1b");
        Assert.False(ok2);
        Assert.Contains("manually", error2);
    }

    [Fact]
    public async Task Unreachable_Server_Fails_Without_Throwing()
    {
        var dead = new ModelManager(_http, "http://127.0.0.1:1", "ollama");
        var (ok, error) = await dead.InstallAsync("x:1b");
        Assert.False(ok);
        Assert.Contains("pull failed", error);
    }
}
