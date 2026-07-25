using System.Diagnostics;
using System.Net.Sockets;
using Clusterbuck.Worker;
using Xunit;

namespace Clusterbuck.Worker.Tests;

/// <summary>
/// Model discovery (M6a): installed via the portable OpenAI /v1/models, loaded/digests via
/// the vendor adapter. Driven against the repo's fake model server, which serves all three
/// shapes — so this proves the real parsing paths without needing Ollama installed.
/// </summary>
public sealed class ModelInventoryTests : IAsyncLifetime
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
            $"{script} --port {_port} --models alpha:1b,beta:7b --loaded beta:7b")
        {
            RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false,
        });
        for (var i = 0; i < 50; i++)
        {
            try
            {
                await _http.GetAsync($"http://127.0.0.1:{_port}/healthz");
                return;
            }
            catch (HttpRequestException)
            {
                await Task.Delay(100);
            }
        }
        throw new InvalidOperationException("fake model server did not start");
    }

    public Task DisposeAsync()
    {
        try { _server?.Kill(entireProcessTree: true); } catch { }
        _http.Dispose();
        return Task.CompletedTask;
    }

    private ModelInventory Inventory(string manager = "auto") =>
        new(_http, $"http://127.0.0.1:{_port}/v1", manager);

    [Fact]
    public async Task Installed_Comes_From_Generic_OpenAI_Endpoint()
    {
        var installed = await Inventory().InstalledAsync();
        Assert.Equal(new[] { "alpha:1b", "beta:7b" }, installed);
    }

    [Fact]
    public async Task Loaded_Comes_From_Vendor_Adapter()
    {
        Assert.Equal(new[] { "beta:7b" }, await Inventory().LoadedAsync());
    }

    [Fact]
    public async Task Loaded_Is_Empty_When_Manager_Is_None()
    {
        // `none` ⇒ no vendor calls at all; loaded is honestly "unknown", not invented.
        Assert.Empty(await Inventory("none").LoadedAsync());
        // …but portable discovery still works.
        Assert.Equal(2, (await Inventory("none").InstalledAsync()).Count);
    }

    [Fact]
    public async Task Digests_Are_Reported_Per_Artifact()
    {
        var digests = await Inventory().DigestsAsync();
        Assert.Equal(2, digests.Count);
        Assert.StartsWith("sha256:", digests["alpha:1b"]);
        Assert.NotEqual(digests["alpha:1b"], digests["beta:7b"]);
    }

    [Fact]
    public async Task Unreachable_Server_Degrades_To_Empty()
    {
        // A down model server must never take the heartbeat down.
        var dead = new ModelInventory(_http, "http://127.0.0.1:1/v1", "auto");
        Assert.Empty(await dead.InstalledAsync());
        Assert.Empty(await dead.LoadedAsync());
        Assert.Empty(await dead.DigestsAsync());
    }

    [Fact]
    public void NativeBase_Strips_OpenAI_Suffix()
    {
        Assert.Equal("http://h:1234", new ModelInventory(_http, "http://h:1234/v1", "auto").NativeBase);
        Assert.Equal("http://h:1234", new ModelInventory(_http, "http://h:1234", "auto").NativeBase);
    }
}
