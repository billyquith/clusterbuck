namespace Clusterbuck.Worker.Commands;

/// <summary>`cbk enroll` — probe the hardware, join with a one-time token, persist identity.</summary>
public static class EnrollCommand
{
    public static async Task<int> RunAsync(CliArgs cli)
    {
        var token = cli.Opt("--token");
        if (string.IsNullOrWhiteSpace(token))
        {
            Out.Error("--token is required (mint one on the coordinator: POST /nodes/tokens)");
            return 1;
        }
        var server = cli.OptOrEnv("--server", "CBK_SERVER_URL", "http://localhost:8000");
        var statePath = cli.Opt("--state") ?? NodeStateStore.DefaultPath;
        var profile = cli.Opt("--profile") ?? "shared";

        var req = HardwareProbe.Build(token, profile);
        Out.Dim($"probed ram={req.Hw.RamGb}GB accel={req.Hw.Accelerator} " +
                $"disk={req.Hw.DiskFreeGb}GB arch={req.Arch}");

        // Enrollment is authenticated by the join token, not the operator key (ADR 26), so a
        // plain client is correct here — a worker never needs the admin secret.
        using var http = new HttpClient();
        EnrollResponse resp;
        try
        {
            resp = await new RegistryClient(http, server).EnrollAsync(req);
        }
        catch (Exception e)
        {
            Out.Error($"enroll failed: {e.Message}");
            return 1;
        }

        NodeStateStore.Save(statePath, new NodeState
        {
            NodeId = resp.NodeId,
            NodeKey = resp.NodeKey,
            Server = server,
            Capabilities = resp.Proposed.Capabilities,
            Ladder = resp.Proposed.Ladder,
            Mode = "active",
        });
        Out.Good($"enrolled as {resp.NodeId} · capabilities: " +
                 string.Join(", ", resp.Proposed.Capabilities));
        Out.Dim($"identity saved to {statePath}");
        return 0;
    }
}
