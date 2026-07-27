using System.Security.Cryptography;

namespace Clusterbuck.Worker;

/// <summary>
/// Applies a signed worker update (protocols.md §7, ADR 13). An update channel is
/// remote-code-execution by design, so the order of operations here is the security
/// boundary and is deliberately strict:
///
///   1. **Refuse without a pinned public key.** No key ⇒ no self-update, ever. Signed or
///      nothing; there is no "trust this once" path.
///   2. **Verify the signature before fetching.** The signature covers the url, so a
///      redirected download is rejected before any network call to the attacker's host.
///   3. **Verify the digest after fetching**, before the bytes are allowed near the
///      install path. A signature over a manifest says nothing about what actually arrived.
///   4. **Retain the previous binary** as `cbk.prev`, so a bad release can be rolled back.
///   5. **Swap, then re-exec.** The replaced process starts the new binary and exits.
///
/// Deliberately NOT implemented here: canary rings and automatic crash-loop rollback. The
/// retained previous binary makes rollback *possible* (and `Rollback()` performs it), but
/// deciding a release is crash-looping needs multi-node observation this cannot honestly
/// verify on one machine — see ADR 13. `cbk.prev` plus a manual rollback is the honest
/// half.
/// </summary>
public sealed class UpdateApplier
{
    private readonly HttpClient _http;
    private readonly string? _publicKeyPem;
    private readonly Action<string> _log;

    public UpdateApplier(HttpClient http, string? publicKeyPem, Action<string>? log = null)
    {
        _http = http;
        _publicKeyPem = publicKeyPem;
        _log = log ?? Console.WriteLine;
    }

    /// <summary>Where this agent's own executable lives, or null under `dotnet run`.</summary>
    public static string? CurrentExecutable
    {
        get
        {
            var path = Environment.ProcessPath;
            if (path is null) return null;
            // Under `dotnet cbk.dll` the process is the shared host, not our binary — a
            // self-replacing update would clobber the SDK. Only a published apphost qualifies.
            var name = Path.GetFileNameWithoutExtension(path);
            return name.Equals("dotnet", StringComparison.OrdinalIgnoreCase) ? null : path;
        }
    }

    public enum Outcome { Applied, Skipped, Refused, Failed }

    public sealed record Result(Outcome Outcome, string Detail);

    /// <summary>
    /// Verify and apply a manifest. On success the process re-execs and does not return.
    /// </summary>
    public async Task<Result> ApplyAsync(UpdateManifest manifest, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(_publicKeyPem))
            return new(Outcome.Refused,
                "no pinned public key (CBK_UPDATE_PUBKEY): self-update is signed-or-nothing");

        var target = CurrentExecutable;
        if (target is null)
            return new(Outcome.Skipped,
                "running via the dotnet host, not a published binary — nothing to replace");

        if (manifest.Version == WorkerConfig.AgentVersion)
            return new(Outcome.Skipped, $"already on {manifest.Version}");

        // (2) Signature first: it covers the url, so this rejects a redirected download
        // before any request is made to an attacker-chosen host.
        if (!UpdateVerifier.Verify(manifest, _publicKeyPem))
            return new(Outcome.Refused,
                $"signature invalid for {manifest.Version} ({manifest.Rid}) — refusing");

        if (UpdateVerifier.ShouldPauseForSkew(manifest))
            _log($"note: {manifest.Version} requires protocol {manifest.ProtocolVersion}, " +
                 $"this agent speaks {UpdateVerifier.ProtocolVersion}");

        var dir = Path.GetDirectoryName(target)!;
        var staged = Path.Combine(dir, "cbk.new");
        var previous = Path.Combine(dir, "cbk.prev");

        try
        {
            _log($"fetching {manifest.Version} ({manifest.Rid}) from {manifest.Url}");
            using (var resp = await _http.GetAsync(manifest.Url, ct))
            {
                resp.EnsureSuccessStatusCode();
                await using var fs = File.Create(staged);
                await resp.Content.CopyToAsync(fs, ct);
            }

            // (3) The manifest's signature says nothing about what actually arrived.
            var actual = Convert.ToHexString(
                await SHA256.HashDataAsync(File.OpenRead(staged), ct)).ToLowerInvariant();
            if (!actual.Equals(manifest.Sha256, StringComparison.OrdinalIgnoreCase))
            {
                File.Delete(staged);
                return new(Outcome.Refused,
                    $"digest mismatch: expected {manifest.Sha256}, got {actual}");
            }

            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(staged,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
                    UnixFileMode.GroupRead | UnixFileMode.GroupExecute |
                    UnixFileMode.OtherRead | UnixFileMode.OtherExecute);

            // (4) Retain the outgoing binary so a bad release can be reverted.
            if (File.Exists(previous)) File.Delete(previous);
            File.Move(target, previous);          // a running executable can be renamed
            File.Move(staged, target);
            _log($"installed {manifest.Version}; previous binary retained at {previous}");

            // (5) Hand over to the new binary and step aside.
            Reexec(target);
            return new(Outcome.Applied, $"applied {manifest.Version}, re-executing");
        }
        catch (Exception e)
        {
            try { if (File.Exists(staged)) File.Delete(staged); } catch { }
            // If the swap half-completed, put the old binary back rather than leaving none.
            try
            {
                if (!File.Exists(target) && File.Exists(previous)) File.Move(previous, target);
            }
            catch { }
            return new(Outcome.Failed, $"update to {manifest.Version} failed: {e.Message}");
        }
    }

    /// <summary>Restore the retained previous binary. The manual half of rollback.</summary>
    public static bool Rollback(Action<string>? log = null)
    {
        var target = CurrentExecutable;
        if (target is null) return false;
        var previous = Path.Combine(Path.GetDirectoryName(target)!, "cbk.prev");
        if (!File.Exists(previous)) return false;
        var scrap = target + ".bad";
        if (File.Exists(scrap)) File.Delete(scrap);
        File.Move(target, scrap);
        File.Move(previous, target);
        log?.Invoke($"rolled back to the retained binary; bad build kept at {scrap}");
        return true;
    }

    private void Reexec(string target)
    {
        var args = Environment.GetCommandLineArgs().Skip(1).ToArray();
        var psi = new System.Diagnostics.ProcessStartInfo(target) { UseShellExecute = false };
        foreach (var a in args) psi.ArgumentList.Add(a);
        System.Diagnostics.Process.Start(psi);
        _log("re-executed as the new version; this process is exiting");
        // Give the child a moment to start before the parent's exit closes shared handles.
        Thread.Sleep(250);
        Environment.Exit(0);
    }
}
