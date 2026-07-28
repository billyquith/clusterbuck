using System.IO;
using System.Text.Json;
using Clusterbuck.Worker;
using Xunit;

namespace Clusterbuck.Worker.Tests;

/// <summary>
/// The cross-language crypto boundary (ADR 13/22): a manifest signed by the Python
/// coordinator (DER ECDSA-P256) must verify in the C# worker, and any tamper must fail.
/// The fixture is signed once with a throwaway key whose private half is discarded; only
/// the public key + signed manifest are committed. This is the proof the DER-vs-P1363
/// interop is handled — if Verify returned false here, the signature format is wrong.
/// </summary>
public sealed class UpdateVerifierTests
{
    private static string ContractDir => ContractPath();

    private static string ContractPath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var c = Path.Combine(dir.FullName, "contract", "update-manifest.schema.json");
            if (File.Exists(c)) return Path.Combine(dir.FullName, "contract");
            dir = dir.Parent;
        }
        throw new DirectoryNotFoundException("contract/ not found");
    }

    private static (UpdateManifest, string pem) LoadFixture()
    {
        var manifest = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(ContractDir, "examples", "update-manifest.valid.json")),
            CbkJsonContext.Default.UpdateManifest)!;
        var pem = File.ReadAllText(Path.Combine(ContractDir, "examples", "update-signing.pub.pem"));
        return (manifest, pem);
    }

    [Fact]
    public void Verifies_Python_Signed_Manifest()
    {
        var (manifest, pem) = LoadFixture();
        Assert.True(UpdateVerifier.Verify(manifest, pem));  // DER interop proven
    }

    [Fact]
    public void Rejects_Tampered_Artifact_Digest()
    {
        var (manifest, pem) = LoadFixture();
        var tampered = manifest with { Sha256 = new string('0', 64) };
        Assert.False(UpdateVerifier.Verify(tampered, pem));
    }

    [Fact]
    public void Rejects_Tampered_Version()
    {
        var (manifest, pem) = LoadFixture();
        Assert.False(UpdateVerifier.Verify(manifest with { Version = "9.9.9" }, pem));
    }

    [Fact]
    public void Rejects_Garbage_Signature()
    {
        var (manifest, pem) = LoadFixture();
        Assert.False(UpdateVerifier.Verify(manifest with { Signature = "not-base64!!" }, pem));
        Assert.False(UpdateVerifier.Verify(manifest with { Signature = "AAAA" }, pem));
    }

    [Fact]
    public void Skew_Gate_Pauses_When_Behind()
    {
        // Worker speaks protocol 1; a release requiring 2 should pause pulling.
        Assert.True(UpdateVerifier.ShouldPauseForSkew(new UpdateManifest { ProtocolVersion = 2 }));
        Assert.False(UpdateVerifier.ShouldPauseForSkew(new UpdateManifest { ProtocolVersion = 1 }));
        Assert.False(UpdateVerifier.ShouldPauseForSkew(new UpdateManifest { ProtocolVersion = null }));
    }
}
