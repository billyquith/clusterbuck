using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;

namespace Clusterbuck.Worker;

/// <summary>A signed worker release manifest (contract/update-manifest.schema.json).</summary>
public sealed record UpdateManifest
{
    [JsonPropertyName("version")] public string Version { get; init; } = "";
    [JsonPropertyName("rid")] public string Rid { get; init; } = "";
    [JsonPropertyName("url")] public string Url { get; init; } = "";
    [JsonPropertyName("sha256")] public string Sha256 { get; init; } = "";
    [JsonPropertyName("channel")] public string Channel { get; init; } = "";
    [JsonPropertyName("protocol_version")] public int? ProtocolVersion { get; init; }
    [JsonPropertyName("signature")] public string Signature { get; init; } = "";
}

/// <summary>
/// Verifies a release manifest before any update is applied (ADR 13). An update channel is
/// RCE by design, so this is the security boundary: no valid signature over the pinned
/// artifact ⇒ refuse.
///
/// Interop note (the trap): Python `cryptography` emits DER (SEC1/RFC 3279) ECDSA
/// signatures, but .NET's VerifyData defaults to IEEE-P1363 (raw r‖s) and returns false —
/// not an error — on a DER blob. Passing DSASignatureFormat.Rfc3279DerSequence is the fix.
/// </summary>
public static class UpdateVerifier
{
    /// <summary>The queue-contract protocol version this worker speaks.</summary>
    public const int ProtocolVersion = 1;

    public static bool Verify(UpdateManifest manifest, string publicKeyPem)
    {
        // Must match signing.py::signing_payload exactly, including url and
        // protocol_version — signing only the first four fields let an attacker redirect the
        // fetch host or stall the fleet via an inflated protocol_version.
        var payload = Encoding.UTF8.GetBytes(string.Join("\n",
            manifest.Version, manifest.Rid, manifest.Sha256, manifest.Channel,
            manifest.Url, (manifest.ProtocolVersion ?? 1).ToString(CultureInfo.InvariantCulture)));
        byte[] signature;
        try
        {
            signature = Convert.FromBase64String(manifest.Signature);
        }
        catch (FormatException)
        {
            return false;
        }

        using var ecdsa = ECDsa.Create();
        ecdsa.ImportFromPem(publicKeyPem);
        return ecdsa.VerifyData(
            payload, signature, HashAlgorithmName.SHA256,
            DSASignatureFormat.Rfc3279DerSequence);  // ← accept Python's DER
    }

    /// <summary>
    /// Skew gate (protocols.md §7): a worker too far behind the release's protocol pauses
    /// pulling until it has updated, rather than speaking a stale queue contract.
    /// </summary>
    public static bool ShouldPauseForSkew(UpdateManifest manifest) =>
        manifest.ProtocolVersion is int required && ProtocolVersion < required;
}
