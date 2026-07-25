namespace Clusterbuck.Worker;

/// <summary>
/// Presence-mode model ladder (ADR 10): a shared machine runs a small model while its
/// user is `active` and swaps in the large ones when `away`. Climbing the ladder
/// (active → away) is damped by hysteresis because cold-loading a big model is expensive;
/// descending (away → active) and pausing are immediate because eviction is cheap.
///
/// This is the ladder *logic*, driven by a supplied desired mode. Detecting presence from
/// the OS (screen lock / input idle) is deferred; M4b sets the mode manually (env / CLI /
/// `cbk pause`), which is also the honest fallback for machines without a clean signal.
/// </summary>
public sealed class PresenceLadder
{
    private readonly IReadOnlyDictionary<string, List<string>> _ladder;
    private readonly IReadOnlyList<string> _allCapabilities;
    private readonly double _hysteresisSeconds;
    private readonly Func<double> _clock;

    private string _effective = "active";
    private double? _awayPendingSince;

    public PresenceLadder(
        IReadOnlyDictionary<string, List<string>>? ladder,
        IReadOnlyList<string> allCapabilities,
        double hysteresisSeconds = 120,
        Func<double>? clock = null)
    {
        _ladder = ladder ?? new Dictionary<string, List<string>>();
        _allCapabilities = allCapabilities;
        _hysteresisSeconds = hysteresisSeconds;
        _clock = clock ?? (() => Environment.TickCount64 / 1000.0);
    }

    public string EffectiveMode => _effective;

    /// <summary>Feed the desired mode; returns the effective mode after hysteresis.</summary>
    public string Update(string desired)
    {
        switch (desired)
        {
            case "paused":
            case "active":
                _effective = desired;      // descend / pause immediately
                _awayPendingSince = null;
                break;
            case "away":
                if (_effective == "away") break;
                var now = _clock();
                _awayPendingSince ??= now;
                if (now - _awayPendingSince.Value >= _hysteresisSeconds)
                {
                    _effective = "away";   // climbed after staying away long enough
                    _awayPendingSince = null;
                }
                break;
        }
        return _effective;
    }

    /// <summary>Capabilities to serve in the current effective mode (empty when paused).</summary>
    public IReadOnlyList<string> Capabilities()
    {
        if (_effective == "paused") return Array.Empty<string>();
        if (_ladder.TryGetValue(_effective, out var caps)) return caps;
        return _allCapabilities;  // no ladder entry ⇒ serve everything this node can
    }
}
