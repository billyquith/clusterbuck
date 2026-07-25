using Clusterbuck.Worker;
using Xunit;

namespace Clusterbuck.Worker.Tests;

/// <summary>
/// Presence ladder logic (ADR 10): small model while active, big models when away, with
/// hysteresis climbing and immediate descent/pause. The mode source is manual in M4b;
/// this proves the ladder mechanics independent of it.
/// </summary>
public sealed class PresenceLadderTests
{
    private static readonly Dictionary<string, List<string>> Ladder = new()
    {
        ["active"] = new() { "8b-extract" },
        ["away"] = new() { "8b-extract", "32b-reason", "70b-reason" },
    };
    private static readonly string[] All = { "8b-extract", "32b-reason", "70b-reason" };

    [Fact]
    public void Active_Serves_Small_Model_Only()
    {
        var l = new PresenceLadder(Ladder, All);
        Assert.Equal("active", l.Update("active"));
        Assert.Equal(new[] { "8b-extract" }, l.Capabilities());
    }

    [Fact]
    public void Climbing_To_Away_Requires_Hysteresis()
    {
        var t = 0.0;
        var l = new PresenceLadder(Ladder, All, hysteresisSeconds: 120, clock: () => t);

        Assert.Equal("active", l.Update("away"));   // first sighting → pending, not yet
        t = 60;
        Assert.Equal("active", l.Update("away"));   // still within hysteresis
        t = 130;
        Assert.Equal("away", l.Update("away"));     // stably away long enough → climb
        Assert.Equal(All, l.Capabilities());
    }

    [Fact]
    public void Descent_Is_Immediate()
    {
        var t = 0.0;
        var l = new PresenceLadder(Ladder, All, hysteresisSeconds: 120, clock: () => t);
        t = 200; l.Update("away");                   // starts the hysteresis timer
        t = 400; Assert.Equal("away", l.Update("away"));  // climbed after it elapsed
        Assert.Equal("active", l.Update("active"));  // no hysteresis descending
    }

    [Fact]
    public void Paused_Serves_Nothing()
    {
        var l = new PresenceLadder(Ladder, All);
        Assert.Equal("paused", l.Update("paused"));
        Assert.Empty(l.Capabilities());
    }

    [Fact]
    public void No_Ladder_Entry_Serves_All()
    {
        var l = new PresenceLadder(null, All);
        l.Update("active");
        Assert.Equal(All, l.Capabilities());  // no ladder ⇒ serve everything
    }
}
