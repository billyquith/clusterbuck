using Clusterbuck.Worker;
using Xunit;

namespace Clusterbuck.Worker.Tests;

/// <summary>
/// Redis URL parsing. The password case matters because dropping it silently makes an
/// authenticated broker look simply unreachable — a confusing failure to diagnose.
/// </summary>
public sealed class WorkerConfigTests
{
    [Fact]
    public void Bare_HostPort_Passes_Through()
    {
        Assert.Equal(("localhost:6379", 0), WorkerConfig.ParseRedis("localhost:6379"));
    }

    [Fact]
    public void Url_Yields_Host_And_Database()
    {
        Assert.Equal(("h:6380", 3), WorkerConfig.ParseRedis("redis://h:6380/3"));
    }

    [Fact]
    public void Default_Port_Is_Applied()
    {
        var (config, db) = WorkerConfig.ParseRedis("redis://h/1");
        Assert.Equal("h:6379", config);
        Assert.Equal(1, db);
    }

    [Fact]
    public void Password_Only_Userinfo_Is_Carried()
    {
        var (config, _) = WorkerConfig.ParseRedis("redis://:s3cret@h:6379/0");
        Assert.Contains("password=s3cret", config);
        Assert.StartsWith("h:6379", config);
        Assert.DoesNotContain("user=", config);   // no username half present
    }

    [Fact]
    public void User_And_Password_Are_Both_Carried()
    {
        var (config, _) = WorkerConfig.ParseRedis("redis://alice:s3cret@h:6379/0");
        Assert.Contains("password=s3cret", config);
        Assert.Contains("user=alice", config);
    }

    [Fact]
    public void Percent_Encoded_Password_Is_Decoded()
    {
        var (config, _) = WorkerConfig.ParseRedis("redis://:p%40ss%3Aword@h:6379/0");
        Assert.Contains("password=p@ss:word", config);
    }
}
