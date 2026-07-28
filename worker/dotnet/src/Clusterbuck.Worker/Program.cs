using Clusterbuck.Worker;
using Clusterbuck.Worker.Commands;

// Plain switch dispatch, no CLI framework — see Cli.cs for why (AOT).
var cli = CliArgs.Parse(args);

// --version is checked before the help branch: a bare `cbk --version` has no verb, so the
// "no verb ⇒ usage" rule would otherwise swallow it. Release tooling greps this.
if (cli.Has("--version") || cli.Verb == "version")
{
    Out.Line(WorkerConfig.AgentVersion);
    return 0;
}

if (cli.Verb is "" or "help" or "--help" || cli.Has("--help"))
{
    Usage();
    return cli.Verb is "" ? 1 : 0;
}

try
{
    return cli.Verb switch
    {
        "work" => await WorkCommand.RunAsync(cli),
        "submit" => await SubmitCommand.RunAsync(cli),
        "status" => await StatusCommand.RunAsync(cli),
        "fleet" => await FleetCommand.RunAsync(cli),
        "enroll" => await EnrollCommand.RunAsync(cli),
        "pause" => ModeCommand.Run(cli, "paused"),
        "resume" => ModeCommand.Run(cli, "active"),
        _ => Unknown(cli.Verb),
    };
}
catch (Exception e)
{
    Out.Error($"error: {e.Message}");
    return 1;
}

static int Unknown(string verb)
{
    Out.Error($"unknown command '{verb}'");
    Usage();
    return 1;
}

static void Usage()
{
    Out.Line("cbk — clusterbuck worker agent");
    Out.Line("");
    Out.Line("usage: cbk <command> [options]");
    Out.Line("");
    Out.Line("  work                    Pull jobs for this node's capabilities and run them");
    Out.Line("                            -c, --capabilities <list>   override served tiers");
    Out.Line("                            --model <name>              override the model");
    Out.Line("                            --state <path>              persisted node identity");
    Out.Line("  submit                  Submit an async job to the coordinator");
    Out.Line("                            -p, --prompt <text>         (required)");
    Out.Line("                            --capability <tier> | --task-class <c> --min-ability <n>");
    Out.Line("                            --urgency <urgent|necessary|waitable>");
    Out.Line("                            --privacy <local_only|cloud_ok>");
    Out.Line("  status <job_id>         Poll a job's status and result");
    Out.Line("  fleet                   List the coordinator's capability/node registry");
    Out.Line("  enroll                  Probe hardware and join the fleet");
    Out.Line("                            -t, --token <join-token>    (required)");
    Out.Line("                            --profile <dedicated|shared|background>");
    Out.Line("                            --state <path>");
    Out.Line("  pause | resume          Owner eviction: stop / resume claiming jobs");
    Out.Line("");
    Out.Line("  --server <url>          Coordinator URL (or CBK_SERVER_URL)");
    Out.Line("  --version               Print this agent's version");
    Out.Line("");
    Out.Line("Configuration is by environment (CBK_*); see docs/deployment.md.");
}
