using Clusterbuck.Worker.Commands;
using Spectre.Console.Cli;

var app = new CommandApp();
app.Configure(config =>
{
    config.SetApplicationName("cbk");
    config.AddCommand<WorkCommand>("work")
        .WithDescription("Pull jobs for this node's capabilities and run them on the local model server.");
    config.AddCommand<SubmitCommand>("submit")
        .WithDescription("Submit an async job to the server (convenience client).");
    config.AddCommand<StatusCommand>("status")
        .WithDescription("Poll a job's status/result by id.");
    config.AddCommand<FleetCommand>("fleet")
        .WithDescription("List the server's capability/node registry.");
});
return await app.RunAsync(args);
