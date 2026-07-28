namespace Clusterbuck.Worker;

/// <summary>
/// A hand-rolled argument parser and console writer, replacing Spectre.Console.Cli.
///
/// Spectre is excellent, but its command binding is reflection-based, which made the worker
/// **un-AOT-able**: `PublishAot=true` emitted IL2104/IL3053 trim and AOT-analysis warnings, and
/// IL3000 for `Assembly.Location` under single-file. Those risk a clean link that then fails at
/// *runtime* on argument parsing — the worst failure shape for an agent that fans out across a
/// fleet. ADR 19 wanted a lean AOT single-file binary; seven verbs did not justify giving that
/// up, so the framework goes and this stays deliberately dull: no reflection, no attributes,
/// no DI. See ADR 28.
/// </summary>
public sealed class CliArgs
{
    private readonly List<string> _positional = new();
    private readonly Dictionary<string, string?> _options = new(StringComparer.OrdinalIgnoreCase);

    public string Verb { get; }

    private CliArgs(string verb) => Verb = verb;

    /// <summary>
    /// Parse `verb [--opt value] [--flag] [positional…]`. Also accepts `--opt=value` and
    /// short forms the previous CLI exposed (`-c`, `-p`, `-t`), which are mapped to their
    /// long names so existing invocations and the e2e scripts keep working.
    /// </summary>
    public static CliArgs Parse(string[] argv)
    {
        var verb = argv.Length > 0 && !argv[0].StartsWith('-') ? argv[0] : "";
        var cli = new CliArgs(verb);
        var start = verb.Length > 0 ? 1 : 0;

        for (var i = start; i < argv.Length; i++)
        {
            var a = argv[i];
            if (!a.StartsWith('-')) { cli._positional.Add(a); continue; }

            var name = Canonical(a.Split('=', 2)[0]);
            if (a.Contains('='))
            {
                cli._options[name] = a.Split('=', 2)[1];
                continue;
            }
            // A following non-dash token is this option's value; otherwise it is a flag.
            if (i + 1 < argv.Length && !argv[i + 1].StartsWith('-'))
                cli._options[name] = argv[++i];
            else
                cli._options[name] = null;      // present, no value ⇒ flag
        }
        return cli;
    }

    private static string Canonical(string opt) => opt switch
    {
        "-c" => "--capabilities",
        "-p" => "--prompt",
        "-t" => "--token",
        "-h" => "--help",
        _ => opt,
    };

    public string? Opt(string name) => _options.TryGetValue(name, out var v) ? v : null;
    public bool Has(string name) => _options.ContainsKey(name);
    public string? Arg(int index) => index < _positional.Count ? _positional[index] : null;

    public int? OptInt(string name) =>
        int.TryParse(Opt(name), out var n) ? n : null;

    /// <summary>Option value, else the environment variable, else the fallback.</summary>
    public string OptOrEnv(string name, string envVar, string fallback) =>
        Opt(name) is { Length: > 0 } v ? v
        : Environment.GetEnvironmentVariable(envVar) is { Length: > 0 } e ? e
        : fallback;
}

/// <summary>
/// Console output. Colour is emitted only to a real terminal, and suppressed when NO_COLOR is
/// set — so piping into the e2e scripts' `grep` yields clean text rather than escape codes.
/// </summary>
public static class Out
{
    private static readonly bool Colour =
        !Console.IsOutputRedirected &&
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable("NO_COLOR"));

    private const string Esc = "\u001b";
    private const string Reset = "\u001b[0m";

    private static string Wrap(string text, string code) =>
        Colour ? $"{Esc}[{code}m{text}{Reset}" : text;

    public static void Line(string text) => Console.WriteLine(text);
    public static void Dim(string text) => Console.WriteLine(Wrap(text, "90"));
    public static void Good(string text) => Console.WriteLine(Wrap(text, "32"));
    public static void Warn(string text) => Console.WriteLine(Wrap(text, "33"));
    public static void Info(string text) => Console.WriteLine(Wrap(text, "36"));
    public static void Error(string text) => Console.Error.WriteLine(Wrap(text, "31"));

    /// <summary>A plain column-aligned table — enough for `cbk fleet`.</summary>
    public static void Table(IReadOnlyList<string> headers, IReadOnlyList<string[]> rows)
    {
        var widths = headers.Select(h => h.Length).ToArray();
        foreach (var row in rows)
            for (var i = 0; i < widths.Length && i < row.Length; i++)
                widths[i] = Math.Max(widths[i], (row[i] ?? "").Length);

        string Render(IReadOnlyList<string> cells) =>
            "  " + string.Join("  ", cells.Select((c, i) => (c ?? "").PadRight(widths[i]))).TrimEnd();

        Console.WriteLine(Wrap(Render(headers), "1"));
        Console.WriteLine("  " + string.Join("  ", widths.Select(w => new string('-', w))));
        foreach (var row in rows) Console.WriteLine(Render(row));
    }
}
