using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Clusterbuck.Worker;

/// <summary>
/// Probes the node's hardware for enrollment (protocols.md §6). Small per-OS shims, no
/// heavyweight hardware-info dependency. Everything is best-effort with safe fallbacks —
/// a probe that can't read a value degrades rather than failing enrollment. The optional
/// throughput micro-benchmark (a timed call to the local model server) is left null here.
/// </summary>
public static class HardwareProbe
{
    public static EnrollRequest Build(string joinToken, string profile) => new()
    {
        JoinToken = joinToken,
        Hostname = SafeHostname(),
        Os = OsName(),
        Arch = RuntimeInformation.OSArchitecture.ToString().ToLowerInvariant(),
        Hw = new HwProbe
        {
            RamGb = Math.Round(RamBytes() / 1_073_741_824.0, 1),
            Accelerator = Accelerator(),
            DiskFreeGb = Math.Round(DiskFreeBytes() / 1_073_741_824.0, 1),
        },
        Profile = profile,
    };

    private static string OsName()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) return "darwin";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) return "linux";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return "windows";
        return "unknown";
    }

    private static string SafeHostname()
    {
        try { return Environment.MachineName; } catch { return "unknown"; }
    }

    private static double RamBytes()
    {
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                var s = Run("sysctl", "-n hw.memsize");
                if (double.TryParse(s.Trim(), out var b)) return b;
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                foreach (var line in File.ReadLines("/proc/meminfo"))
                    if (line.StartsWith("MemTotal:"))
                    {
                        var kb = double.Parse(new string(line.Where(char.IsDigit).ToArray()));
                        return kb * 1024;
                    }
            }
        }
        catch { /* fall through */ }
        // Fallback: the GC's view of available memory (a floor, not physical RAM).
        return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
    }

    private static string Accelerator()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
            && RuntimeInformation.OSArchitecture == Architecture.Arm64)
            return "metal";
        try
        {
            if (!string.IsNullOrWhiteSpace(Run("nvidia-smi", "--query-gpu=name --format=csv,noheader")))
                return "cuda";
        }
        catch { /* no nvidia-smi */ }
        return "cpu";
    }

    private static double DiskFreeBytes()
    {
        try
        {
            var root = Path.GetPathRoot(AppContext.BaseDirectory) ?? "/";
            return new DriveInfo(root).AvailableFreeSpace;
        }
        catch { return 0; }
    }

    private static string Run(string file, string args)
    {
        using var p = Process.Start(new ProcessStartInfo(file, args)
        {
            RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false,
        })!;
        var outp = p.StandardOutput.ReadToEnd();
        p.WaitForExit(2000);
        return outp;
    }
}
