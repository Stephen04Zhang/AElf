using System;
using AElf.Kernel;

namespace AElf.OS.Network.Grpc.Helpers;

public static class CommonHelper
{
    public static string GenerateRequestId()
    {
        var timeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var guid = Guid.NewGuid().ToString();
        return timeMs.ToString() + '_' + guid;
    }

    public static bool GreaterThanSupportStreamMinVersion(this string version)
    {
        return Version.Parse(version).CompareTo(KernelConstants.SupportStreamMinVersion) >= 0;
    }
}