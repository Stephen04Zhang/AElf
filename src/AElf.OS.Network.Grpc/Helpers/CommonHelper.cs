using System;

namespace AElf.OS.Network.Grpc.Helpers;

public class CommonHelper
{
    public static string GenerateRequestId()
    {
        var timeMs = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        var guid = Guid.NewGuid().ToString();
        return timeMs.ToString() + '_' + guid;
    }
}