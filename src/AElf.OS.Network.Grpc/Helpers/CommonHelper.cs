using System;

namespace AElf.OS.Network.Grpc.Helpers;

public class CommonHelper
{
    public static string GenerateRequestId()
    {
        var timeMs = DateTime.UtcNow.Second;
        var guid = Guid.NewGuid().ToString();
        return timeMs.ToString() + '-' + guid;
    }
}