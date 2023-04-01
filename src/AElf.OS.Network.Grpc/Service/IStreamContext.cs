using System.Text;
using Google.Protobuf.Collections;
using Grpc.Core;

namespace AElf.OS.Network.Grpc;

public interface IStreamContext
{
    string GetPeerInfo();
    string GetPubKey();
    byte[] GetSessionId();
}

public class ServiceStreamContext : IStreamContext
{
    public ServerCallContext Context;

    public ServiceStreamContext(ServerCallContext context)
    {
        Context = context;
    }

    public string GetPeerInfo()
    {
        return Context.GetPeerInfo();
    }

    public string GetPubKey()
    {
        return Context.GetPublicKey();
    }

    public byte[] GetSessionId()
    {
        return Context.GetSessionId();
    }
}

public class StreamMessageMetaStreamContext : IStreamContext
{
    private MapField<string, string> _meta;

    public StreamMessageMetaStreamContext(MapField<string, string> meta)
    {
        _meta = meta;
    }

    public string GetPeerInfo()
    {
        return _meta[GrpcConstants.PeerInfoMetadataKey];
    }

    public string GetPubKey()
    {
        return _meta[GrpcConstants.PubkeyMetadataKey];
    }

    public byte[] GetSessionId()
    {
        var val = _meta[GrpcConstants.SessionIdMetadataKey];
        return val == null ? null : Encoding.ASCII.GetBytes(val);
    }
}