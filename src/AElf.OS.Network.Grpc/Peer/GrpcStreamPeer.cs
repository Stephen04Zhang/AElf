using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using AElf.OS.Network.Application;
using AElf.OS.Network.Protocol.Types;
using Grpc.Core;

namespace AElf.OS.Network.Grpc;

public class GrpcStreamPeer : GrpcPeer
{
    private AsyncDuplexStreamingCall<StreamMessage, StreamMessage> _duplexStreamingCall;
    private StreamClient _streamClient;
    private Task _serveTask;
    private CancellationTokenSource _streamListenTaskTokenSource;
    public byte[] StreamInboundSessionId { get; set; }
    public byte[] SessionId { get; set; }

    public GrpcStreamPeer(GrpcClient client, DnsEndPoint remoteEndpoint, PeerConnectionInfo peerConnectionInfo) : base(client,
        remoteEndpoint, peerConnectionInfo)
    {
    }

    public void StartServe(AsyncDuplexStreamingCall<StreamMessage, StreamMessage> duplexStreamingCall, StreamClient streamClient, Task serveTask, CancellationTokenSource tokenSource)
    {
        _duplexStreamingCall = duplexStreamingCall;
        _streamClient = streamClient;
        _serveTask = serveTask;
        _streamListenTaskTokenSource = tokenSource;
    }

    public override async Task PingAsync()
    {
        await base.PingAsync();
        if (_streamClient == null) return;
        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, PingRequestTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        await _streamClient.PingAsync(data);
    }

    public override async Task CheckHealthAsync()
    {
        await base.CheckHealthAsync();
        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, CheckHealthTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        if (_streamClient == null) return;
        try
        {
            await _streamClient.CheckHealthAsync(data);
        }
        catch (Exception e)
        {
            throw new NetworkException("failed to check stream health", e, NetworkExceptionType.Unrecoverable);
        }
    }

    public override async Task DisconnectAsync(bool gracefulDisconnect)
    {
        await _duplexStreamingCall?.RequestStream?.CompleteAsync();
        _duplexStreamingCall?.Dispose();
        _streamListenTaskTokenSource?.Cancel();
        await base.DisconnectAsync(gracefulDisconnect);
    }

    public IAsyncStreamWriter<StreamMessage> GetResponseStream()
    {
        return _duplexStreamingCall?.RequestStream;
    }
}