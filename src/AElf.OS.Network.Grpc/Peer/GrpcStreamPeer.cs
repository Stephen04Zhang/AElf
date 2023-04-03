using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AElf.OS.Network.Application;
using AElf.OS.Network.Protocol.Types;
using Grpc.Core;

namespace AElf.OS.Network.Grpc;

public class GrpcStreamPeer : GrpcPeer, IStreamWriter
{
    private AsyncDuplexStreamingCall<StreamMessage, StreamMessage> _duplexStreamingCall;
    private StreamClient _streamClient;
    private Task _serveTask;
    private CancellationTokenSource _streamListenTaskTokenSource;
    protected readonly ActionBlock<StreamJob> _sendStreamJobs;

    public GrpcStreamPeer(GrpcClient client, DnsEndPoint remoteEndpoint, PeerConnectionInfo peerConnectionInfo) : base(client,
        remoteEndpoint, peerConnectionInfo)
    {
        _sendStreamJobs = new ActionBlock<StreamJob>(SendStreamJobAsync);
    }

    public void StartServe(AsyncDuplexStreamingCall<StreamMessage, StreamMessage> duplexStreamingCall, StreamClient streamClient, Task serveTask, CancellationTokenSource tokenSource)
    {
        _duplexStreamingCall = duplexStreamingCall;
        _streamClient = streamClient;
        _serveTask = serveTask;
        _streamListenTaskTokenSource = tokenSource;
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
        catch (RpcException e)
        {
            _duplexStreamingCall.Dispose();
            _duplexStreamingCall = null;
            _streamClient = null;
            base.HandleRpcException(e, "stream health check");
        }
        catch (TimeoutException e)
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

    public async Task WriteAsync(StreamMessage message)
    {
        await _sendStreamJobs.SendAsync(new StreamJob
        {
            StreamMessage = message, SendCallback = ex =>
            {
                if (ex != null) throw ex;
            }
        });
    }

    public WriteOptions WriteOptions { get; set; }

    private async Task SendStreamJobAsync(StreamJob job)
    {
        if (!IsReady)
            return;

        try
        {
            if (job.StreamMessage == null) return;
            var responseStream = GetResponseStream();
            if (responseStream == null) return;
            await responseStream.WriteAsync(job.StreamMessage);
        }
        catch (RpcException ex)
        {
            job.SendCallback?.Invoke(HandleRpcException(ex, $"Could not write to stream to {this}: "));
            await Task.Delay(StreamRecoveryWaitTime);
            return;
        }

        job.SendCallback?.Invoke(null);
    }
}