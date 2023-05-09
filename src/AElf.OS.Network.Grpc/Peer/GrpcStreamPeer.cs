using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AElf.CSharp.Core.Extension;
using AElf.Kernel;
using AElf.OS.Network.Application;
using AElf.OS.Network.Events;
using AElf.OS.Network.Grpc.Helpers;
using AElf.OS.Network.Protocol;
using AElf.OS.Network.Protocol.Types;
using AElf.Types;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Utils;
using Microsoft.Extensions.Logging;
using Volo.Abp.EventBus.Local;

namespace AElf.OS.Network.Grpc;

public class GrpcStreamPeer : GrpcPeer
{
    private const int StreamWaitTime = 1000;
    private ILocalEventBus EventBus;
    private AsyncDuplexStreamingCall<StreamMessage, StreamMessage> _duplexStreamingCall;
    private CancellationTokenSource _streamListenTaskTokenSource;
    private IAsyncStreamWriter<StreamMessage> _clientStreamWriter;
    private readonly IHandshakeProvider _handshakeProvider;

    private readonly IStreamTaskResourcePool _streamTaskResourcePool;
    private readonly Dictionary<string, string> _peerMeta;

    protected readonly ActionBlock<StreamJob> _sendStreamJobs;
    public ILogger<PeerDialer> Logger { get; set; }
    private bool _isComplete { get; set; }

    public GrpcStreamPeer(GrpcClient client, DnsEndPoint remoteEndpoint, PeerConnectionInfo peerConnectionInfo,
        IAsyncStreamWriter<StreamMessage> clientStreamWriter,
        IStreamTaskResourcePool streamTaskResourcePool, Dictionary<string, string> peerMeta,
        ILocalEventBus eventBus, IHandshakeProvider handshakeProvider, ILogger<PeerDialer> Logger) : base(client,
        remoteEndpoint, peerConnectionInfo)
    {
        _clientStreamWriter = clientStreamWriter;
        _streamTaskResourcePool = streamTaskResourcePool;
        _peerMeta = peerMeta;
        EventBus = eventBus;
        _handshakeProvider = handshakeProvider;
        _sendStreamJobs = new ActionBlock<StreamJob>(WriteStreamJobAsync);
        this.Logger = Logger;
    }


    public async Task<bool> BuildStreamAndListenAsync()
    {
        _duplexStreamingCall = _client.RequestByStream(new CallOptions().WithDeadline(DateTime.MaxValue));
        _clientStreamWriter = _duplexStreamingCall.RequestStream;
        var tokenSource = new CancellationTokenSource();
        Task.Run(async () =>
        {
            try
            {
                Logger.LogDebug("start listen {remoteEndPoint}'s reply", RemoteEndpoint.ToString());
                await _duplexStreamingCall.ResponseStream.ForEachAsync(async req =>
                {
                    var reply = StreamMessage.Parser.ParseFrom(req.ToByteString());
                    Logger.LogDebug("receive {requestid}", reply.RequestId);
                    await EventBus.PublishAsync(new StreamMessageReceivedEvent(req.ToByteString(), Info.Pubkey));
                });
                Logger.LogDebug("streaming listen end and complete, {remoteEndPoint} successful", RemoteEndpoint.ToString());
                _isComplete = true;
            }
            catch (Exception e)
            {
                Logger.LogDebug(e, "err happen while listen {remoteEndPoint}", RemoteEndpoint.ToString());
            }
        }, tokenSource.Token);
        Logger.LogDebug("start stream handshake to {remoteEndPoint}", RemoteEndpoint.ToString());
        var handshake = await _handshakeProvider.GetHandshakeAsync();
        var handShakeReply = await HandShakeAsync(new HandshakeRequest { Handshake = handshake });
        if (!await ProcessHandshakeReplyAsync(handShakeReply, RemoteEndpoint))
        {
            await DisconnectAsync(true);
            Logger.LogDebug("stream handshake failed, and return {remoteEndPoint} successful", RemoteEndpoint.ToString());
            return false;
        }

        InboundSessionId = handshake.SessionId.ToByteArray();
        Info.SessionId = handShakeReply.Handshake.SessionId.ToByteArray();
        Logger.LogDebug("streaming Handshake to {remoteEndPoint} successful.sessionInfo {InboundSessionId} {SessionId}", RemoteEndpoint.ToString(), InboundSessionId.ToHex(), Info.SessionId.ToHex());
        _isComplete = false;
        _streamListenTaskTokenSource = tokenSource;
        return true;
    }

    public override async Task DisconnectAsync(bool gracefulDisconnect)
    {
        _isComplete = true;
        _sendStreamJobs.Complete();
        try
        {
            await _duplexStreamingCall?.RequestStream?.CompleteAsync();
        }
        catch (Exception)
        {
            // swallow the exception, we don't care because we're disconnecting.
        }

        _duplexStreamingCall?.Dispose();
        _streamListenTaskTokenSource?.Cancel();
        await base.DisconnectAsync(gracefulDisconnect);
    }

    private async Task<HandshakeReply> HandShakeAsync(HandshakeRequest request)
    {
        var metadata = new Metadata
        {
            { GrpcConstants.RetryCountMetadataKey, "0" },
            { GrpcConstants.TimeoutMetadataKey, UpdateHandshakeTimeout.ToString() },
        };
        var grpcRequest = new GrpcRequest { ErrorMessage = "handshake failed." };
        var reply = await RequestAsync(() => StreamRequestAsync(MessageType.HandShake, request, metadata), grpcRequest, true);
        return HandshakeReply.Parser.ParseFrom(reply.Message);
    }

    public override async Task ConfirmHandshakeAsync()
    {
        var request = new GrpcRequest { ErrorMessage = "Could not send confirm handshake." };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, UpdateHandshakeTimeout.ToString() },
        };
        await RequestAsync(() => StreamRequestAsync(MessageType.ConfirmHandShake, new ConfirmHandshakeRequest(), data), request, true);
    }

    protected override async Task BroadcastBlockAsync(BlockWithTransactions blockWithTransactions)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast block failed." };
        await RequestAsync(() => StreamRequestAsync(MessageType.BlockBroadcast, blockWithTransactions), request);
    }

    protected override async Task SendAnnouncementAsync(BlockAnnouncement header)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast block announcement failed." };
        await RequestAsync(() => StreamRequestAsync(MessageType.AnnouncementBroadcast, header), request);
    }

    protected override async Task SendTransactionAsync(Transaction transaction)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast transaction failed." };
        await RequestAsync(() => StreamRequestAsync(MessageType.TransactionBroadcast, transaction), request);
    }

    public override async Task SendLibAnnouncementAsync(LibAnnouncement libAnnouncement)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast lib announcement failed." };
        await RequestAsync(() => StreamRequestAsync(MessageType.LibAnnouncementBroadcast, libAnnouncement), request);
    }

    public override async Task<List<BlockWithTransactions>> GetBlocksAsync(Hash firstHash, int count)
    {
        var blockRequest = new BlocksRequest { PreviousBlockHash = firstHash, Count = count };
        var blockInfo = $"{{ first: {firstHash}, count: {count} }}";

        var request = new GrpcRequest
        {
            ErrorMessage = $"Get blocks for {blockInfo} failed.",
            MetricName = nameof(MetricNames.GetBlocks),
            MetricInfo = $"Get blocks for {blockInfo}"
        };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, BlocksRequestTimeout.ToString() },
        };
        var listMessage = await RequestAsync(() => StreamRequestAsync(MessageType.RequestBlocks, blockRequest, data), request);
        return BlockList.Parser.ParseFrom(listMessage.Message).Blocks.ToList();
    }

    public override async Task<BlockWithTransactions> GetBlockByHashAsync(Hash hash)
    {
        var blockRequest = new BlockRequest { Hash = hash };

        var request = new GrpcRequest
        {
            ErrorMessage = $"Block request for {hash} failed.",
            MetricName = nameof(MetricNames.GetBlock),
            MetricInfo = $"Block request for {hash}"
        };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, BlockRequestTimeout.ToString() },
        };
        var blockMessage = await RequestAsync(() => StreamRequestAsync(MessageType.RequestBlock, blockRequest, data), request);
        return BlockReply.Parser.ParseFrom(blockMessage.Message).Block;
    }

    public override async Task<NodeList> GetNodesAsync(int count = NetworkConstants.DefaultDiscoveryMaxNodesToRequest)
    {
        var request = new GrpcRequest { ErrorMessage = "Request nodes failed." };
        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, GetNodesTimeout.ToString() },
        };
        var listMessage = await RequestAsync(() => StreamRequestAsync(MessageType.GetNodes, new NodesRequest { MaxCount = count }, data), request);
        return NodeList.Parser.ParseFrom(listMessage.Message);
    }

    public override async Task CheckHealthAsync()
    {
        await base.CheckHealthAsync();
        var request = new GrpcRequest { ErrorMessage = "Check health failed." };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, CheckHealthTimeout.ToString() },
        };
        await RequestAsync(() => StreamRequestAsync(MessageType.HealthCheck, new HealthCheckRequest(), data), request);
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

    private async Task WriteStreamJobAsync(StreamJob job)
    {
        //avoid write stream concurrency
        try
        {
            if (job.StreamMessage == null) return;
            Logger.LogDebug("write request={requestId} {streamType}-{messageType}", job.StreamMessage.RequestId, job.StreamMessage.StreamType, job.StreamMessage.MessageType);
            await _clientStreamWriter.WriteAsync(job.StreamMessage);
        }
        catch (RpcException e)
        {
            HandleStreamRpcException(e, job);
            return;
        }

        job.SendCallback?.Invoke(null);
    }

    protected virtual async void HandleStreamRpcException(RpcException e, StreamJob job)
    {
        if (await RebuildStreamAsync())
        {
            //rebuild and retry
            await _clientStreamWriter.WriteAsync(job.StreamMessage);
            return;
        }

        _isComplete = true; //inform to rebuild next time
    }

    protected async Task<bool> RebuildStreamAsync()
    {
        if (!_isComplete) return true;
        try
        {
            _streamListenTaskTokenSource?.Cancel();
            await BuildStreamAndListenAsync();
        }
        catch (Exception e)
        {
            Logger.LogDebug(e, "rebuild failed, {peer}", this);
            return false;
        }

        return true;
    }

    protected async Task<TResp> RequestAsync<TResp>(Func<Task<TResp>> func, GrpcRequest request, bool skipCheck = false)
    {
        if (!skipCheck && (!IsReady || !await RebuildStreamAsync()))
            throw new NetworkException($"Dropping request, peer is not ready - {this}.", NetworkExceptionType.NotConnected);
        var recordRequestTime = !string.IsNullOrEmpty(request.MetricName);
        var requestStartTime = TimestampHelper.GetUtcNow();
        try
        {
            var resp = await func();
            if (recordRequestTime)
                RecordMetric(request, requestStartTime, (TimestampHelper.GetUtcNow() - requestStartTime).Milliseconds());
            return resp;
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, request.ErrorMessage);
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
            throw;
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
                await DisconnectAsync(true);
            throw;
        }
    }

    protected async Task<StreamMessage> StreamRequestAsync(MessageType messageType, IMessage message, Metadata header = null)
    {
        var requestId = CommonHelper.GenerateRequestId();
        var streamMessage = new StreamMessage { StreamType = StreamType.Request, MessageType = messageType, RequestId = requestId, Message = message.ToByteString() };
        AddAllHeaders(streamMessage, header);
        var promise = new TaskCompletionSource<StreamMessage>();
        await _streamTaskResourcePool.RegistryTaskPromiseAsync(requestId, messageType, promise);
        await _sendStreamJobs.SendAsync(new StreamJob
        {
            StreamMessage = streamMessage, SendCallback = ex =>
            {
                if (ex != null) throw ex;
            }
        });
        return await _streamTaskResourcePool.GetResultAsync(promise, requestId, GetTimeOutFromHeader(header));
    }

    private void AddAllHeaders(StreamMessage streamMessage, Metadata metadata = null)
    {
        foreach (var kv in _peerMeta)
        {
            streamMessage.Meta[kv.Key] = kv.Value;
        }

        streamMessage.Meta[GrpcConstants.SessionIdMetadataKey] = OutboundSessionId.ToHex();
        if (metadata == null) return;
        foreach (var e in metadata)
        {
            if (e.IsBinary)
            {
                streamMessage.Meta[e.Key] = e.ValueBytes.ToHex();
                continue;
            }

            streamMessage.Meta[e.Key] = e.Value;
        }
    }

    private int GetTimeOutFromHeader(Metadata header)
    {
        if (header == null) return StreamWaitTime;
        var t = header.Get(GrpcConstants.TimeoutMetadataKey)?.Value;
        return t == null ? StreamWaitTime : int.Parse(t);
    }

    private async Task<bool> ProcessHandshakeReplyAsync(HandshakeReply handshakeReply, DnsEndPoint remoteEndpoint)
    {
        // verify handshake
        if (handshakeReply.Error != HandshakeError.HandshakeOk)
        {
            Logger.LogWarning("Handshake error: {remoteEndpoint} {Error}.", remoteEndpoint, handshakeReply.Error);

            return false;
        }

        if (await _handshakeProvider.ValidateHandshakeAsync(handshakeReply.Handshake) ==
            HandshakeValidationResult.Ok) return true;
        Logger.LogWarning("Connect error: {remoteEndpoint} {handshakeReply}.", remoteEndpoint, handshakeReply);
        return false;
    }
}