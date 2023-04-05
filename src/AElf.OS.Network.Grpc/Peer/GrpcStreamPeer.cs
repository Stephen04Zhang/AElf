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
using AElf.OS.Network.Grpc.Helpers;
using AElf.OS.Network.Protocol.Types;
using AElf.Types;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AElf.OS.Network.Grpc;

public class GrpcStreamPeer : GrpcPeer
{
    private const int StreamWaitTime = 500;
    private readonly AsyncDuplexStreamingCall<StreamMessage, StreamMessage> _duplexStreamingCall;
    private CancellationTokenSource _streamListenTaskTokenSource;
    private readonly IAsyncStreamWriter<StreamMessage> _clientStreamWriter;

    private readonly IStreamTaskResourcePool _streamTaskResourcePool;
    private readonly Dictionary<string, string> _peerMeta;

    protected readonly ActionBlock<StreamJob> _sendStreamJobs;
    public ILogger<GrpcStreamPeer> Logger { get; set; }

    public GrpcStreamPeer(GrpcClient client, DnsEndPoint remoteEndpoint, PeerConnectionInfo peerConnectionInfo,
        AsyncDuplexStreamingCall<StreamMessage, StreamMessage> duplexStreamingCall,
        IAsyncStreamWriter<StreamMessage> clientStreamWriter,
        IStreamTaskResourcePool streamTaskResourcePool, Dictionary<string, string> peerMeta) : base(client,
        remoteEndpoint, peerConnectionInfo)
    {
        _duplexStreamingCall = duplexStreamingCall;
        _clientStreamWriter = duplexStreamingCall?.RequestStream ?? clientStreamWriter;
        _streamTaskResourcePool = streamTaskResourcePool;
        _peerMeta = peerMeta;
        _sendStreamJobs = new ActionBlock<StreamJob>(WriteStreamJobAsync);
        Logger = NullLogger<GrpcStreamPeer>.Instance;
    }


    public void StartServe(CancellationTokenSource listenTaskTokenSource)
    {
        _streamListenTaskTokenSource = listenTaskTokenSource;
    }

    public override async Task DisconnectAsync(bool gracefulDisconnect)
    {
        _sendStreamJobs.Complete();
        await _duplexStreamingCall?.RequestStream?.CompleteAsync();
        _duplexStreamingCall?.Dispose();
        _streamListenTaskTokenSource?.Cancel();
        await base.DisconnectAsync(gracefulDisconnect);
    }

    public async Task<HandshakeReply> HandShakeAsync(HandshakeRequest request)
    {
        var metadata = new Metadata
        {
            { GrpcConstants.RetryCountMetadataKey, "0" },
        };
        var grpcRequest = new GrpcRequest { ErrorMessage = "handshake failed." };
        var reply = await RequestAsync(() => StreamRequestAsync(MessageType.HandShake, request, AddPeerMeta(metadata)), grpcRequest);
        return HandshakeReply.Parser.ParseFrom(reply.Message);
    }

    public override async Task ConfirmHandshakeAsync()
    {
        var request = new GrpcRequest { ErrorMessage = "Could not send confirm handshake." };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, UpdateHandshakeTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        await RequestAsync(() => StreamRequestAsync(MessageType.ConfirmHandShake, new ConfirmHandshakeRequest(), AddPeerMeta(data)), request);
    }

    public override async Task BroadcastBlockAsync(BlockWithTransactions blockWithTransactions)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast block failed." };
        await RequestAsync(() => StreamRequestAsync(MessageType.BlockBroadcast, blockWithTransactions, AddPeerMeta()), request);
    }

    public override async Task SendAnnouncementAsync(BlockAnnouncement header)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast block announcement failed." };
        await RequestAsync(() => StreamRequestAsync(MessageType.AnnouncementBroadcast, header, AddPeerMeta()), request);
    }

    public override async Task SendTransactionAsync(Transaction transaction)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast transaction failed." };
        await RequestAsync(() => StreamRequestAsync(MessageType.TransactionBroadcast, transaction, AddPeerMeta()), request);
    }

    public override async Task SendLibAnnouncementAsync(LibAnnouncement libAnnouncement)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast lib announcement failed." };
        await RequestAsync(() => StreamRequestAsync(MessageType.LibAnnouncementBroadcast, libAnnouncement, AddPeerMeta()), request);
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
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        var listMessage = await RequestAsync(() => StreamRequestAsync(MessageType.RequestBlocks, blockRequest, AddPeerMeta(data)), request);
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
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        var blockMessage = await RequestAsync(() => StreamRequestAsync(MessageType.RequestBlock, blockRequest, AddPeerMeta(data)), request);
        return BlockWithTransactions.Parser.ParseFrom(blockMessage.Message);
    }

    public override async Task<NodeList> GetNodesAsync(int count = NetworkConstants.DefaultDiscoveryMaxNodesToRequest)
    {
        var request = new GrpcRequest { ErrorMessage = "Request nodes failed." };
        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, GetNodesTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        var listMessage = await RequestAsync(() => StreamRequestAsync(MessageType.GetNodes, new NodesRequest { MaxCount = count }, AddPeerMeta(data)), request);
        return NodeList.Parser.ParseFrom(listMessage.Message);
    }

    public override async Task CheckHealthAsync()
    {
        await base.CheckHealthAsync();
        var request = new GrpcRequest { ErrorMessage = "Check health failed." };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, CheckHealthTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        await RequestAsync(() => StreamRequestAsync(MessageType.HealthCheck, new HealthCheckRequest(), AddPeerMeta(data)), request);
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

    private async Task WriteStreamJobAsync(StreamJob job)
    {
        //avoid write stream concurrency
        try
        {
            if (job.StreamMessage == null) return;
            Logger.LogDebug("write request={requestId} {streamType}-{messageType}", job.StreamMessage.RequestId, job.StreamMessage.StreamType, job.StreamMessage.MessageType);
            await _clientStreamWriter.WriteAsync(job.StreamMessage);
        }
        catch (RpcException ex)
        {
            job.SendCallback?.Invoke(HandleRpcException(ex, $"Could not write to stream to {this}: "));
            await Task.Delay(StreamRecoveryWaitTime);
            return;
        }

        job.SendCallback?.Invoke(null);
    }

    protected Metadata AddPeerMeta(Metadata metadata = null)
    {
        metadata ??= new Metadata();
        foreach (var kv in _peerMeta)
        {
            metadata.Add(kv.Key, kv.Value);
        }

        metadata.Add(GrpcConstants.SessionIdMetadataKey, Info.SessionId);
        return metadata;
    }

    protected async Task<TResp> RequestAsync<TResp>(Func<Task<TResp>> func, GrpcRequest request)
    {
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

    protected async Task<StreamMessage> StreamRequestAsync(MessageType messageType, IMessage message, Metadata header)
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

    private void AddAllHeaders(StreamMessage streamMessage, Metadata header)
    {
        if (header == null) return;

        foreach (var e in header)
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
}