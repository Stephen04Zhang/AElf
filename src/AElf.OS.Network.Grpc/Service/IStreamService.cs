using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AElf.OS.Network.Application;
using AElf.OS.Network.Infrastructure;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Volo.Abp.DependencyInjection;

namespace AElf.OS.Network.Grpc;

public interface IStreamService
{
    Task ProcessStreamReplyAsync(ByteString reply, string clientPubKey);
    Task ProcessStreamRequestAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, ServerCallContext context);
}

public class StreamService : IStreamService, ISingletonDependency
{
    public ILogger<StreamService> Logger { get; set; }
    private readonly IConnectionService _connectionService;
    private readonly IStreamTaskResourcePool _streamTaskResourcePool;
    private readonly IPeerPool _peerPool;
    private readonly ITaskQueueManager _taskQueueManager;
    private readonly IEnumerable<IStreamMethod> _streamMethods;


    public StreamService(IConnectionService connectionService, IStreamTaskResourcePool streamTaskResourcePool, IPeerPool peerPool, ITaskQueueManager taskQueueManager,
        IEnumerable<IStreamMethod> streamMethods)
    {
        Logger = NullLogger<StreamService>.Instance;
        _connectionService = connectionService;
        _streamTaskResourcePool = streamTaskResourcePool;
        _peerPool = peerPool;
        _taskQueueManager = taskQueueManager;
        _streamMethods = streamMethods;
    }

    public async Task ProcessStreamRequestAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, ServerCallContext context)
    {
        //todo do auth here
        await DoProcessAsync(request, responseStream, new ServiceStreamContext(context));
    }

    public async Task ProcessStreamReplyAsync(ByteString reply, string clientPubKey)
    {
        var message = StreamMessage.Parser.ParseFrom(reply);
        Logger.LogInformation("receive {requestId} {streamType} {meta}", message.RequestId, message.StreamType, message.Meta);

        var peer = _peerPool.FindPeerByPublicKey(clientPubKey);
        var streamPeer = peer as GrpcStreamPeer;
        try
        {
            if (!AuthMetaContext(message, streamPeer)) return;
            await DoProcessAsync(message, streamPeer?.GetResponseStream(), new StreamMessageMetaStreamContext(message.Meta));
            Logger.LogInformation("handle stream call success, clientPubKey={clientPubKey} request={requestId} {streamType}", clientPubKey, message.RequestId, message.StreamType);
        }
        catch (RpcException ex)
        {
            await HandleNetworkExceptionAsync(peer, streamPeer.HandleRpcException(ex, $"Could not broadcast to {this}: "));
            Logger.LogError(ex, "handle stream call failed, clientPubKey={clientPubKey} request={requestId} {streamType}", clientPubKey, message.RequestId, message.StreamType);
        }
        catch (Exception ex)
        {
            await HandleNetworkExceptionAsync(peer, new NetworkException("Unknown exception during broadcast.", ex));
            Logger.LogError(ex, "handle stream call failed, clientPubKey={clientPubKey} request={requestId} {streamType}", clientPubKey, message.RequestId, message.StreamType);
        }
    }

    private async Task DoProcessAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, IStreamContext streamContext)
    {
        Logger.LogInformation("receive {requestId} {streamType}", request.RequestId, request.StreamType);
        switch (request.StreamType)
        {
            case StreamType.Reply:
                Logger.LogInformation("receive {RequestId} {streamType}", request.RequestId, request.StreamType);
                _streamTaskResourcePool.TrySetResult(request.RequestId, request);
                return;
            case StreamType.Request:
                await ProcessRequestAsync(request, responseStream, streamContext);
                return;
            case StreamType.Unknown:
            default:
                Logger.LogWarning("unhandled stream request: {requestId} {type}", request.RequestId, request.StreamType);
                return;
        }
    }

    private async Task ProcessRequestAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, IStreamContext streamContext)
    {
        try
        {
            var method = _streamMethods.FirstOrDefault(e => e.Method == request.MessageType);
            if (method == null) Logger.LogWarning("unhandled stream request: {requestId} {type}", request.RequestId, request.StreamType);
            var reply = method == null ? new VoidReply() : await method.InvokeAsync(request, streamContext, responseStream);
            await responseStream.WriteAsync(new StreamMessage
                { StreamType = StreamType.Reply, MessageType = request.MessageType, RequestId = request.RequestId, Message = reply == null ? new VoidReply().ToByteString() : reply.ToByteString() });
        }
        catch (Exception e)
        {
            Logger.LogWarning(e, "request failed {requestId} {streamType}", request.RequestId, request.StreamType);
            throw;
        }
    }


    private async Task HandleNetworkExceptionAsync(IPeer peer, NetworkException exception)
    {
        if (exception.ExceptionType == NetworkExceptionType.Unrecoverable)
        {
            Logger.LogInformation(exception, $"Removing unrecoverable {peer}.");
            await _connectionService.TrySchedulePeerReconnectionAsync(peer);
        }
        else if (exception.ExceptionType == NetworkExceptionType.PeerUnstable)
        {
            Logger.LogDebug(exception, $"Queuing peer for reconnection {peer.RemoteEndpoint}.");
            _taskQueueManager.Enqueue(async () => await RecoverPeerAsync(peer), NetworkConstants.PeerReconnectionQueueName);
        }
    }

    private async Task RecoverPeerAsync(IPeer peer)
    {
        if (peer.IsReady) // peer recovered already
            return;
        var success = await peer.TryRecoverAsync();
        if (!success) await _connectionService.TrySchedulePeerReconnectionAsync(peer);
    }


    private bool AuthMetaContext(StreamMessage message, GrpcStreamPeer streamPeer)
    {
        if (!IsNeedAuth(message)) return true;
        if (streamPeer == null)
        {
            Logger.LogWarning("Could not find peer {requestId}", message.RequestId);
            return false;
        }

        string sessionStr = message.Meta[GrpcConstants.SessionIdMetadataKey];
        if (sessionStr == null)
        {
            Logger.LogWarning("Wrong context session id {pubkey}, {MessageType}, {peer}", streamPeer.Info.Pubkey, message.MessageType, streamPeer);
            return false;
        }

        // check that the peers session is equal to one announced in the headers
        var sessionId = Encoding.ASCII.GetBytes(sessionStr);
        if (streamPeer.InboundSessionId.BytesEqual(sessionId)) return true;
        if (streamPeer.InboundSessionId == null)
        {
            Logger.LogWarning("Wrong inbound session id {peer}, {requestId}", streamPeer, message.RequestId);
            return false;
        }

        Logger.LogWarning("Unequal session id, {Peer} ({InboundSessionId} vs {sessionId}) {MessageType} {pubkey}", streamPeer, streamPeer.InboundSessionId.ToHex(), sessionId.ToHex(), message.MessageType, streamPeer.Info.Pubkey);
        return false;
    }


    private bool IsNeedAuth(StreamMessage streamMessage)
    {
        return streamMessage.StreamType == StreamType.Request && streamMessage.MessageType is not MessageType.Ping or MessageType.HandShake;
    }
}