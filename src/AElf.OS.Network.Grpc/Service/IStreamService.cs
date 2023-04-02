using System;
using System.Collections.Generic;
using System.Linq;
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
        if (!ValidRequestContext(request, context)) return;
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
            if (!ValidReplyMetaContext(message, streamPeer)) return;
            await DoProcessAsync(message, streamPeer?.GetResponseStream(), new StreamMessageMetaStreamContext(message.Meta));
            Logger.LogInformation("handle stream call success, clientPubKey={clientPubKey} request={requestId} {streamType}-{messageType}", clientPubKey, message.RequestId, message.StreamType, message.MessageType);
        }
        catch (RpcException ex)
        {
            await HandleNetworkExceptionAsync(peer, streamPeer.HandleRpcException(ex, $"Could not broadcast to {this}: "));
            Logger.LogError(ex, "handle stream call failed, clientPubKey={clientPubKey} request={requestId} {streamType}-{messageType}", clientPubKey, message.RequestId, message.StreamType, message.MessageType);
        }
        catch (Exception ex)
        {
            await HandleNetworkExceptionAsync(peer, new NetworkException("Unknown exception during broadcast.", ex));
            Logger.LogError(ex, "handle stream call failed, clientPubKey={clientPubKey} request={requestId} {streamType}-{messageType}", clientPubKey, message.RequestId, message.StreamType, message.MessageType);
        }
    }

    private async Task DoProcessAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, IStreamContext streamContext)
    {
        Logger.LogInformation("receive {requestId} {streamType}-{messageType}", request.RequestId, request.StreamType, request.MessageType);
        switch (request.StreamType)
        {
            case StreamType.Reply:
                _streamTaskResourcePool.TrySetResult(request.RequestId, request);
                return;
            case StreamType.Request:
                await ProcessRequestAsync(request, responseStream, streamContext);
                return;
            case StreamType.Unknown:
            default:
                Logger.LogWarning("unhandled stream request: {requestId} {streamType}-{messageType}", request.RequestId, request.StreamType, request.MessageType);
                return;
        }
    }
    

    private async Task ProcessRequestAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, IStreamContext streamContext)
    {
        try
        {
            var method = _streamMethods.FirstOrDefault(e => e.Method == request.MessageType);
            if (method == null) Logger.LogWarning("unhandled stream request: {requestId} {streamType}-{messageType}", request.RequestId, request.StreamType, request.MessageType);
            var reply = method == null ? new VoidReply() : await method.InvokeAsync(request, streamContext, responseStream);
            var peer = _peerPool.FindPeerByPublicKey(streamContext.GetPubKey());
            var message = new StreamMessage
            {
                StreamType = StreamType.Reply, MessageType = request.MessageType,
                RequestId = request.RequestId, Message = reply == null ? new VoidReply().ToByteString() : reply.ToByteString()
            };
            if (peer != null)
                message.Meta.Add(GrpcConstants.SessionIdMetadataKey, peer.Info.SessionId.ToHex());
            else
                Logger.LogWarning("peer not found {requestId} {streamType}-{messageType}", request.RequestId, request.StreamType, request.MessageType);
            await responseStream.WriteAsync(message);
        }
        catch (Exception e)
        {
            Logger.LogWarning(e, "request failed {requestId} {streamType}-{messageType}", request.RequestId, request.StreamType, request.MessageType);
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

    private bool ValidRequestContext(StreamMessage message, ServerCallContext context)
    {
        if (!IsNeedAuth(message)) return true;
        var pubKey = context.GetPublicKey();
        var peer = _peerPool.FindPeerByPublicKey(pubKey);
        if (peer == null)
        {
            Logger.LogWarning("Could not find peer {requestId} {pubkey} {streamType}-{messageType}", message.RequestId, pubKey, message.StreamType, message.MessageType);
            return false;
        }

        // check that the peers session is equal to one announced in the headers
        var sessionId = context.GetSessionId();

        if (peer.InboundSessionId.BytesEqual(sessionId))
        {
            context.RequestHeaders.Add(new Metadata.Entry(GrpcConstants.PeerInfoMetadataKey, $"{peer}"));
            return true;
        }

        if (peer.InboundSessionId == null)
        {
            Logger.LogWarning("Wrong inbound session id {peer}, {streamType}-{MessageType}", context.Peer, message.StreamType, message.MessageType);
            return false;
        }

        if (sessionId == null)
        {
            Logger.LogWarning("Wrong inbound session id {peer}, {requestId}", peer, message.RequestId);
            return false;
        }

        Logger.LogWarning("Unequal session id, ({InboundSessionId} {infoSession} vs {sessionId}) {streamType}-{MessageType} {pubkey}  {Peer}", peer.InboundSessionId.ToHex(), peer.Info.SessionId.ToHex(),
            sessionId.ToHex(), message.StreamType, message.MessageType, peer.Info.Pubkey, peer);
        return false;
    }

    private bool ValidReplyMetaContext(StreamMessage message, GrpcStreamPeer streamPeer)
    {
        if (!IsNeedAuth(message)) return true;
        if (streamPeer == null)
        {
            Logger.LogWarning("Could not find peer {requestId} {streamType}-{messageType}", message.RequestId, message.StreamType, message.MessageType);
            return false;
        }

        var sessionIdHex = message.Meta[GrpcConstants.SessionIdMetadataKey];
        if (sessionIdHex == null)
        {
            Logger.LogWarning("Wrong context session id {pubkey}, {MessageType}, {peer}", streamPeer.Info.Pubkey, message.MessageType, streamPeer);
            return false;
        }

        // check that the peers session is equal to one announced in the headers
        if (streamPeer.InboundSessionId.ToHex().Equals(sessionIdHex))
        {
            message.Meta[GrpcConstants.PeerInfoMetadataKey] = streamPeer.ToString();
            return true;
        }

        if (streamPeer.InboundSessionId == null)
        {
            Logger.LogWarning("Wrong inbound session id {peer}, {requestId}", streamPeer, message.RequestId);
            return false;
        }

        Logger.LogWarning("Unequal session id, ({InboundSessionId} {infoSession} vs {sessionId}) {streamType}-{MessageType} {pubkey}  {Peer}", streamPeer.InboundSessionId.ToHex(), streamPeer.Info.SessionId.ToHex(),
            sessionIdHex, message.StreamType, message.MessageType, streamPeer.Info.Pubkey, streamPeer);
        return false;
    }

    private bool IsNeedAuth(StreamMessage streamMessage)
    {
        if (streamMessage.StreamType == StreamType.Reply) return false;
        return streamMessage.MessageType != MessageType.Ping &&
               streamMessage.MessageType != MessageType.HandShake &&
               streamMessage.MessageType != MessageType.HealthCheck;
    }
}