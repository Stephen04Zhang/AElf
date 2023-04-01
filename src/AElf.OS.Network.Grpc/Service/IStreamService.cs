using System;
using System.Text;
using System.Threading.Tasks;
using AElf.OS.Network.Application;
using AElf.OS.Network.Grpc.Helpers;
using AElf.OS.Network.Infrastructure;
using AElf.Types;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Volo.Abp.DependencyInjection;

namespace AElf.OS.Network.Grpc;

public interface IStreamService
{
    Task ProcessReplyAsync(ByteString reply, string clientPubKey);
    Task ProcessRequestAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, ServerCallContext context);
}

public class StreamService : IStreamService, ISingletonDependency
{
    public ILogger<StreamService> Logger { get; set; }
    private readonly IConnectionService _connectionService;
    private readonly IStreamTaskResourcePool _streamTaskResourcePool;
    private readonly IPeerPool _peerPool;
    private readonly ITaskQueueManager _taskQueueManager;
    private readonly IServiceProvider _serviceProvider;


    public StreamService(IConnectionService connectionService, IStreamTaskResourcePool streamTaskResourcePool, IPeerPool peerPool, ITaskQueueManager taskQueueManager, IServiceProvider serviceProvider)
    {
        Logger = NullLogger<StreamService>.Instance;
        _connectionService = connectionService;
        _streamTaskResourcePool = streamTaskResourcePool;
        _peerPool = peerPool;
        _taskQueueManager = taskQueueManager;
        _serviceProvider = serviceProvider;
    }

    public async Task ProcessRequestAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, ServerCallContext context)
    {
        await DoProcessAsync(request, responseStream, new ServiceContextProvider(context));
    }

    public async Task ProcessReplyAsync(ByteString reply, string clientPubKey)
    {
        var message = StreamMessage.Parser.ParseFrom(reply);
        Logger.LogInformation("receive {requestId} {streamType} {meta}", message.RequestId, message.StreamType, message.Meta);

        var peer = _peerPool.FindPeerByPublicKey(clientPubKey);
        var streamPeer = peer as GrpcStreamPeer;
        try
        {
            if (!AuthMetaContext(message, streamPeer)) return;
            await DoProcessAsync(message, streamPeer?.GetResponseStream(), new StreamMessageMetaContextProvider(message.Meta));
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

    private async Task DoProcessAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, IContextProvider contextProvider)
    {
        Logger.LogInformation("receive {requestId} {streamType}", request.RequestId, request.StreamType);
        switch (request.StreamType)
        {
            case StreamType.Reply:
                Logger.LogInformation("receive {RequestId} {streamType}", request.RequestId, request.StreamType);
                _streamTaskResourcePool.TrySetResult(request.RequestId, request);
                return;
            case StreamType.Request:
                await ProcessRequestAsync(request, responseStream, contextProvider);
                return;
            case StreamType.Unknown:
            default:
                Logger.LogWarning("unhandled stream request: {requestId} {type}", request.RequestId, request.StreamType);
                return;
        }
    }

    private async Task ProcessRequestAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, IContextProvider contextProvider)
    {
        try
        {
            IMessage reply = null;
            switch (request.MessageType)
            {
                case MessageType.HandShake:
                    var context = (contextProvider as ServiceContextProvider).Context;
                    reply = await ProcessHandShakeAsync(request, responseStream, context);
                    break;
                case MessageType.GetNodes:
                    reply = await _serviceProvider.GetNodesAsync(NodesRequest.Parser.ParseFrom(request.Message), contextProvider.GetPeerInfo());
                    break;
                case MessageType.HealthCheck:
                    reply = new HealthCheckReply();
                    break;
                case MessageType.Ping:
                    break;
                case MessageType.Disconnect:
                    await _serviceProvider.DisconnectAsync(DisconnectReason.Parser.ParseFrom(request.Message), request.RequestId, contextProvider.GetPeerInfo(), contextProvider.GetPubKey());
                    break;
                case MessageType.ConfirmHandShake:
                    await _serviceProvider.ConfirmHandshakeAsync(request.RequestId, contextProvider.GetPeerInfo(), contextProvider.GetPubKey());
                    break;

                case MessageType.RequestBlock:
                    reply = await _serviceProvider.RequestBlockAsync(request.RequestId, BlockRequest.Parser.ParseFrom(request.Message), contextProvider.GetPeerInfo(), contextProvider.GetPubKey());
                    break;
                case MessageType.RequestBlocks:
                    reply = await _serviceProvider.RequestBlocksAsync(request.RequestId, BlocksRequest.Parser.ParseFrom(request.Message), contextProvider.GetPeerInfo());
                    break;

                case MessageType.BlockBroadcast:
                    await _serviceProvider.ProcessBlockAsync(BlockWithTransactions.Parser.ParseFrom(request.Message), contextProvider.GetPubKey());
                    break;
                case MessageType.AnnouncementBroadcast:
                    await _serviceProvider.ProcessAnnouncementAsync(BlockAnnouncement.Parser.ParseFrom(request.Message), contextProvider.GetPubKey());
                    break;
                case MessageType.TransactionBroadcast:
                    await _serviceProvider.ProcessTransactionAsync(Transaction.Parser.ParseFrom(request.Message), contextProvider.GetPubKey());
                    break;
                case MessageType.LibAnnouncementBroadcast:
                    await _serviceProvider.ProcessLibAnnouncementAsync(LibAnnouncement.Parser.ParseFrom(request.Message), contextProvider.GetPubKey());
                    break;
                case MessageType.Any:
                default:
                    Logger.LogWarning("unhandled stream request: {requestId} {type}", request.RequestId, request.StreamType);
                    break;
            }

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

    private async Task<HandshakeReply> ProcessHandShakeAsync(StreamMessage request, IAsyncStreamWriter<StreamMessage> responseStream, ServerCallContext context)
    {
        try
        {
            Logger.LogDebug($"Peer {context.Peer} has requested a handshake.");

            if (context.AuthContext?.Properties != null)
                foreach (var authProperty in context.AuthContext.Properties)
                    Logger.LogDebug($"Auth property: {authProperty.Name} -> {authProperty.Value}");

            if (!GrpcEndPointHelper.ParseDnsEndPoint(context.Peer, out var peerEndpoint))
                return new HandshakeReply { Error = HandshakeError.InvalidConnection };
            return await _connectionService.DoHandshakeByStreamAsync(peerEndpoint, responseStream, HandshakeRequest.Parser.ParseFrom(request.Message).Handshake);
        }
        catch (Exception e)
        {
            Logger.LogWarning(e, $"Handshake failed - {context.Peer}: ");
            throw;
        }
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

public interface IContextProvider
{
    string GetPeerInfo();
    string GetPubKey();
    byte[] GetSessionId();
}

public class ServiceContextProvider : IContextProvider
{
    public ServerCallContext Context;

    public ServiceContextProvider(ServerCallContext context)
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

public class StreamMessageMetaContextProvider : IContextProvider
{
    private MapField<string, string> _meta;

    public StreamMessageMetaContextProvider(MapField<string, string> meta)
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