using System;
using System.Threading.Tasks;
using AElf.OS.Network.Grpc.Helpers;
using AElf.Types;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Volo.Abp.DependencyInjection;

namespace AElf.OS.Network.Grpc;

public interface IStreamMethod
{
    MessageType Method { get; }
    Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null);
}

public abstract class StreamMethod : IStreamMethod
{
    public abstract MessageType Method { get; }
    protected readonly IConnectionService ConnectionService;
    protected readonly IGrpcRequestProcessor GrpcRequestProcessor;

    protected StreamMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor)
    {
        ConnectionService = connectionService;
        GrpcRequestProcessor = grpcRequestProcessor;
    }

    public abstract Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null);
}

public class HandShakeMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.HandShake;
    public ILogger<HandShakeMethod> Logger { get; set; }

    public HandShakeMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
        Logger = NullLogger<HandShakeMethod>.Instance;
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        if (streamContext is not ServiceStreamContext serviceStreamContext) return new VoidReply();
        var context = serviceStreamContext.Context;
        try
        {
            Logger.LogDebug($"Peer {context.Peer} has requested a handshake.");

            if (context.AuthContext?.Properties != null)
                foreach (var authProperty in context.AuthContext.Properties)
                    Logger.LogDebug($"Auth property: {authProperty.Name} -> {authProperty.Value}");

            if (!GrpcEndPointHelper.ParseDnsEndPoint(context.Peer, out var peerEndpoint))
                return new HandshakeReply { Error = HandshakeError.InvalidConnection };
            return await ConnectionService.DoHandshakeByStreamAsync(peerEndpoint, responseStream, HandshakeRequest.Parser.ParseFrom(request.Message).Handshake);
        }
        catch (Exception e)
        {
            Logger.LogWarning(e, $"Handshake failed - {context.Peer}: ");
            throw;
        }
    }
}

public class GetNodesMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.GetNodes;

    public GetNodesMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        return await GrpcRequestProcessor.GetNodesAsync(NodesRequest.Parser.ParseFrom(request.Message), streamContext.GetPeerInfo());
    }
}

public class HealthCheckMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.HealthCheck;

    public HealthCheckMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        return new HealthCheckReply();
    }
}

public class PingMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.Ping;

    public PingMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        return new PongReply();
    }
}

public class DisconnectMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.Disconnect;

    public DisconnectMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        await GrpcRequestProcessor.DisconnectAsync(DisconnectReason.Parser.ParseFrom(request.Message), request.RequestId, streamContext.GetPeerInfo(), streamContext.GetPubKey());
        return new VoidReply();
    }
}

public class ConfirmHandShakeMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.ConfirmHandShake;

    public ConfirmHandShakeMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        await GrpcRequestProcessor.ConfirmHandshakeAsync(streamContext.GetPeerInfo(), streamContext.GetPubKey(), request.RequestId);
        return new VoidReply();
    }
}

public class RequestBlockMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.RequestBlock;

    public RequestBlockMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        return await GrpcRequestProcessor.GetBlockAsync(BlockRequest.Parser.ParseFrom(request.Message), streamContext.GetPeerInfo(), streamContext.GetPubKey(), request.RequestId);
    }
}

public class RequestBlocksMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.RequestBlocks;

    public RequestBlocksMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        return await GrpcRequestProcessor.GetBlocksAsync(BlocksRequest.Parser.ParseFrom(request.Message), streamContext.GetPeerInfo(), request.RequestId);
    }
}

public class BlockBroadcastMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.BlockBroadcast;

    public BlockBroadcastMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        await GrpcRequestProcessor.ProcessBlockAsync(BlockWithTransactions.Parser.ParseFrom(request.Message), streamContext.GetPubKey());
        return new VoidReply();
    }
}

public class AnnouncementBroadcastMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.AnnouncementBroadcast;

    public AnnouncementBroadcastMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        await GrpcRequestProcessor.ProcessAnnouncementAsync(BlockAnnouncement.Parser.ParseFrom(request.Message), streamContext.GetPubKey());
        return new VoidReply();
    }
}

public class TransactionBroadcastMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.TransactionBroadcast;

    public TransactionBroadcastMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        await GrpcRequestProcessor.ProcessTransactionAsync(Transaction.Parser.ParseFrom(request.Message), streamContext.GetPubKey());
        return new VoidReply();
    }
}

public class LibAnnouncementBroadcastMethod : StreamMethod, ISingletonDependency
{
    public override MessageType Method => MessageType.LibAnnouncementBroadcast;

    public LibAnnouncementBroadcastMethod(IConnectionService connectionService, IGrpcRequestProcessor grpcRequestProcessor) : base(connectionService, grpcRequestProcessor)
    {
    }

    public override async Task<IMessage> InvokeAsync(StreamMessage request, IStreamContext streamContext, IAsyncStreamWriter<StreamMessage> responseStream = null)
    {
        await GrpcRequestProcessor.ProcessLibAnnouncementAsync(LibAnnouncement.Parser.ParseFrom(request.Message), streamContext.GetPubKey());
        return new VoidReply();
    }
}