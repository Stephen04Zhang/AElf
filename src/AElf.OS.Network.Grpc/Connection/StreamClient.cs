using System.Threading.Tasks;
using AElf.OS.Network.Grpc.Helpers;
using AElf.Types;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Volo.Abp.DependencyInjection;

namespace AElf.OS.Network.Grpc;

public class StreamClient
{
    private const int StreamWaitTime = 500;
    public readonly IAsyncStreamWriter<StreamMessage> ClientStreamWriter;
    private readonly IStreamTaskResourcePool _streamTaskResourcePool;
    public ILogger<StreamClient> Logger { get; }

    public StreamClient(IAsyncStreamWriter<StreamMessage> clientStreamWriter, IStreamTaskResourcePool streamTaskResourcePool)
    {
        ClientStreamWriter = clientStreamWriter;
        _streamTaskResourcePool = streamTaskResourcePool;
        Logger = NullLogger<StreamClient>.Instance;
    }

    public async Task<NodeList> GetNodesAsync(NodesRequest nodesRequest, Metadata header)
    {
        var reply = await RequestAsync(MessageType.GetNodes, nodesRequest.ToByteString(), header, GetTimeOutFromHeader(header));
        var nodeList = new NodeList();
        nodeList.MergeFrom(reply.Message);
        return nodeList;
    }

    public async Task<HandshakeReply> HandShakeAsync(HandshakeRequest request, Metadata header)
    {
        var reply = await RequestAsync(MessageType.HandShake, request.ToByteString(), header);
        return HandshakeReply.Parser.ParseFrom(reply.Message);
    }

    public async Task<VoidReply> CheckHealthAsync(Metadata header)
    {
        await RequestAsync(MessageType.HealthCheck, new HealthCheckRequest().ToByteString(), header, GetTimeOutFromHeader(header));
        return new VoidReply();
    }

    public async Task<BlockWithTransactions> RequestBlockAsync(BlockRequest blockRequest, Metadata header)
    {
        var reply = await RequestAsync(MessageType.RequestBlock, blockRequest.ToByteString(), header, GetTimeOutFromHeader(header));
        var blockWithTransactions = new BlockWithTransactions();
        blockWithTransactions.MergeFrom(reply.Message);
        return blockWithTransactions;
    }

    public async Task<BlockList> RequestBlocksAsync(BlocksRequest blockRequest, Metadata header)
    {
        var reply = await RequestAsync(MessageType.RequestBlocks, blockRequest.ToByteString(), header, GetTimeOutFromHeader(header));
        var blockList = new BlockList();
        blockList.MergeFrom(reply.Message);
        return blockList;
    }

    public async Task<VoidReply> DisconnectAsync(DisconnectReason disconnectReason, Metadata header)
    {
        await RequestAsync(MessageType.Disconnect, disconnectReason.ToByteString(), header);
        return new VoidReply();
    }

    public async Task<VoidReply> ConfirmHandshakeAsync(ConfirmHandshakeRequest confirmHandshakeRequest, Metadata header)
    {
        await RequestAsync(MessageType.HandShake, confirmHandshakeRequest.ToByteString(), header, GetTimeOutFromHeader(header));
        return new VoidReply();
    }

    public async Task<VoidReply> BroadcastBlockAsync(BlockWithTransactions blockWithTransactions, Metadata header)
    {
        await RequestAsync(MessageType.BlockBroadcast, blockWithTransactions.ToByteString(), header);
        return new VoidReply();
    }

    public async Task<PongReply> PingAsync(Metadata header)
    {
        var msg = await RequestAsync(MessageType.Ping, new PingRequest().ToByteString(), header);
        return msg == null ? null : PongReply.Parser.ParseFrom(msg.Message);
    }

    public async Task<VoidReply> BroadcastAnnouncementBlockAsync(BlockAnnouncement header, Metadata meta)
    {
        await RequestAsync(MessageType.AnnouncementBroadcast, header.ToByteString(), meta);
        return new VoidReply();
    }

    public async Task<VoidReply> BroadcastTransactionAsync(Transaction transaction, Metadata meta)
    {
        await RequestAsync(MessageType.TransactionBroadcast, transaction.ToByteString(), meta);
        return new VoidReply();
    }

    public async Task<VoidReply> BroadcastLibAnnouncementAsync(LibAnnouncement libAnnouncement, Metadata header)
    {
        await RequestAsync(MessageType.LibAnnouncementBroadcast, libAnnouncement.ToByteString(), header);
        return new VoidReply();
    }

    private int GetTimeOutFromHeader(Metadata header)
    {
        if (header == null) return StreamWaitTime;
        var t = header.Get(GrpcConstants.TimeoutMetadataKey)?.Value;
        return int.Parse(t);
    }

    private async Task<StreamMessage> RequestAsync(MessageType messageType, ByteString reply, Metadata header, int timeout = StreamWaitTime)
    {
        var requestId = CommonHelper.GenerateRequestId();
        var streamMessage = new StreamMessage { StreamType = StreamType.Request, MessageType = messageType, RequestId = requestId, Message = reply };
        AddAllHeaders(streamMessage, header);
        await ClientStreamWriter.WriteAsync(streamMessage);
        _streamTaskResourcePool.RegistryTaskPromise(requestId, messageType, new TaskCompletionSource<StreamMessage>());
        return await _streamTaskResourcePool.GetResultAsync(requestId, timeout);
    }

    private void AddAllHeaders(StreamMessage streamMessage, Metadata header)
    {
        if (header == null)
        {
            return;
        }

        foreach (var e in header)
        {
            if (e.IsBinary)
            {
                streamMessage.Meta.Add(e.Key, e.ValueBytes.ToHex());
                continue;
            }

            streamMessage.Meta.Add(e.Key, e.Value);
        }
    }
}

public interface IStreamClientProvider
{
    StreamClient GetStreamClient(IAsyncStreamWriter<StreamMessage> clientStreamWriter);
}

public class StreamClientProvider : IStreamClientProvider, ISingletonDependency
{
    private readonly IStreamTaskResourcePool _streamTaskResourcePool;

    public StreamClientProvider(IStreamTaskResourcePool streamTaskResourcePool)
    {
        _streamTaskResourcePool = streamTaskResourcePool;
    }

    public StreamClient GetStreamClient(IAsyncStreamWriter<StreamMessage> clientStreamWriter)
    {
        return new StreamClient(clientStreamWriter, _streamTaskResourcePool);
    }
}