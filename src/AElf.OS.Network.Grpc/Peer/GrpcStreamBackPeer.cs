using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using AElf.CSharp.Core.Extension;
using AElf.Kernel;
using AElf.OS.Network.Application;
using AElf.OS.Network.Protocol.Types;
using AElf.Types;
using Grpc.Core;

namespace AElf.OS.Network.Grpc;

public class GrpcStreamBackPeer : GrpcPeerBase
{
    private readonly StreamClient _streamClient;
    private readonly Dictionary<string, string> _peerMeta;

    public GrpcStreamBackPeer(DnsEndPoint remoteEndpoint, PeerConnectionInfo peerConnectionInfo, StreamClient streamClient, Dictionary<string, string> peerMeta) : base(null, remoteEndpoint, peerConnectionInfo)
    {
        _streamClient = streamClient;
        _peerMeta = peerMeta;
    }

    public override async Task DisconnectAsync(bool gracefulDisconnect)
    {
        if (!IsConnected) return;
        IsConnected = false;
        // send disconnect message if the peer is still connected and the connection
        // is stable.
        if (!gracefulDisconnect) return;
        try
        {
            var request = new GrpcRequest { ErrorMessage = "Could not send disconnect." };
            await RequestAsync(() => _streamClient.DisconnectAsync(new DisconnectReason
                { Why = DisconnectReason.Types.Reason.Shutdown }, AddPeerMeta(new Metadata { { GrpcConstants.SessionIdMetadataKey, OutboundSessionId } })), request);
        }
        catch (Exception)
        {
            // swallow the exception, we don't care because we're disconnecting.
        }
    }

    public override async Task ConfirmHandshakeAsync()
    {
        var request = new GrpcRequest { ErrorMessage = "Could not send confirm handshake." };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, UpdateHandshakeTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        await RequestAsync(() => _streamClient.ConfirmHandshakeAsync(new ConfirmHandshakeRequest(), AddPeerMeta(data)), request);
    }

    public override async Task BroadcastBlockAsync(BlockWithTransactions blockWithTransactions)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast block failed." };
        await RequestAsync(() => _streamClient.BroadcastBlockAsync(blockWithTransactions, AddPeerMeta()), request);
    }

    public override async Task SendAnnouncementAsync(BlockAnnouncement header)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast block announcement failed." };
        await RequestAsync(() => _streamClient.BroadcastAnnouncementBlockAsync(header, AddPeerMeta()), request);
    }

    public override async Task SendTransactionAsync(Transaction transaction)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast transaction failed." };
        await RequestAsync(() => _streamClient.BroadcastTransactionAsync(transaction, AddPeerMeta()), request);
    }

    public override async Task SendLibAnnouncementAsync(LibAnnouncement libAnnouncement)
    {
        var request = new GrpcRequest { ErrorMessage = "broadcast lib announcement failed." };
        await RequestAsync(() => _streamClient.BroadcastLibAnnouncementAsync(libAnnouncement, AddPeerMeta()), request);
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
        var list = await RequestAsync(() => _streamClient.RequestBlocksAsync(blockRequest, AddPeerMeta(data)), request);
        return list == null ? new List<BlockWithTransactions>() : list.Blocks.ToList();
    }

    public override async Task<NodeList> GetNodesAsync(int count = NetworkConstants.DefaultDiscoveryMaxNodesToRequest)
    {
        var request = new GrpcRequest { ErrorMessage = "Request nodes failed." };
        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, GetNodesTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        return await RequestAsync(() => _streamClient.GetNodesAsync(new NodesRequest { MaxCount = count }, AddPeerMeta(data)), request);
    }

    public override async Task CheckHealthAsync()
    {
        var request = new GrpcRequest { ErrorMessage = "Check health failed." };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, CheckHealthTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        await RequestAsync(() => _streamClient.CheckHealthAsync(AddPeerMeta(data)), request);
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
        return await RequestAsync(() => _streamClient.RequestBlockAsync(blockRequest, AddPeerMeta(data)), request);
    }

    private async Task<TResp> RequestAsync<TResp>(Func<Task<TResp>> func, GrpcRequest request)
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

    private Metadata AddPeerMeta(Metadata metadata = null)
    {
        metadata ??= new Metadata();
        foreach (var kv in _peerMeta)
        {
            metadata.Add(kv.Key, kv.Value);
        }

        metadata.Add(GrpcConstants.SessionIdMetadataKey, Info.SessionId);
        return metadata;
    }

    public override Task<bool> TryRecoverAsync()
    {
        return Task.FromResult(true);
    }


    public override NetworkException HandleRpcException(RpcException exception, string errorMessage)
    {
        var message = $"Failed request to {this}: {errorMessage}";
        var type = NetworkExceptionType.Rpc;
        if (exception.StatusCode ==
            // there was an exception, not related to connectivity.
            StatusCode.Cancelled)
        {
            message = $"Request was cancelled {this}: {errorMessage}";
            type = NetworkExceptionType.Unrecoverable;
        }
        else if (exception.StatusCode == StatusCode.Unknown)
        {
            message = $"Exception in handler {this}: {errorMessage}";
            type = NetworkExceptionType.HandlerException;
        }

        return new NetworkException(message, exception, type);
    }
}