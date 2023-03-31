using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using AElf.OS.Network.Application;
using AElf.OS.Network.Metrics;
using AElf.OS.Network.Protocol.Types;
using AElf.Types;
using Grpc.Core;

namespace AElf.OS.Network.Grpc;

public class GrpcStreamBackPeer : GrpcPeerBase
{
    private readonly StreamClient _streamClient;
    private readonly Dictionary<string, string> _peerMeta;

    public GrpcStreamBackPeer(GrpcClient client, DnsEndPoint remoteEndpoint, PeerConnectionInfo peerConnectionInfo, StreamClient streamClient, Dictionary<string, string> peerMeta) : base(client, remoteEndpoint, peerConnectionInfo)
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
            await _streamClient.DisconnectAsync(new DisconnectReason
                { Why = DisconnectReason.Types.Reason.Shutdown }, AddPeerMeta(new Metadata { { GrpcConstants.SessionIdMetadataKey, Info.SessionId } }));
        }
        catch (Exception)
        {
        }
    }

    public override async Task PingAsync()
    {
        if (_streamClient == null) return;
        try
        {
            await _streamClient.PingAsync(AddPeerMeta(null));
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, "BroadcastBlockAsync failed");
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
            throw;
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
            {
                await DisconnectAsync(true);
            }

            throw;
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
        try
        {
            await _streamClient.ConfirmHandshakeAsync(new ConfirmHandshakeRequest(), AddPeerMeta(data));
        }
        catch (InvalidOperationException)
        {
            await DisconnectAsync(true);
            throw;
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, request.ErrorMessage);
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
            throw;
        }
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

    public override async Task BroadcastBlockAsync(BlockWithTransactions blockWithTransactions)
    {
        try
        {
            await _streamClient.BroadcastBlockAsync(blockWithTransactions, AddPeerMeta(null));
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, "BroadcastBlockAsync failed");
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
            throw;
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
            {
                await DisconnectAsync(true);
            }

            throw;
        }
    }

    public override async Task SendAnnouncementAsync(BlockAnnouncement header)
    {
        try
        {
            await _streamClient.BroadcastAnnouncementBlockAsync(header, AddPeerMeta(null));
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, "BroadcastBlockAsync failed");
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
            throw;
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
            {
                await DisconnectAsync(true);
            }

            throw;
        }
    }

    public override async Task SendTransactionAsync(Transaction transaction)
    {
        try
        {
            await _streamClient.BroadcastTransactionAsync(transaction, AddPeerMeta(null));
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, "BroadcastBlockAsync failed");
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
            throw;
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
            {
                await DisconnectAsync(true);
            }

            throw;
        }
    }

    public override async Task SendLibAnnouncementAsync(LibAnnouncement libAnnouncement)
    {
        try
        {
            await _streamClient.BroadcastLibAnnouncementAsync(libAnnouncement, AddPeerMeta(null));
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, "BroadcastBlockAsync failed");
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
            throw;
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
            {
                await DisconnectAsync(true);
            }

            throw;
        }
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
        try
        {
            var list = await _streamClient.RequestBlocksAsync(blockRequest, AddPeerMeta(data));
            return list == null ? new List<BlockWithTransactions>() : list.Blocks.ToList();
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, request.ErrorMessage);
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
            {
                await DisconnectAsync(true);
            }
        }

        return new List<BlockWithTransactions>();
    }

    public override async Task<NodeList> GetNodesAsync(int count = NetworkConstants.DefaultDiscoveryMaxNodesToRequest)
    {
        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, GetNodesTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };

        try
        {
            return await _streamClient.GetNodesAsync(new NodesRequest { MaxCount = count }, AddPeerMeta(data));
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, "Request nodes failed.");
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
            {
                await DisconnectAsync(true);
            }
        }

        return null;
    }

    public override async Task CheckHealthAsync()
    {
        var request = new GrpcRequest { ErrorMessage = "Check health failed." };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, CheckHealthTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        try
        {
            await _streamClient.CheckHealthAsync(AddPeerMeta(data));
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, request.ErrorMessage);
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
            {
                await DisconnectAsync(true);
            }
        }
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
        try
        {
            return await _streamClient.RequestBlockAsync(blockRequest, AddPeerMeta(data));
        }
        catch (RpcException e)
        {
            var networkException = HandleRpcException(e, request.ErrorMessage);
            if (networkException.ExceptionType == NetworkExceptionType.Unrecoverable)
                await DisconnectAsync(true);
        }
        catch (Exception e)
        {
            if (e is TimeoutException or InvalidOperationException)
            {
                await DisconnectAsync(true);
            }
        }

        return new BlockWithTransactions();
    }

    private Metadata AddPeerMeta(Metadata metadata)
    {
        metadata ??= new Metadata();
        foreach (var kv in _peerMeta)
        {
            metadata.Add(kv.Key, kv.Value);
        }

        return metadata;
    }

    public override async Task<bool> TryRecoverAsync()
    {
        IsConnected = false;
        return IsConnected;
    }

    public override Dictionary<string, List<RequestMetric>> GetRequestMetrics()
    {
        return null;
    }
}