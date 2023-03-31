using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using AElf.Kernel;
using AElf.OS.Network.Application;
using AElf.OS.Network.Metrics;
using AElf.OS.Network.Protocol.Types;
using AElf.Types;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace AElf.OS.Network.Grpc;

/// <summary>
///     Represents a connection to a peer.
/// </summary>
public class GrpcPeer : GrpcPeerBase
{
    protected AsyncClientStreamingCall<BlockAnnouncement, VoidReply> _announcementStreamCall;
    protected AsyncClientStreamingCall<BlockWithTransactions, VoidReply> _blockStreamCall;
    protected AsyncClientStreamingCall<LibAnnouncement, VoidReply> _libAnnouncementStreamCall;

    protected AsyncClientStreamingCall<Transaction, VoidReply> _transactionStreamCall;

    public GrpcPeer(GrpcClient client, DnsEndPoint remoteEndpoint, PeerConnectionInfo peerConnectionInfo) : base(client, remoteEndpoint, peerConnectionInfo)
    {
    }

    public override Dictionary<string, List<RequestMetric>> GetRequestMetrics()
    {
        var metrics = new Dictionary<string, List<RequestMetric>>();
        foreach (var roundtripTime in _recentRequestsRoundtripTimes.ToArray())
        {
            var metricsToAdd = new List<RequestMetric>();

            metrics.Add(roundtripTime.Key, metricsToAdd);
            foreach (var requestMetric in roundtripTime.Value) metricsToAdd.Add(requestMetric);
        }

        return metrics;
    }

    public override Task<NodeList> GetNodesAsync(int count = NetworkConstants.DefaultDiscoveryMaxNodesToRequest)
    {
        var request = new GrpcRequest { ErrorMessage = "Request nodes failed." };
        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, GetNodesTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };

        return RequestAsync(() => _client.GetNodesAsync(new NodesRequest { MaxCount = count }, data), request);
    }


    public override async Task CheckHealthAsync()
    {
        var request = new GrpcRequest { ErrorMessage = "Check health failed." };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, CheckHealthTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };

        await RequestAsync(() => _client.CheckHealthAsync(new HealthCheckRequest(), data), request);
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

        var blockReply = await RequestAsync(() => _client.RequestBlockAsync(blockRequest, data), request);

        return blockReply?.Block;
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

        var list = await RequestAsync(() => _client.RequestBlocksAsync(blockRequest, data), request);

        if (list == null)
            return new List<BlockWithTransactions>();

        return list.Blocks.ToList();
    }

    public override async Task<bool> TryRecoverAsync()
    {
        if (_channel.State == ChannelState.Shutdown)
            return false;

        await _channel.TryWaitForStateChangedAsync(_channel.State,
            DateTime.UtcNow.AddSeconds(NetworkConstants.DefaultPeerRecoveryTimeout));

        // Either we connected again or the state change wait timed out.
        if (_channel.State == ChannelState.TransientFailure || _channel.State == ChannelState.Connecting)
        {
            IsConnected = false;
            return false;
        }

        return true;
    }

    public override async Task DisconnectAsync(bool gracefulDisconnect)
    {
        IsConnected = false;
        IsShutdown = true;

        // we complete but no need to await the jobs
        _sendAnnouncementJobs.Complete();
        _sendBlockJobs.Complete();
        _sendTransactionJobs.Complete();

        _announcementStreamCall?.Dispose();
        _transactionStreamCall?.Dispose();
        _blockStreamCall?.Dispose();

        // send disconnect message if the peer is still connected and the connection
        // is stable.
        if (gracefulDisconnect && (_channel.State == ChannelState.Idle || _channel.State == ChannelState.Ready))
        {
            var request = new GrpcRequest { ErrorMessage = "Could not send disconnect." };

            try
            {
                var metadata = new Metadata { { GrpcConstants.SessionIdMetadataKey, OutboundSessionId } };

                await RequestAsync(
                    () => _client.DisconnectAsync(new DisconnectReason
                        { Why = DisconnectReason.Types.Reason.Shutdown }, metadata), request);
            }
            catch (NetworkException)
            {
                // swallow the exception, we don't care because we're disconnecting.
            }
        }

        try
        {
            await _channel.ShutdownAsync();
        }
        catch (InvalidOperationException)
        {
            // if channel already shutdown
        }
    }

    public override async Task PingAsync()
    {
        var request = new GrpcRequest
        {
            ErrorMessage = $"ping failed.",
            MetricInfo = $"ping"
        };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, PingRequestTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };
        await RequestAsync(() => _client.PingAsync(new PingRequest(), data), request);
    }

    public override async Task ConfirmHandshakeAsync()
    {
        var request = new GrpcRequest { ErrorMessage = "Could not send confirm handshake." };

        var data = new Metadata
        {
            { GrpcConstants.TimeoutMetadataKey, UpdateHandshakeTimeout.ToString() },
            { GrpcConstants.SessionIdMetadataKey, OutboundSessionId }
        };

        await RequestAsync(() => _client.ConfirmHandshakeAsync(new ConfirmHandshakeRequest(), data), request);
    }

    protected async Task<TResp> RequestAsync<TResp>(Func<AsyncUnaryCall<TResp>> func, GrpcRequest requestParams)
    {
        var metricsName = requestParams.MetricName;
        var timeRequest = !string.IsNullOrEmpty(metricsName);
        var requestStartTime = TimestampHelper.GetUtcNow();

        Stopwatch requestTimer = null;

        if (timeRequest)
            requestTimer = Stopwatch.StartNew();

        try
        {
            var response = await func();
            if (timeRequest)
            {
                requestTimer.Stop();
                RecordMetric(requestParams, requestStartTime, requestTimer.ElapsedMilliseconds);
            }

            return response;
        }
        catch (ObjectDisposedException ex)
        {
            throw new NetworkException("Peer is closed", ex, NetworkExceptionType.Unrecoverable);
        }
        catch (AggregateException ex)
        {
            if (!(ex.InnerException is RpcException rpcException))
                throw new NetworkException($"Unknown exception. {this}: {requestParams.ErrorMessage}",
                    NetworkExceptionType.Unrecoverable);

            throw HandleRpcException(rpcException, requestParams.ErrorMessage);
        }
    }

    private void RecordMetric(GrpcRequest grpcRequest, Timestamp requestStartTime, long elapsedMilliseconds)
    {
        var metrics = _recentRequestsRoundtripTimes[grpcRequest.MetricName];

        while (metrics.Count >= MaxMetricsPerMethod)
            metrics.TryDequeue(out _);

        metrics.Enqueue(new RequestMetric
        {
            Info = grpcRequest.MetricInfo,
            RequestTime = requestStartTime,
            MethodName = grpcRequest.MetricName,
            RoundTripTime = elapsedMilliseconds
        });
    }


    /// <summary>
    ///     This method handles the case where the peer is potentially down. If the Rpc call
    ///     put the channel in TransientFailure or Connecting, we give the connection a certain time to recover.
    /// </summary>
    public override NetworkException HandleRpcException(RpcException exception, string errorMessage)
    {
        var message = $"Failed request to {this}: {errorMessage}";
        var type = NetworkExceptionType.Rpc;

        if (_channel.State != ChannelState.Ready)
        {
            // if channel has been shutdown (unrecoverable state) remove it.
            if (_channel.State == ChannelState.Shutdown)
            {
                message = $"Peer is shutdown - {this}: {errorMessage}";
                type = NetworkExceptionType.Unrecoverable;
            }
            else if (_channel.State == ChannelState.TransientFailure || _channel.State == ChannelState.Connecting)
            {
                // from this we try to recover
                message = $"Peer is unstable - {this}: {errorMessage}";
                type = NetworkExceptionType.PeerUnstable;
            }
            else
            {
                // if idle just after an exception, disconnect.
                message = $"Peer idle, channel state {_channel.State} - {this}: {errorMessage}";
                type = NetworkExceptionType.Unrecoverable;
            }
        }
        else
        {
            // there was an exception, not related to connectivity.
            if (exception.StatusCode == StatusCode.Cancelled)
            {
                message = $"Request was cancelled {this}: {errorMessage}";
                type = NetworkExceptionType.Unrecoverable;
            }
            else if (exception.StatusCode == StatusCode.Unknown)
            {
                message = $"Exception in handler {this}: {errorMessage}";
                type = NetworkExceptionType.HandlerException;
            }
        }

        return new NetworkException(message, exception, type);
    }

    #region Streaming

    public override async Task BroadcastBlockAsync(BlockWithTransactions blockWithTransactions)
    {
        if (_blockStreamCall == null)
            _blockStreamCall = _client.BlockBroadcastStream(new Metadata
                { { GrpcConstants.SessionIdMetadataKey, OutboundSessionId } });

        try
        {
            await _blockStreamCall.RequestStream.WriteAsync(blockWithTransactions);
        }
        catch (RpcException)
        {
            _blockStreamCall.Dispose();
            _blockStreamCall = null;

            throw;
        }
    }

    /// <summary>
    ///     Send a announcement to the peer using the stream call.
    ///     Note: this method is not thread safe.
    /// </summary>
    public override async Task SendAnnouncementAsync(BlockAnnouncement header)
    {
        if (_announcementStreamCall == null)
            _announcementStreamCall = _client.AnnouncementBroadcastStream(new Metadata
                { { GrpcConstants.SessionIdMetadataKey, OutboundSessionId } });

        try
        {
            await _announcementStreamCall.RequestStream.WriteAsync(header);
        }
        catch (RpcException)
        {
            _announcementStreamCall.Dispose();
            _announcementStreamCall = null;

            throw;
        }
    }

    /// <summary>
    ///     Send a transaction to the peer using the stream call.
    ///     Note: this method is not thread safe.
    /// </summary>
    public override async Task SendTransactionAsync(Transaction transaction)
    {
        if (_transactionStreamCall == null)
            _transactionStreamCall = _client.TransactionBroadcastStream(new Metadata
                { { GrpcConstants.SessionIdMetadataKey, OutboundSessionId } });

        try
        {
            await _transactionStreamCall.RequestStream.WriteAsync(transaction);
        }
        catch (RpcException)
        {
            _transactionStreamCall.Dispose();
            _transactionStreamCall = null;

            throw;
        }
    }

    /// <summary>
    ///     Send a lib announcement to the peer using the stream call.
    ///     Note: this method is not thread safe.
    /// </summary>
    public override async Task SendLibAnnouncementAsync(LibAnnouncement libAnnouncement)
    {
        if (_libAnnouncementStreamCall == null)
            _libAnnouncementStreamCall = _client.LibAnnouncementBroadcastStream(new Metadata
                { { GrpcConstants.SessionIdMetadataKey, OutboundSessionId } });

        try
        {
            await _libAnnouncementStreamCall.RequestStream.WriteAsync(libAnnouncement);
        }
        catch (RpcException)
        {
            _libAnnouncementStreamCall.Dispose();
            _libAnnouncementStreamCall = null;

            throw;
        }
    }

    #endregion
}