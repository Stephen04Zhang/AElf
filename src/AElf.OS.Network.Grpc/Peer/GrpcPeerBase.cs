using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Net;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using AElf.CSharp.Core.Extension;
using AElf.Kernel;
using AElf.OS.Network.Application;
using AElf.OS.Network.Infrastructure;
using AElf.OS.Network.Metrics;
using AElf.OS.Network.Protocol.Types;
using AElf.Types;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AElf.OS.Network.Grpc;

/// <summary>
///     Represents a connection to a peer.
/// </summary>
public abstract class GrpcPeerBase : IPeer
{
    protected const int MaxMetricsPerMethod = 100;
    protected const int BlockRequestTimeout = 700;
    protected const int CheckHealthTimeout = 1000;
    protected const int BlocksRequestTimeout = 5000;
    protected const int PingRequestTimeout = 1500;
    protected const int GetNodesTimeout = 500;
    protected const int UpdateHandshakeTimeout = 3000;
    protected const int StreamRecoveryWaitTime = 500;

    protected const int BlockCacheMaxItems = 1024;
    protected const int TransactionCacheMaxItems = 10_000;

    protected const int QueuedTransactionTimeout = 10_000;
    protected const int QueuedBlockTimeout = 100_000;

    protected readonly Channel _channel;
    protected readonly PeerService.PeerServiceClient _client;
    private readonly BoundedExpirationCache _knownBlockCache;

    private readonly BoundedExpirationCache _knownTransactionCache;

    protected readonly ConcurrentDictionary<string, ConcurrentQueue<RequestMetric>> _recentRequestsRoundtripTimes;

    protected readonly ActionBlock<StreamJob> _sendAnnouncementJobs;
    protected readonly ActionBlock<StreamJob> _sendBlockJobs;
    protected readonly ActionBlock<StreamJob> _sendTransactionJobs;
    public ILogger<GrpcPeerBase> Logger { get; set; }

    public GrpcPeerBase(GrpcClient client, DnsEndPoint remoteEndpoint, PeerConnectionInfo peerConnectionInfo)
    {
        _channel = client?.Channel;
        _client = client?.Client;

        RemoteEndpoint = remoteEndpoint;
        Info = peerConnectionInfo;

        _knownTransactionCache = new BoundedExpirationCache(TransactionCacheMaxItems, QueuedTransactionTimeout);
        _knownBlockCache = new BoundedExpirationCache(BlockCacheMaxItems, QueuedBlockTimeout);

        _recentRequestsRoundtripTimes = new ConcurrentDictionary<string, ConcurrentQueue<RequestMetric>>();
        RecentRequestsRoundtripTimes =
            new ReadOnlyDictionary<string, ConcurrentQueue<RequestMetric>>(_recentRequestsRoundtripTimes);

        _recentRequestsRoundtripTimes.TryAdd(nameof(MetricNames.GetBlock), new ConcurrentQueue<RequestMetric>());
        _recentRequestsRoundtripTimes.TryAdd(nameof(MetricNames.GetBlocks), new ConcurrentQueue<RequestMetric>());

        _sendAnnouncementJobs = new ActionBlock<StreamJob>(SendStreamJobAsync,
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = NetworkConstants.DefaultMaxBufferedAnnouncementCount
            });
        _sendBlockJobs = new ActionBlock<StreamJob>(SendStreamJobAsync,
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = NetworkConstants.DefaultMaxBufferedBlockCount
            });
        _sendTransactionJobs = new ActionBlock<StreamJob>(SendStreamJobAsync,
            new ExecutionDataflowBlockOptions
            {
                BoundedCapacity = NetworkConstants.DefaultMaxBufferedTransactionCount
            });
        Logger = NullLogger<GrpcPeerBase>.Instance;
    }

    public Timestamp LastSentHandshakeTime { get; private set; }

    public bool IsConnected { get; set; }

    public bool IsShutdown { get; set; }
    public Hash CurrentBlockHash { get; private set; }
    public long CurrentBlockHeight { get; private set; }

    /// <summary>
    ///     Session ID to use when sending messages to this peer, announced at connection
    ///     from the other peer.
    /// </summary>
    public byte[] OutboundSessionId => Info.SessionId;

    public IReadOnlyDictionary<string, ConcurrentQueue<RequestMetric>> RecentRequestsRoundtripTimes { get; }

    /// <summary>
    ///     Property that describes that describes if the peer is ready for send/request operations. It's based
    ///     on the state of the underlying channel and the IsConnected.
    /// </summary>
    public bool IsReady => _channel != null ? (_channel.State == ChannelState.Idle || _channel.State == ChannelState.Ready) && IsConnected : IsConnected;

    public string ConnectionStatus => _channel != null ? _channel.State.ToString() : "unknown";

    public bool IsInvalid =>
        !IsConnected &&
        Info.ConnectionTime.AddMilliseconds(NetworkConstants.PeerConnectionTimeout) <
        TimestampHelper.GetUtcNow();


    public Hash LastKnownLibHash { get; private set; }
    public long LastKnownLibHeight { get; private set; }
    public Timestamp LastReceivedHandshakeTime { get; private set; }
    public SyncState SyncState { get; set; }

    /// <summary>
    ///     Session ID to use when authenticating messages from this peer, announced to the
    ///     remote peer at connection.
    /// </summary>
    public byte[] InboundSessionId { get; set; }

    public DnsEndPoint RemoteEndpoint { get; }
    public int BufferedTransactionsCount => _sendTransactionJobs.InputCount;
    public int BufferedBlocksCount => _sendBlockJobs.InputCount;
    public int BufferedAnnouncementsCount => _sendAnnouncementJobs.InputCount;

    public PeerConnectionInfo Info { get; }

    public Dictionary<string, List<RequestMetric>> GetRequestMetrics()
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

    public abstract Task<NodeList> GetNodesAsync(int count = NetworkConstants.DefaultDiscoveryMaxNodesToRequest);

    public void UpdateLastKnownLib(LibAnnouncement libAnnouncement)
    {
        if (libAnnouncement.LibHeight <= LastKnownLibHeight) return;

        LastKnownLibHash = libAnnouncement.LibHash;
        LastKnownLibHeight = libAnnouncement.LibHeight;
    }

    public abstract Task CheckHealthAsync();

    public abstract Task<BlockWithTransactions> GetBlockByHashAsync(Hash hash);

    public abstract Task<List<BlockWithTransactions>> GetBlocksAsync(Hash firstHash, int count);

    public abstract Task<bool> TryRecoverAsync();

    public bool KnowsBlock(Hash hash)
    {
        return _knownBlockCache.HasHash(hash, false);
    }

    public bool TryAddKnownBlock(Hash blockHash)
    {
        return _knownBlockCache.TryAdd(blockHash);
    }

    public bool KnowsTransaction(Hash hash)
    {
        return _knownTransactionCache.HasHash(hash, false);
    }

    public bool TryAddKnownTransaction(Hash transactionHash)
    {
        return _knownTransactionCache.TryAdd(transactionHash);
    }

    public abstract Task DisconnectAsync(bool gracefulDisconnect);

    public abstract Task PingAsync();

    public void UpdateLastReceivedHandshake(Handshake handshake)
    {
        LastKnownLibHeight = handshake.HandshakeData.LastIrreversibleBlockHeight;
        CurrentBlockHash = handshake.HandshakeData.BestChainHash;
        CurrentBlockHeight = handshake.HandshakeData.BestChainHeight;
        LastReceivedHandshakeTime = handshake.HandshakeData.Time;
    }

    public void UpdateLastSentHandshake(Handshake handshake)
    {
        LastSentHandshakeTime = handshake.HandshakeData.Time;
    }

    public abstract Task ConfirmHandshakeAsync();

    public override string ToString()
    {
        return $"{{ listening-port: {RemoteEndpoint}, {Info} }}";
    }


    #region Streaming

    public void EnqueueTransaction(Transaction transaction, Action<NetworkException> sendCallback)
    {
        if (!IsReady)
            throw new NetworkException($"Dropping transaction, peer is not ready - {this}.",
                NetworkExceptionType.NotConnected);

        _sendTransactionJobs.Post(new StreamJob { Transaction = transaction, SendCallback = sendCallback });
    }

    public void EnqueueAnnouncement(BlockAnnouncement announcement, Action<NetworkException> sendCallback)
    {
        if (!IsReady)
            throw new NetworkException($"Dropping announcement, peer is not ready - {this}.",
                NetworkExceptionType.NotConnected);

        _sendAnnouncementJobs.Post(new StreamJob { BlockAnnouncement = announcement, SendCallback = sendCallback });
    }

    public void EnqueueBlock(BlockWithTransactions blockWithTransactions, Action<NetworkException> sendCallback)
    {
        if (!IsReady)
            throw new NetworkException($"Dropping block, peer is not ready - {this}.",
                NetworkExceptionType.NotConnected);

        _sendBlockJobs.Post(
            new StreamJob { BlockWithTransactions = blockWithTransactions, SendCallback = sendCallback });
    }

    public void EnqueueLibAnnouncement(LibAnnouncement libAnnouncement, Action<NetworkException> sendCallback)
    {
        if (!IsReady)
            throw new NetworkException($"Dropping lib announcement, peer is not ready - {this}.",
                NetworkExceptionType.NotConnected);

        _sendAnnouncementJobs.Post(new StreamJob
        {
            LibAnnouncement = libAnnouncement,
            SendCallback = sendCallback
        });
    }

    public abstract NetworkException HandleRpcException(RpcException ex, string errorMessage);

    private async Task SendStreamJobAsync(StreamJob job)
    {
        if (!IsReady)
            return;

        try
        {
            if (job.Transaction != null)
                await SendTransactionAsync(job.Transaction);
            else if (job.BlockAnnouncement != null)
                await SendAnnouncementAsync(job.BlockAnnouncement);
            else if (job.BlockWithTransactions != null)
                await BroadcastBlockAsync(job.BlockWithTransactions);
            else if (job.LibAnnouncement != null) await SendLibAnnouncementAsync(job.LibAnnouncement);
        }
        catch (RpcException ex)
        {
            job.SendCallback?.Invoke(HandleRpcException(ex, $"Could not broadcast to {this}: "));
            await Task.Delay(StreamRecoveryWaitTime);
            return;
        }
        catch (Exception ex)
        {
            job.SendCallback?.Invoke(new NetworkException("Unknown exception during broadcast.", ex));
            throw;
        }

        job.SendCallback?.Invoke(null);
    }

    public abstract Task BroadcastBlockAsync(BlockWithTransactions blockWithTransactions);

    /// <summary>
    ///     Send a announcement to the peer using the stream call.
    ///     Note: this method is not thread safe.
    /// </summary>
    public abstract Task SendAnnouncementAsync(BlockAnnouncement header);

    /// <summary>
    ///     Send a transaction to the peer using the stream call.
    ///     Note: this method is not thread safe.
    /// </summary>
    public abstract Task SendTransactionAsync(Transaction transaction);

    /// <summary>
    ///     Send a lib announcement to the peer using the stream call.
    ///     Note: this method is not thread safe.
    /// </summary>
    public abstract Task SendLibAnnouncementAsync(LibAnnouncement libAnnouncement);

    #endregion
}

public enum MetricNames
{
    GetBlocks,
    GetBlock
}