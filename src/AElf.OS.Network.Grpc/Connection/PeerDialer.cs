using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using AElf.Kernel;
using AElf.Kernel.Account.Application;
using AElf.Kernel.SmartContract;
using AElf.OS.Network.Events;
using AElf.OS.Network.Grpc.Helpers;
using AElf.OS.Network.Infrastructure;
using AElf.OS.Network.Protocol;
using AElf.OS.Network.Protocol.Types;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Core.Utils;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Org.BouncyCastle.X509;
using Volo.Abp.EventBus.Local;
using Volo.Abp.Threading;

namespace AElf.OS.Network.Grpc;

/// <summary>
///     Provides functionality to setup a connection to a distant node by exchanging some
///     low level information.
/// </summary>
public class PeerDialer : IPeerDialer
{
    private readonly IAccountService _accountService;
    private readonly IHandshakeProvider _handshakeProvider;
    private KeyCertificatePair _clientKeyCertificatePair;
    private IStreamTaskResourcePool _streamTaskResourcePool;
    public ILocalEventBus EventBus { get; set; }

    public PeerDialer(IAccountService accountService,
        IHandshakeProvider handshakeProvider, IStreamTaskResourcePool streamTaskResourcePool)
    {
        _accountService = accountService;
        _handshakeProvider = handshakeProvider;
        _streamTaskResourcePool = streamTaskResourcePool;
        EventBus = NullLocalEventBus.Instance;

        Logger = NullLogger<PeerDialer>.Instance;

        CreateClientKeyCertificatePair();
    }

    private NetworkOptions NetworkOptions => NetworkOptionsSnapshot.Value;
    public IOptionsSnapshot<NetworkOptions> NetworkOptionsSnapshot { get; set; }

    public ILogger<PeerDialer> Logger { get; set; }

    /// <summary>
    ///     Given an IP address, will create a handshake to the distant node for
    ///     further communications.
    /// </summary>
    /// <returns>The created peer</returns>
    public async Task<GrpcPeerBase> DialPeerAsync(DnsEndPoint remoteEndpoint)
    {
        var client = await CreateClientAsync(remoteEndpoint);

        if (client == null)
            return null;

        var handshake = await _handshakeProvider.GetHandshakeAsync(KernelConstants.PreProtocolVersion);
        var handshakeReply = await CallDoHandshakeAsync(client, remoteEndpoint, handshake);

        if (!await ProcessHandshakeReplyAsync(handshakeReply, remoteEndpoint))
        {
            await client.Channel.ShutdownAsync();
            return null;
        }

        var connectionInfo = new PeerConnectionInfo
        {
            Pubkey = handshakeReply.Handshake.HandshakeData.Pubkey.ToHex(),
            ConnectionTime = TimestampHelper.GetUtcNow(),
            ProtocolVersion = handshakeReply.Handshake.HandshakeData.Version,
            SessionId = handshakeReply.Handshake.SessionId.ToByteArray(),
            IsInbound = false,
            NodeVersion = handshakeReply.Handshake.HandshakeData.NodeVersion
        };
        GrpcPeerBase peer;

        if (UpgradeToStream(handshake, handshakeReply.Handshake))
        {
            peer = await DailStreamPeerAsync(client, remoteEndpoint, connectionInfo);
            if (peer == null) return peer;
        }
        else
        {
            peer = new GrpcPeer(client, remoteEndpoint, connectionInfo);
            peer.InboundSessionId = handshake.SessionId.ToByteArray();
        }


        Logger.LogDebug("peer sessionId {InboundSessionId} {sessionId}", peer.InboundSessionId.ToHex(), connectionInfo.SessionId.ToHex());
        peer.UpdateLastReceivedHandshake(handshakeReply.Handshake);
        peer.UpdateLastSentHandshake(handshake);
        return peer;
    }

    private async Task<bool> ProcessHandshakeReplyAsync(HandshakeReply handshakeReply, DnsEndPoint remoteEndpoint)
    {
        // verify handshake
        if (handshakeReply.Error != HandshakeError.HandshakeOk)
        {
            Logger.LogWarning("Handshake error: {remoteEndpoint} {Error}.", remoteEndpoint, handshakeReply.Error);

            return false;
        }

        if (await _handshakeProvider.ValidateHandshakeAsync(handshakeReply.Handshake) ==
            HandshakeValidationResult.Ok) return true;
        Logger.LogWarning("Connect error: {remoteEndpoint} {handshakeReply}.", remoteEndpoint, handshakeReply);
        return false;
    }


    public async Task<GrpcPeerBase> DialBackPeerByStreamAsync(DnsEndPoint remoteEndpoint, IAsyncStreamWriter<StreamMessage> responseStream, Handshake handshake)
    {
        Logger.LogWarning("receive stream ping reply");
        var info = new PeerConnectionInfo
        {
            Pubkey = handshake.HandshakeData.Pubkey.ToHex(),
            ConnectionTime = TimestampHelper.GetUtcNow(),
            SessionId = handshake.SessionId.ToByteArray(),
            ProtocolVersion = handshake.HandshakeData.Version,
            IsInbound = true,
            NodeVersion = handshake.HandshakeData.NodeVersion
        };
        var nodePubkey = (await _accountService.GetPublicKeyAsync()).ToHex();
        var meta = new Dictionary<string, string>()
        {
            { GrpcConstants.PubkeyMetadataKey, nodePubkey },
            { GrpcConstants.PeerInfoMetadataKey, info.ToString() }
        };
        Logger.LogWarning("DialBackPeerByStreamAsync meta={meta}", meta);
        var peer = new GrpcStreamBackPeer(remoteEndpoint, info, responseStream, _streamTaskResourcePool, meta);

        peer.UpdateLastReceivedHandshake(handshake);

        return peer;
    }

    public async Task<bool> CheckEndpointAvailableAsync(DnsEndPoint remoteEndpoint)
    {
        var client = await CreateClientAsync(remoteEndpoint);

        if (client == null)
            return false;
        try
        {
            await PingNodeAsync(client, remoteEndpoint);
            await client.Channel.ShutdownAsync();
            return true;
        }
        catch (Exception e)
        {
            Logger.LogWarning(e, $"Could not ping peer {remoteEndpoint}.");
            return false;
        }
    }

    public async Task<GrpcPeerBase> DialBackPeerAsync(DnsEndPoint remoteEndpoint, Handshake handshake)
    {
        var client = await CreateClientAsync(remoteEndpoint);

        if (client == null)
            return null;

        await PingNodeAsync(client, remoteEndpoint);
        var connectionInfo = new PeerConnectionInfo
        {
            Pubkey = handshake.HandshakeData.Pubkey.ToHex(),
            ConnectionTime = TimestampHelper.GetUtcNow(),
            SessionId = handshake.SessionId.ToByteArray(),
            ProtocolVersion = handshake.HandshakeData.Version,
            IsInbound = true,
            NodeVersion = handshake.HandshakeData.NodeVersion
        };
        var peer = new GrpcPeer(client, remoteEndpoint, connectionInfo);

        peer.UpdateLastReceivedHandshake(handshake);

        return peer;
    }

    private void CreateClientKeyCertificatePair()
    {
        _clientKeyCertificatePair = TlsHelper.GenerateKeyCertificatePair();
    }

    /// <summary>
    ///     Calls the server side DoHandshake RPC method, in order to establish a 2-way connection.
    /// </summary>
    /// <returns>The reply from the server.</returns>
    private async Task<HandshakeReply> CallDoHandshakeAsync(GrpcClient client, DnsEndPoint remoteEndPoint,
        Handshake handshake)
    {
        HandshakeReply handshakeReply;

        try
        {
            var metadata = new Metadata
            {
                { GrpcConstants.RetryCountMetadataKey, "0" },
                { GrpcConstants.TimeoutMetadataKey, (NetworkOptions.PeerDialTimeout * 2).ToString() }
            };
            handshakeReply = await client.Client.DoHandshakeAsync(new HandshakeRequest { Handshake = handshake }, metadata);

            Logger.LogDebug($"Handshake to {remoteEndPoint} successful.");
        }
        catch (Exception)
        {
            await client.Channel.ShutdownAsync();
            throw;
        }

        return handshakeReply;
    }

    private bool UpgradeToStream(Handshake handshake, Handshake handshakeReply)
    {
        return handshake.HandshakeData.ListeningPort == KernelConstants.ClosedPort && handshakeReply.HandshakeData.Version == KernelConstants.ProtocolVersion;
    }

    private async Task<GrpcStreamPeer> DailStreamPeerAsync(GrpcClient client, DnsEndPoint remoteEndpoint, PeerConnectionInfo connectionInfo)
    {
        try
        {
            
            var call = client.Client.RequestByStream(new CallOptions().WithDeadline(DateTime.MaxValue));
            var streamPeer = new GrpcStreamPeer(client, remoteEndpoint, connectionInfo, call, null, _streamTaskResourcePool, new Dictionary<string, string>());
            var tokenSource = new CancellationTokenSource();
            Task.Run(async () =>
            {
                await call.ResponseStream.ForEachAsync(async req => await
                    EventBus.PublishAsync(new StreamMessageReceivedEvent(req.ToByteString(), streamPeer.Info.Pubkey)));
            }, tokenSource.Token);
            streamPeer.StartServe(tokenSource);
            var handshake = await _handshakeProvider.GetHandshakeAsync();
            var handShakeReply = await streamPeer.HandShakeAsync(new HandshakeRequest { Handshake = handshake });
            if (!await ProcessHandshakeReplyAsync(handShakeReply, remoteEndpoint))
            {
                await streamPeer.DisconnectAsync(true);
                return null;
            }

            streamPeer.InboundSessionId = handshake.SessionId.ToByteArray();
            streamPeer.Info.SessionId = handShakeReply.Handshake.SessionId.ToByteArray();
            Logger.LogDebug("streaming Handshake to {remoteEndPoint} successful.sessionInfo {InboundSessionId} {SessionId}", remoteEndpoint.ToString(), streamPeer.InboundSessionId.ToHex(), streamPeer.Info.SessionId.ToHex());
            return streamPeer;
        }
        catch (Exception e)
        {
            Logger.LogError(e, "stream handle shake failed {remoteEndpoint}", remoteEndpoint);
            await client.Channel.ShutdownAsync();
            throw;
        }
    }


    /// <summary>
    ///     Checks that the distant node is reachable by pinging it.
    /// </summary>
    /// <returns>The reply from the server.</returns>
    private async Task PingNodeAsync(GrpcClient client, DnsEndPoint peerEndpoint)
    {
        try
        {
            var metadata = new Metadata
            {
                { GrpcConstants.RetryCountMetadataKey, "0" },
                { GrpcConstants.TimeoutMetadataKey, NetworkOptions.PeerDialTimeout.ToString() }
            };

            await client.Client.PingAsync(new PingRequest(), metadata);

            Logger.LogDebug($"Pinged {peerEndpoint} successfully.");
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, $"Could not ping {peerEndpoint}.");
            await client.Channel.ShutdownAsync();
            throw;
        }
    }

    /// <summary>
    ///     Creates a channel/client pair with the appropriate options and interceptors.
    /// </summary>
    /// <returns>A tuple of the channel and client</returns>
    private async Task<GrpcClient> CreateClientAsync(DnsEndPoint remoteEndpoint)
    {
        var certificate = await RetrieveServerCertificateAsync(remoteEndpoint);

        if (certificate == null)
            return null;

        Logger.LogDebug($"Upgrading connection to TLS: {certificate}.");
        ChannelCredentials credentials =
            new SslCredentials(TlsHelper.ObjectToPem(certificate), _clientKeyCertificatePair);

        var channel = new Channel(remoteEndpoint.ToString(), credentials, new List<ChannelOption>
        {
            new(ChannelOptions.MaxSendMessageLength, GrpcConstants.DefaultMaxSendMessageLength),
            new(ChannelOptions.MaxReceiveMessageLength, GrpcConstants.DefaultMaxReceiveMessageLength),
            new(ChannelOptions.SslTargetNameOverride, GrpcConstants.DefaultTlsCommonName),
            new(GrpcConstants.GrpcArgKeepalivePermitWithoutCalls, GrpcConstants.GrpcArgKeepalivePermitWithoutCallsOpen),
            new(GrpcConstants.GrpcArgHttp2MaxPingsWithoutData, GrpcConstants.GrpcArgHttp2MaxPingsWithoutDataVal),
            new(GrpcConstants.GrpcArgKeepaliveTimeoutMs, GrpcConstants.GrpcArgKeepaliveTimeoutMsVal),
            new(GrpcConstants.GrpcArgKeepaliveTimeMs, GrpcConstants.GrpcArgKeepaliveTimeMsVal)
        });

        var nodePubkey = AsyncHelper.RunSync(() => _accountService.GetPublicKeyAsync()).ToHex();

        var interceptedChannel = channel.Intercept(metadata =>
        {
            metadata.Add(GrpcConstants.PubkeyMetadataKey, nodePubkey);
            return metadata;
        }).Intercept(new RetryInterceptor());

        var client = new PeerService.PeerServiceClient(interceptedChannel);

        return new GrpcClient(channel, client, certificate);
    }

    private async Task<X509Certificate> RetrieveServerCertificateAsync(DnsEndPoint remoteEndpoint)
    {
        Logger.LogDebug($"Starting certificate retrieval for {remoteEndpoint}.");

        TcpClient client = null;

        try
        {
            client = new TcpClient();
            using (var cts = new CancellationTokenSource())
            {
                cts.CancelAfter(NetworkConstants.DefaultSslCertifFetchTimeout);
                await client.ConnectAsync(remoteEndpoint.Host, remoteEndpoint.Port).WithCancellation(cts.Token);

                using (var sslStream = new SslStream(client.GetStream(), true, (a, b, c, d) => true))
                {
                    sslStream.ReadTimeout = NetworkConstants.DefaultSslCertifFetchTimeout;
                    sslStream.WriteTimeout = NetworkConstants.DefaultSslCertifFetchTimeout;
                    await sslStream.AuthenticateAsClientAsync(remoteEndpoint.Host).WithCancellation(cts.Token);

                    if (sslStream.RemoteCertificate == null)
                    {
                        Logger.LogDebug($"Certificate from {remoteEndpoint} is null");
                        return null;
                    }

                    Logger.LogDebug($"Retrieved certificate for {remoteEndpoint}.");

                    return FromX509Certificate(sslStream.RemoteCertificate);
                }
            }
        }
        catch (OperationCanceledException)
        {
            Logger.LogDebug($"Certificate retrieval connection timeout for {remoteEndpoint}.");
            return null;
        }
        catch (Exception ex)
        {
            // swallow exception because it's currently not a hard requirement to 
            // upgrade the connection.
            Logger.LogWarning(ex, $"Could not retrieve certificate from {remoteEndpoint}.");
        }
        finally
        {
            client?.Close();
        }

        return null;
    }

    public static X509Certificate FromX509Certificate(
        System.Security.Cryptography.X509Certificates.X509Certificate x509Cert)
    {
        return new X509CertificateParser().ReadCertificate(x509Cert.GetRawCertData());
    }
}