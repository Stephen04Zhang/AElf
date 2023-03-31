using System.Net;
using System.Threading.Tasks;
using Grpc.Core;

namespace AElf.OS.Network.Grpc;

public interface IPeerDialer
{
    Task<GrpcPeerBase> DialPeerAsync(DnsEndPoint remoteEndpoint);
    Task<GrpcPeerBase> DialBackPeerAsync(DnsEndPoint remoteEndpoint, Handshake handshake);

    Task<GrpcPeerBase> DialBackPeerByStreamAsync(DnsEndPoint remoteEndPoint ,IAsyncStreamWriter<StreamMessage> responseStream, Handshake handshake);

    Task<bool> CheckEndpointAvailableAsync(DnsEndPoint remoteEndpoint);
}