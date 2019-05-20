using System.Threading.Tasks;
using AElf.Kernel.Node.Application;
using AElf.OS.Network;
using AElf.OS.Network.Events;
using AElf.OS.Network.Grpc;
using AElf.OS.Network.Infrastructure;
using Grpc.Core;
using Shouldly;
using Xunit;

namespace AElf.OS.Handlers
{
    public class SyncStateAnnouncementEventHandlerTests : SyncTestBase 
    {
        private SyncStateAnnouncementEventHandler _handler;
        
        private IPeerPool _peerPool;
        private IBlockchainNodeContextService _blockchainNodeContextService;

        public SyncStateAnnouncementEventHandlerTests()
        {
            _handler = GetRequiredService<SyncStateAnnouncementEventHandler>();
            
            _peerPool = GetRequiredService<IPeerPool>();
            _blockchainNodeContextService = GetRequiredService<IBlockchainNodeContextService>();
        }

        [Fact]
        public async Task NullAnnounceShouldNotChangeState()
        {
            await _handler.HandleEventAsync(new AnnouncementReceivedEventData(null, null));
            
            var isSyncing = _blockchainNodeContextService.IsNodeSyncing();
            isSyncing.ShouldBeFalse();
        }

        [Fact]
        public async Task OnePeer_InGap_ShouldTriggerSync()
        {
            var announce = new PeerNewBlockAnnouncement { BlockHash = Hash.FromString("block1"), BlockHeight = 15 };

            var bp1 = CreatePeer("bp1");
            bp1.HandlerRemoteAnnounce(announce);
            
            _peerPool.AddPeer(bp1);
            
            // announce from b1 for block1
            var blockAnnounce = new AnnouncementReceivedEventData(announce, "b1");
            await _handler.HandleEventAsync(blockAnnounce);
            
            _blockchainNodeContextService.IsNodeSyncing().ShouldBeTrue();
        }
        
        [Fact]
        public async Task EnoughAnnouncements_InGap_ShouldTriggerSync()
        {
            var announce = new PeerNewBlockAnnouncement { BlockHash = Hash.FromString("block1"), BlockHeight = 15 };

            var bp1 = CreatePeer("bp1");
            var bp2 = CreatePeer("bp2");
            var bp3 = CreatePeer("bp3");
            
            _peerPool.AddPeer(bp1);
            _peerPool.AddPeer(bp2);
            _peerPool.AddPeer(bp3);
            
            // b1 announces the block1
            bp1.HandlerRemoteAnnounce(announce);
            var blockAnnounceBp1 = new AnnouncementReceivedEventData(announce, "b1");
            await _handler.HandleEventAsync(blockAnnounceBp1);
            
            // one is not enough
            _blockchainNodeContextService.IsNodeSyncing().ShouldBeFalse();
            
            // b2 announces the block1
            bp2.HandlerRemoteAnnounce(announce);
            var blockAnnounceBp2 = new AnnouncementReceivedEventData(announce, "b2");
            await _handler.HandleEventAsync(blockAnnounceBp2);
            
            _blockchainNodeContextService.IsNodeSyncing().ShouldBeTrue();
        }
        
        [Fact]
        public async Task EnoughAnnouncements_OutSideGap_ShouldFinishSync()
        {
            _blockchainNodeContextService.SetSyncing(true);
            
            var announce = new PeerNewBlockAnnouncement { BlockHash = Hash.FromString("block1"), BlockHeight = 11 };

            var bp1 = CreatePeer("bp1");
            var bp2 = CreatePeer("bp2");
            var bp3 = CreatePeer("bp3");
            
            _peerPool.AddPeer(bp1);
            _peerPool.AddPeer(bp2);
            _peerPool.AddPeer(bp3);
            
            // b1, b2 announces the block1
            bp1.HandlerRemoteAnnounce(announce);
            bp2.HandlerRemoteAnnounce(announce);

            await _handler.HandleEventAsync(new AnnouncementReceivedEventData(announce, "b1"));
            
            _blockchainNodeContextService.IsNodeSyncing().ShouldBeFalse();
        }

        private GrpcPeer CreatePeer(string publicKey)
        {
            return new GrpcPeer(new Channel("127.0.0.1:5000", ChannelCredentials.Insecure), null, publicKey, null, 0, 0, 0);
        }
    }
}