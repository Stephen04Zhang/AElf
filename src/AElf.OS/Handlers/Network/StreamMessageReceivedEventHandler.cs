using System;
using System.Threading.Tasks;
using AElf.OS.Network.Events;
using AElf.OS.Network.Grpc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Volo.Abp.DependencyInjection;
using Volo.Abp.EventBus;

namespace AElf.OS.Handlers;

public class StreamMessageReceivedEventHandler : ILocalEventHandler<StreamMessageReceivedEvent>, ITransientDependency
{
    private readonly IStreamService _streamService;
    public ILogger<StreamMessageReceivedEventHandler> Logger { get; set; }

    public StreamMessageReceivedEventHandler(IStreamService streamService)
    {
        _streamService = streamService;
        Logger = NullLogger<StreamMessageReceivedEventHandler>.Instance;
    }

    public Task HandleEventAsync(StreamMessageReceivedEvent eventData)
    {
        try
        {
            Logger.LogDebug("handle receive message");
            //because our message do not have relation between each other, so we want it to be processed concurrency
            var reply = StreamMessage.Parser.ParseFrom(eventData.Message);
            Logger.LogDebug("handle {requestid}", reply.RequestId);
            _streamService.ProcessStreamReplyAsync(reply, eventData.ClientPubkey);
            return Task.CompletedTask;
        }
        catch (Exception e)
        {
            Logger.LogDebug(e, "handler expcetion");
        }
    }
}