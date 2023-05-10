using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using AElf.OS.Network.Events;
using AElf.OS.Network.Protocol;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Volo.Abp.DependencyInjection;
using Volo.Abp.EventBus.Local;
using Volo.Abp.Uow;

namespace AElf.OS.Network.Grpc;

public interface IStreamTaskResourcePool
{
    Task RegistryTaskPromiseAsync(string requestId, MessageType messageType, TaskCompletionSource<StreamMessage> promise);
    void TrySetResult(string requestId, StreamMessage reply);
    Task<StreamMessage> GetResultAsync(TaskCompletionSource<StreamMessage> promise, string requestId, int timeOut);
    Task PublishAsync(StreamMessage request, string pubkey);
    Task<Handshake> GetHandshakeAsync();
    Task<HandshakeValidationResult> ValidateHandshakeAsync(Handshake handshake);
}

public class StreamTaskResourcePool : IStreamTaskResourcePool, ISingletonDependency
{
    private readonly IHandshakeProvider _handshakeProvider;
    private readonly ConcurrentDictionary<string, StreamContext> _promisePool;
    public ILocalEventBus EventBus { get; set; }
    public ILogger<StreamTaskResourcePool> Logger { get; set; }

    public StreamTaskResourcePool(IHandshakeProvider handshakeProvider)
    {
        _handshakeProvider = handshakeProvider;
        _promisePool = new ConcurrentDictionary<string, StreamContext>();
        EventBus = NullLocalEventBus.Instance;
        Logger = NullLogger<StreamTaskResourcePool>.Instance;
    }

    public Task RegistryTaskPromiseAsync(string requestId, MessageType messageType, TaskCompletionSource<StreamMessage> promise)
    {
        _promisePool[requestId] = new StreamContext(messageType, promise);
        return Task.CompletedTask;
    }

    public void TrySetResult(string requestId, StreamMessage reply)
    {
        if (!_promisePool.TryGetValue(requestId, out var request))
        {
            throw new Exception($"{requestId} not found");
        }

        if (request.MessageType != reply.MessageType)
        {
            throw new Exception($"invalid reply type set {reply.StreamType} expect {request.MessageType}");
        }

        Logger.LogDebug("receive {requestId} {streamType}-{messageType}, cost={cost}", requestId, reply.StreamType, request.MessageType, DateTimeOffset.UtcNow.Subtract(request.StartTime).TotalMilliseconds);
        request.Promise.TrySetResult(reply);
    }

    public async Task<StreamMessage> GetResultAsync(TaskCompletionSource<StreamMessage> promise, string requestId, int timeOut)
    {
        try
        {
            var completed = await Task.WhenAny(promise.Task, Task.Delay(timeOut));
            if (completed != promise.Task)
                throw new TimeoutException($"streaming call time out requestId {requestId}");

            var message = await promise.Task;
            return message;
        }
        finally
        {
            _promisePool.TryRemove(requestId, out _);
        }
    }

    public async Task PublishAsync(StreamMessage request, string pubkey)
    {
        await EventBus.PublishAsync(new StreamMessageReceivedEvent(request.ToByteString(), pubkey), false);
    }

    public async Task<Handshake> GetHandshakeAsync()
    {
        return await _handshakeProvider.GetHandshakeAsync();
    }

    public async Task<HandshakeValidationResult> ValidateHandshakeAsync(Handshake handshake)
    {
        return await _handshakeProvider.ValidateHandshakeAsync(handshake);
    }
}

public class StreamContext
{
    public readonly MessageType MessageType;
    public readonly TaskCompletionSource<StreamMessage> Promise;
    public readonly DateTimeOffset StartTime;

    public StreamContext(MessageType messageType, TaskCompletionSource<StreamMessage> promise)
    {
        MessageType = messageType;
        Promise = promise;
        StartTime = DateTimeOffset.UtcNow;
    }
}