using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Volo.Abp.DependencyInjection;

namespace AElf.OS.Network.Grpc;

public interface IStreamTaskResourcePool
{
    Task RegistryTaskPromiseAsync(string requestId, MessageType messageType, TaskCompletionSource<StreamMessage> promise);
    void TrySetResult(string requestId, StreamMessage reply);
    Task<StreamMessage> GetResultAsync(TaskCompletionSource<StreamMessage> promise, string requestId, int timeOut);
}

public class StreamTaskResourcePool : IStreamTaskResourcePool, ISingletonDependency
{
    private readonly ConcurrentDictionary<string, StreamContext> _promisePool;
    public ILogger<StreamTaskResourcePool> Logger { get; set; }

    public StreamTaskResourcePool()
    {
        _promisePool = new ConcurrentDictionary<string, StreamContext>();
        Logger = NullLogger<StreamTaskResourcePool>.Instance;
    }

    public Task RegistryTaskPromiseAsync(string requestId, MessageType messageType, TaskCompletionSource<StreamMessage> promise)
    {
        _promisePool[requestId] = new StreamContext(messageType, promise);
        return Task.CompletedTask;
    }

    public void TrySetResult(string requestId, StreamMessage reply)
    {
        AssertContains(requestId);
        var request = _promisePool[requestId];
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

    private void AssertContains(string requestId)
    {
        if (!_promisePool.ContainsKey(requestId))
        {
            throw new Exception($"{requestId} not found");
        }
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