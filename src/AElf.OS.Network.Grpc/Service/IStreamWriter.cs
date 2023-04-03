using System.Threading.Tasks;
using Grpc.Core;

namespace AElf.OS.Network.Grpc;

public interface IStreamWriter : IAsyncStreamWriter<StreamMessage>
{
    IAsyncStreamWriter<StreamMessage> GetResponseStream();
}

public class StreamRequestWriter : IStreamWriter
{
    private readonly IAsyncStreamWriter<StreamMessage> _responseStream;

    public StreamRequestWriter(IAsyncStreamWriter<StreamMessage> responseStream)
    {
        _responseStream = responseStream;
    }

    public async Task WriteAsync(StreamMessage message)
    {
        await _responseStream.WriteAsync(message);
    }

    public WriteOptions WriteOptions { get; set; }

    public IAsyncStreamWriter<StreamMessage> GetResponseStream()
    {
        return _responseStream;
    }
}