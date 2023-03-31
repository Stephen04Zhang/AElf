namespace AElf.OS.Network.Grpc;

public static class GrpcConstants
{
    public const string PubkeyMetadataKey = "public-key";
    public const string SessionIdMetadataKey = "session-id-bin";
    public const string PeerInfoMetadataKey = "peer-info";
    public const string TimeoutMetadataKey = "timeout";
    public const string RetryCountMetadataKey = "retry-count";
    public const string GrpcRequestCompressKey = "grpc-internal-encoding-request";

    public const string GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS = "grpc.keepalive_permit_without_calls";
    public const string GRPC_ARG_KEEPALIVE_TIMEOUT_MS = "grpc.keepalive_timeout_ms";
    public const string GRPC_ARG_KEEPALIVE_TIME_MS = "grpc.keepalive_time_ms";

    public const string GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS = "";
    public const string GRPC_ARG_HTTP2_MAX_PING_STRIKES = "";

    public const int GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS_OPEN = 1;
    public const int GRPC_ARG_KEEPALIVE_TIMEOUT_MS_VAL = 1000;
    public const int GRPC_ARG_KEEPALIVE_TIME_MS_VAL = 60 * 2 * 1000;

    public const string GrpcGzipConst = "gzip";

    public const int DefaultRequestTimeout = 200;

    public const int DefaultMaxReceiveMessageLength = 100 * 1024 * 1024;
    public const int DefaultMaxSendMessageLength = 100 * 1024 * 1024;

    public const int MaxSendBlockCountLimit = 50;

    public const string DefaultTlsCommonName = "aelf";

    public const int DefaultDiscoveryMaxNodesToResponse = 10;
    public const int StreamRecoveryWaitTime = 500;
}