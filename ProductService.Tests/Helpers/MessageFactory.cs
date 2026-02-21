using Confluent.Kafka;
using Google.Protobuf;

namespace ProductService.Tests.Helpers;

/// <summary>
/// Builds ConsumeResult&lt;string, byte[]&gt; test fixtures for processor tests.
/// </summary>
public static class MessageFactory
{
    public static ConsumeResult<string, byte[]> Build(string topic, string? key, byte[]? value) =>
        new()
        {
            Topic = topic,
            Partition = new Partition(0),
            Offset = new Offset(0),
            Message = new Message<string, byte[]>
            {
                Key = key!,
                Value = value!
            }
        };

    public static ConsumeResult<string, byte[]> Build(string topic, string? key, IMessage proto) =>
        Build(topic, key, proto.ToByteArray());
}
