namespace KafkaInfrastructure.Configuration;

public class KafkaOptions
{
    public string BootstrapServers { get; init; } = "localhost:9092";
    public string ConsumerGroupId { get; init; } = "";
    public string[] Topics { get; init; } = [];
}
