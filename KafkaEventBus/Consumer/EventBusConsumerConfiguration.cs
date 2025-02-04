using Confluent.Kafka;

namespace KafkaEventBus.Consumer;

public class EventBusConsumerConfiguration : EventBusConfiguration
{
    public required ConsumerConfig ConsumerConfig { get; set; }
    public TimeSpan PollInterval { get; set; }
    public TimeSpan PullTimeout { get; set; }
    public int BatchSize { get; set; }
}