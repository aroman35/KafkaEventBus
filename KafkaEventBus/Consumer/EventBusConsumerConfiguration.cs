using Confluent.Kafka;

namespace KafkaEventBus.Consumer;

public class EventBusConsumerConfig
{
    public required ConsumerConfig ConsumerConfig { get; set; }
}