using Confluent.Kafka;

namespace KafkaEventBus.Producer;

public class EventBusProducerConfiguration : EventBusConfiguration
{
    public required ProducerConfig ProducerConfig { get; set; }
    public int BufferLength { get; set; }
    public TimeSpan DeliveryTimeout { get; set; }
    public TimeSpan CircularInterval { get; set; }
    
}