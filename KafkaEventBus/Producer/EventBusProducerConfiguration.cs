using System.Reflection;
using Confluent.Kafka;

namespace KafkaEventBus;

public class EventBusProducerConfiguration
{
    public required ProducerConfig ProducerConfig { get; set; }
    public int BufferLength { get; set; }
    public TimeSpan DeliveryTimeout { get; set; }
    public TimeSpan CircularInterval { get; set; }
    public Dictionary<string, string> TopicNames { get; set; } = [];
    
    public string TopicName<T>()
    {
        var messageName = typeof(T).GetCustomAttribute<MessageAttribute>()?.Name
               ?? ThrowMissingMessageAttribute<T>();
        return TopicNames.TryGetValue(messageName, out var topicName) ? topicName : typeof(T).Name.ToLowerInvariant();
    }

    private static string ThrowMissingMessageAttribute<T>()
    {
        throw new InvalidOperationException(
            $"{typeof(T).Name} has no {nameof(MessageAttribute)}, make sure that all messages are configured with {nameof(MessageAttribute)}.");
    }
}