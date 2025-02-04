using System.Reflection;

namespace KafkaEventBus;

public class EventBusConfiguration
{
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