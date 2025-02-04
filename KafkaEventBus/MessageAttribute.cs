namespace KafkaEventBus;

public class TopicNameAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}