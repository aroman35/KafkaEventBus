namespace KafkaEventBus;

public class MessageAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}