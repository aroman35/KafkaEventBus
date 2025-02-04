using Confluent.Kafka;

namespace KafkaEventBus.Abstractions;

public interface IMessageConverter<TMessage> : ISerializer<TMessage>, IDeserializer<TMessage>
{
}