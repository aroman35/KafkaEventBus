using Confluent.Kafka;
using KafkaEventBus.Abstractions;

namespace KafkaEventBus.Service.Defaults;

public class JsonConverter : IConverterFactory
{
    public IMessageConverter<T> GetMessageConverter<T>()
    {
        return new JsonMessageConverter<T>();
    }
    
    private class JsonMessageConverter<TMessage> : IMessageConverter<TMessage>
    {
        public byte[] Serialize(TMessage data, SerializationContext context)
        {
            return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(data);
        }

        public TMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            var message = System.Text.Json.JsonSerializer.Deserialize<TMessage>(data);
            if (message is not null)
                return message;
            if (isNull)
                return default!;
            throw new MessageNullException();
        }
    }
}