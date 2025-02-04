namespace KafkaEventBus.Abstractions;

public interface IConverterFactory
{
    IMessageConverter<T> GetMessageConverter<T>();
}