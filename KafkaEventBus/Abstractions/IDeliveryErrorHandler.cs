namespace KafkaEventBus.Abstractions;

public interface IDeliveryErrorHandler
{
    void HandleError<TKey, TValue>(string errorMessage, TKey key, TValue value);
}