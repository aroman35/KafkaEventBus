namespace KafkaEventBus.Abstractions;

public interface IEventBus<TKey, TValue>
{
    void Publish(TKey key, TValue message)
    {
        Publish(message, _ => key);
    }
    void Publish(TValue message, Func<TValue, TKey> keySelector);
}