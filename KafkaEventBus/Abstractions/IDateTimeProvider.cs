namespace KafkaEventBus.Abstractions;

public interface IDateTimeProvider
{
    DateTime Now { get; }
    Task Delay(TimeSpan timeSpan, CancellationToken cancellationToken = default);
}