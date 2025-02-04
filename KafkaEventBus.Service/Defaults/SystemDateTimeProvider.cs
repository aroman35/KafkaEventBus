using KafkaEventBus.Abstractions;

namespace KafkaEventBus.Service.Defaults;

public class SystemDateTimeProvider : IDateTimeProvider
{
    public DateTime Now => DateTime.UtcNow;

    public Task Delay(TimeSpan timeSpan, CancellationToken cancellationToken = default)
    {
        return Task.Delay(timeSpan, cancellationToken);
    }
}