using System.Collections.Concurrent;
using KafkaEventBus.Abstractions;
using KafkaEventBus.Producer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaEventBus.Service;

public class EventBus<TKey, TValue>(
    IOptionsSnapshot<EventBusProducerConfiguration> options,
    IConverterFactory converterFactory,
    IDateTimeProvider dateTimeProvider,
    IDeliveryErrorHandler deliveryErrorHandler,
    ILoggerFactory loggerFactory)
    : BackgroundService, IEventBus<TKey, TValue>
{
    private readonly ILogger<EventBus<TKey, TValue>> _logger = loggerFactory.CreateLogger<EventBus<TKey, TValue>>();
    private readonly CoreProducer<TKey, TValue> _producer = new(
        options,
        converterFactory,
        dateTimeProvider,
        deliveryErrorHandler,
        loggerFactory.CreateLogger<CoreProducer<TKey, TValue>>(),
        options.Value.TopicName<TValue>());

    private readonly ConcurrentQueue<EventMessage> _events = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.Value.CircularInterval);
        do
        {
            while (_events.TryDequeue(out var message))
            {
                try
                {
                    _producer.Send(message.Key, message.Value);
                }
                catch (Exception exception)
                {
                    _logger.LogError(exception, "Error while sending event message");
                }
            }
        } while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken));
    }

    public void Publish(TValue message, Func<TValue, TKey> keySelector)
    {
        _events.Enqueue(new EventMessage(keySelector.Invoke(message), message));
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        _producer.FlushExact(cancellationToken);
    }

    public override void Dispose()
    {
        _producer.Dispose();
        base.Dispose();
    }

    private record EventMessage(TKey Key, TValue Value);
}