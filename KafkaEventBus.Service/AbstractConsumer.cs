using KafkaEventBus.Abstractions;
using KafkaEventBus.Consumer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaEventBus.Service;

public abstract class AbstractConsumer<TKey, TValue>(
    IOptionsSnapshot<EventBusConsumerConfiguration> options,
    IConverterFactory converterFactory,
    ILoggerFactory loggerFactory
    ) : BackgroundService
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<AbstractConsumer<TKey, TValue>>();
    private readonly CoreConsumer<TKey, TValue> _coreConsumer = new(
        options,
        converterFactory,
        loggerFactory.CreateLogger<CoreConsumer<TKey, TValue>>(),
        options.Value.TopicName<TValue>());

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _coreConsumer.Subscribe();
        return base.StartAsync(cancellationToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        _coreConsumer.Commit();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.Value.PollInterval);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var messages = _coreConsumer.ReadMessages(stoppingToken).ToList();
                if (!messages.Any())
                {
                    await timer.WaitForNextTickAsync(stoppingToken);
                    continue;
                }
                await HandleMessages(messages, stoppingToken);
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Error consuming messages.");
                throw;
            }
        }
    }
    
    protected abstract Task HandleMessages(List<KeyValuePair<TKey, TValue>> messages, CancellationToken cancellationToken);

    public override void Dispose()
    {
        _coreConsumer.Dispose();
        base.Dispose();
    }
}