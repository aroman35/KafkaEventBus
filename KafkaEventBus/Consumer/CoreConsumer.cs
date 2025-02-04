using Confluent.Kafka;
using KafkaEventBus.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaEventBus.Consumer;

public class CoreConsumer<TKey, TValue> : IDisposable
{
    private readonly IConsumer<TKey, TValue> _consumer;
    private readonly ILogger<CoreConsumer<TKey, TValue>> _logger;
    private readonly IOptions<EventBusConsumerConfiguration> _configuration; 
    private readonly string _topicName;

    public CoreConsumer(
        IOptionsSnapshot<EventBusConsumerConfiguration> consumerConfiguration,
        IConverterFactory converterFactory,
        ILogger<CoreConsumer<TKey, TValue>> logger,
        string topicName
        )
    {
        ArgumentNullException.ThrowIfNull(consumerConfiguration, nameof(consumerConfiguration));
        ArgumentNullException.ThrowIfNull(converterFactory, nameof(converterFactory));
        ArgumentNullException.ThrowIfNull(logger, nameof(logger));
        ArgumentNullException.ThrowIfNull(topicName, nameof(topicName));

        _logger = logger;
        _configuration = consumerConfiguration;
        
        _consumer = new ConsumerBuilder<TKey, TValue>(_configuration.Value.ConsumerConfig)
            .SetValueDeserializer(converterFactory.GetMessageConverter<TValue>())
            .SetKeyDeserializer(converterFactory.GetMessageConverter<TKey>())
            .SetErrorHandler((_, error) => _logger.LogError("Global error in producer: {Error}", error.Reason))
            .SetLogHandler((_, log) => _logger.Log(
                (LogLevel)log.LevelAs(LogLevelType.MicrosoftExtensionsLogging),
                log.Message))
            .SetStatisticsHandler((_, stat) => _logger.LogDebug("Producer statistics: {Statistics}", stat))
            .Build();
        _topicName = topicName;
    }

    public void Subscribe()
    {
        _consumer.Subscribe(_topicName);
    }

    public void Commit()
    {
        _consumer.Commit();
    }

    public IEnumerable<KeyValuePair<TKey, TValue>> ReadMessages(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var consumeResult = _consumer.Consume(_configuration.Value.PullTimeout);
            
            if (consumeResult is null || !consumeResult.IsPartitionEOF)
                break;
            
            yield return Convert(consumeResult.Message);
            
            if (consumeResult.Offset % _configuration.Value.BatchSize == 0)
                break;
        }
    }
    
    public void Dispose()
    {
        _consumer.Dispose();
    }

    private static KeyValuePair<TKey, TValue> Convert(Message<TKey, TValue> message)
    {
        return new KeyValuePair<TKey, TValue>(message.Key, message.Value);
    }
}