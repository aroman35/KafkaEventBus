using Confluent.Kafka;
using KafkaEventBus.Abstractions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaEventBus.Producer;

public class CoreProducer<TKey, TValue> : IDisposable
{
    private readonly IProducer<TKey, TValue> _producer;
    private readonly ILogger _logger;
    private readonly string _topicName;
    private readonly int _bufferLength;
    private readonly IDateTimeProvider _dateTimeProvider;
    private readonly IDeliveryErrorHandler _deliveryErrorHandler;
    private readonly TimeSpan _deliveryTimeout;
    private int _currentBufferedMessagesCount;

    public CoreProducer(
        IOptionsSnapshot<EventBusProducerConfiguration> producerConfig,
        IConverterFactory converterFactory,
        IDateTimeProvider dateTimeProvider,
        IDeliveryErrorHandler deliveryErrorHandler,
        ILogger<CoreProducer<TKey, TValue>> logger,
        string topicName)
    {
        ArgumentNullException.ThrowIfNull(producerConfig, nameof(producerConfig));
        ArgumentNullException.ThrowIfNull(converterFactory, nameof(converterFactory));
        ArgumentNullException.ThrowIfNull(dateTimeProvider, nameof(dateTimeProvider));
        ArgumentNullException.ThrowIfNull(deliveryErrorHandler, nameof(deliveryErrorHandler));
        ArgumentNullException.ThrowIfNull(logger, nameof(logger));
        ArgumentNullException.ThrowIfNull(topicName, nameof(topicName));

        _dateTimeProvider = dateTimeProvider;
        _logger = logger;
        _deliveryErrorHandler = deliveryErrorHandler;
        _producer = new ProducerBuilder<TKey, TValue>(producerConfig.Value.ProducerConfig)
            .SetValueSerializer(converterFactory.GetMessageConverter<TValue>())
            .SetKeySerializer(converterFactory.GetMessageConverter<TKey>())
            .SetErrorHandler((_, error) => _logger.LogError("Global error in producer: {Error}", error.Reason))
            .SetLogHandler((_, log) => _logger.Log(
                (LogLevel)log.LevelAs(LogLevelType.MicrosoftExtensionsLogging),
                log.Message))
            .SetStatisticsHandler((_, stat) => _logger.LogDebug("Producer statistics: {Statistics}", stat))
            .Build();
        _topicName = topicName;
        _bufferLength = producerConfig.Value.BufferLength;
        _deliveryTimeout = producerConfig.Value.DeliveryTimeout;
    }

    public void Send(TKey key, TValue value, Dictionary<string, byte[]>? headers = null)
    {
        _producer.Produce(
            _topicName,
            new Message<TKey, TValue>
            {
                Key = key,
                Value = value,
                Timestamp = new Timestamp(_dateTimeProvider.Now),
                Headers = Convert(headers)
            },
            DeliveryHandler);
        lock (_topicName)
        {
            Interlocked.Increment(ref _currentBufferedMessagesCount);
            if (Interlocked.CompareExchange(ref _currentBufferedMessagesCount, 0, _bufferLength) == _bufferLength)
            {
                var remainedMessages = _producer.Flush(_deliveryTimeout);
                if (remainedMessages > 0)
                {
                    _logger.LogWarning(
                        "Not all messages were flushed. Pay attention, the queue still contains {Count} messages",
                        remainedMessages);
                }
            }
        }
    }

    private void DeliveryHandler(DeliveryReport<TKey, TValue> deliveryReport)
    {
        if (deliveryReport.Error.IsError)
        {
            _deliveryErrorHandler.HandleError(deliveryReport.Error.Reason, deliveryReport.Key, deliveryReport.Value);
        }
    }

    private Headers Convert(Dictionary<string, byte[]>? headers)
    {
        var result = new Headers();
        if (headers is null || headers.Count == 0)
            return result;
        foreach (var (key, value) in headers)
        {
            var header = new Header(key, value);
            result.Add(header);
        }
        return result;
    }

    public void FlushExact(CancellationToken cancellationToken)
    {
        var remainedMessages = 0;
        do
        {
            remainedMessages = _producer.Flush(_deliveryTimeout);
        } while (!cancellationToken.IsCancellationRequested && remainedMessages != 0);
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
}