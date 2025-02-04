using KafkaEventBus.Abstractions;
using Microsoft.Extensions.Logging;

namespace KafkaEventBus.Service.Defaults;

public class LoggingErrorHandler(ILogger<LoggingErrorHandler> logger) : IDeliveryErrorHandler
{
    private readonly ILogger _logger = logger;

    public void HandleError<TKey, TValue>(string errorMessage, TKey key, TValue value)
    {
        _logger.LogError("Unable to get ack: {ErrorMessage}", errorMessage);
    }
}