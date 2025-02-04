using KafkaEventBus.Abstractions;
using KafkaEventBus.Producer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KafkaEventBus.Service.Configuration;

public class EventBusBuilder
{
    private readonly IServiceCollection _services;

    private bool _converterConfigured;
    private bool _optionsConfigured;
    private bool _dateTimeProviderConfigured;
    private bool _errorHandlerConfigured;

    internal EventBusBuilder(IServiceCollection services)
    {
        _services = services;
    }

    public EventBusBuilder WithConverter<TConverter>()
        where TConverter : class, IConverterFactory
    {
        _services.AddSingleton<IConverterFactory, TConverter>();
        _converterConfigured = true;
        return this;
    }
    
    public EventBusBuilder WithConverter<TConverter>(Func<IServiceProvider, TConverter> converterFactory)
        where TConverter : class, IConverterFactory
    {
        _services.AddSingleton<IConverterFactory, TConverter>(converterFactory);
        _converterConfigured = true;
        return this;
    }

    public EventBusBuilder WithDateTimeProvider<TDateTimeProvider>()
        where TDateTimeProvider : class, IDateTimeProvider
    {
        _services.AddSingleton<IDateTimeProvider, TDateTimeProvider>();
        _dateTimeProviderConfigured = true;
        return this;
    }
    
    public EventBusBuilder WithDateTimeProvider<TDateTimeProvider>(Func<IServiceProvider, TDateTimeProvider> dateTimeProvider)
        where TDateTimeProvider : class, IDateTimeProvider
    {
        _services.AddSingleton<IDateTimeProvider, TDateTimeProvider>(dateTimeProvider);
        _dateTimeProviderConfigured = true;
        return this;
    }

    public EventBusBuilder WithErrorHandler<THandler>()
        where THandler : class, IDeliveryErrorHandler
    {
        _services.AddSingleton<IDeliveryErrorHandler, THandler>();
        _errorHandlerConfigured = true;
        return this;
    }

    public EventBusBuilder WithErrorHandler<THandler>(Func<IServiceProvider, THandler> handlerFactory)
        where THandler : class, IDeliveryErrorHandler
    {
        _services.AddSingleton<IDeliveryErrorHandler, THandler>(handlerFactory);
        _errorHandlerConfigured = true;
        return this;
    }

    public EventBusBuilder ForMessage<TKey, TValue>()
    {
        _services.AddSingleton<IEventBus<TKey, TValue>>();
        _services.AddHostedService(provider => (IHostedService)provider.GetRequiredService<IEventBus<TKey, TValue>>());
        
        return this;
    }

    public EventBusBuilder WithOptions(EventBusProducerConfiguration options)
    {
        _services.ConfigureOptions(options);
        _optionsConfigured = true;
        return this;
    }

    internal void Validate()
    {
        if (!_converterConfigured)
            throw new InvalidOperationException($"{nameof(IConverterFactory)} is not configured");
        if (!_optionsConfigured)
            throw new InvalidOperationException($"{nameof(EventBusProducerConfiguration)} is not configured");
        if (!_dateTimeProviderConfigured)
            throw new InvalidOperationException($"{nameof(IDateTimeProvider)} is not configured");
        if (!_errorHandlerConfigured)
            throw new InvalidOperationException($"{nameof(IDeliveryErrorHandler)} is not configured");
    }
}