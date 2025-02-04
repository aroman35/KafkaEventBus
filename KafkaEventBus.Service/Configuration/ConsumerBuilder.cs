using KafkaEventBus.Abstractions;
using KafkaEventBus.Consumer;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaEventBus.Service.Configuration;

public class ConsumerBuilder
{
    private bool _converterConfigured;
    private bool _optionsConfigured;

    private readonly IServiceCollection _services;

    internal ConsumerBuilder(IServiceCollection services)
    {
        _services = services;
    }
    
    public ConsumerBuilder WithConverter<TConverter>()
        where TConverter : class, IConverterFactory
    {
        _services.AddSingleton<IConverterFactory, TConverter>();
        _converterConfigured = true;
        return this;
    }
    
    public ConsumerBuilder WithConverter<TConverter>(Func<IServiceProvider, TConverter> converterFactory)
        where TConverter : class, IConverterFactory
    {
        _services.AddSingleton<IConverterFactory, TConverter>(converterFactory);
        _converterConfigured = true;
        return this;
    }
    
    public ConsumerBuilder WithOptions(EventBusConsumerConfiguration options)
    {
        _services.ConfigureOptions(options);
        _optionsConfigured = true;
        return this;
    }
    
    /// TODO: pass extra consumer configuration as an Action parameter
    public ConsumerBuilder AddMessageConsumer<TKey, TValue, TConsumer>()
        where TConsumer : AbstractConsumer<TKey, TValue>
    {
        _services.AddHostedService<AbstractConsumer<TKey, TValue>>();
        return this;
    }
    
    internal void Validate()
    {
        if (!_converterConfigured)
            throw new InvalidOperationException($"{nameof(IConverterFactory)} is not configured");
        if (!_optionsConfigured)
            throw new InvalidOperationException($"{nameof(EventBusConsumerConfiguration)} is not configured");
    }
}