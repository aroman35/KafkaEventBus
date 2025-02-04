using Microsoft.Extensions.DependencyInjection;

namespace KafkaEventBus.Service.Configuration;

public static class EventBusConfigurationExtensions
{
    public static IServiceCollection AddEventBus(
        this IServiceCollection serviceCollection,
        Action<EventBusBuilder> configurationBuilder)
    {
        var builder = new EventBusBuilder(serviceCollection);
        configurationBuilder.Invoke(builder);
        builder.Validate();
        return serviceCollection;
    }

    public static IServiceCollection AddConsumers(
        this IServiceCollection serviceCollection,
        Action<ConsumerBuilder> configurationBuilder)
    {
        var builder = new ConsumerBuilder(serviceCollection);
        configurationBuilder.Invoke(builder);
        builder.Validate();
        return serviceCollection;
    }
}