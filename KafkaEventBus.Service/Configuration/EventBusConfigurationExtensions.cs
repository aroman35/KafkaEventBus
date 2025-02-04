using Microsoft.Extensions.DependencyInjection;

namespace KafkaEventBus.Service;

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
}