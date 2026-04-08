using Microsoft.Extensions.DependencyInjection;
using OutboxNet.Interfaces;
using OutboxNet.Options;
using OutboxNet.Serialization;

namespace OutboxNet.Extensions;

public static class ServiceCollectionExtensions
{
    public static IOutboxNetBuilder AddOutboxNet(
        this IServiceCollection services,
        Action<OutboxOptions>? configure = null)
    {
        var options = new OutboxOptions();
        configure?.Invoke(options);

        services.Configure<OutboxOptions>(o =>
        {
            o.SchemaName = options.SchemaName;
            o.BatchSize = options.BatchSize;
            o.DefaultVisibilityTimeout = options.DefaultVisibilityTimeout;
            o.InstanceId = options.InstanceId;
            o.MaxConcurrentDeliveries = options.MaxConcurrentDeliveries;
            o.ProcessingMode = options.ProcessingMode;
        });

        services.AddSingleton<IMessageSerializer, JsonMessageSerializer>();

        return new OutboxNetBuilder(services);
    }
}
