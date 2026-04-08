using Microsoft.Extensions.DependencyInjection;
using OutboxNet.Extensions;
using OutboxNet.Interfaces;
using OutboxNet.Processor;

namespace OutboxNet.AzureFunctions.Extensions;

public static class ServiceCollectionExtensions
{
    public static IOutboxNetBuilder AddAzureFunctionsProcessor(this IOutboxNetBuilder builder)
    {
        builder.Services.AddScoped<IOutboxProcessor, OutboxProcessingPipeline>();
        return builder;
    }
}
