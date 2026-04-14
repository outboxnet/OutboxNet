using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OutboxNet.Interfaces;

namespace OutboxNet.AzureFunctions;

public sealed class OutboxTimerFunction
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<OutboxTimerFunction> _logger;

    public OutboxTimerFunction(
        IServiceProvider serviceProvider,
        ILogger<OutboxTimerFunction> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    [Function("OutboxProcessor")]
    public async Task RunAsync(
        // Override via host.json / appsettings: "Outbox:TimerCron": "*/10 * * * * *"
        [TimerTrigger("%Outbox:TimerCron%")] TimerInfo timer,
        CancellationToken ct)
    {
        _logger.LogDebug("Outbox timer function triggered at {Time}", DateTimeOffset.UtcNow);

        using var scope = _serviceProvider.CreateScope();
        var processor = scope.ServiceProvider.GetRequiredService<IOutboxProcessor>();
        _ = await processor.ProcessBatchAsync(ct);
    }
}
