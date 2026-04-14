using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxNet.Interfaces;
using OutboxNet.Options;

namespace OutboxNet.Processor;

public sealed class OutboxProcessorService : BackgroundService
{
    private const int FanoutWarningThreshold = 500;

    private readonly IServiceProvider _serviceProvider;
    private readonly ProcessorOptions _processorOptions;
    private readonly OutboxOptions _outboxOptions;
    private readonly ILogger<OutboxProcessorService> _logger;

    public OutboxProcessorService(
        IServiceProvider serviceProvider,
        IOptions<ProcessorOptions> processorOptions,
        IOptions<OutboxOptions> outboxOptions,
        ILogger<OutboxProcessorService> logger)
    {
        _serviceProvider = serviceProvider;
        _processorOptions = processorOptions.Value;
        _outboxOptions = outboxOptions.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Outbox processor started. Polling interval: {Interval}s, adaptive: {Adaptive}",
            _processorOptions.PollingInterval.TotalSeconds, _processorOptions.AdaptivePolling);

        // Warn if the batch/concurrency product will open many simultaneous DB scopes.
        var fanout = _outboxOptions.BatchSize * _outboxOptions.MaxConcurrentDeliveries;
        if (fanout >= FanoutWarningThreshold)
        {
            _logger.LogWarning(
                "OutboxNet: BatchSize ({BatchSize}) × MaxConcurrentDeliveries ({MaxConcurrent}) = {Fanout}. " +
                "Each concurrent delivery opens its own DB scope and HTTP connection. " +
                "Consider reducing these values if you observe connection-pool exhaustion.",
                _outboxOptions.BatchSize, _outboxOptions.MaxConcurrentDeliveries, fanout);
        }

        var currentInterval = _processorOptions.PollingInterval;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(currentInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            try
            {
                using var scope = _serviceProvider.CreateScope();
                var processor = scope.ServiceProvider.GetRequiredService<IOutboxProcessor>();
                var processed = await processor.ProcessBatchAsync(stoppingToken);

                if (_processorOptions.AdaptivePolling)
                {
                    if (processed > 0)
                    {
                        // Messages found — reset to minimum interval so we drain quickly.
                        if (currentInterval != _processorOptions.PollingInterval)
                        {
                            currentInterval = _processorOptions.PollingInterval;
                            _logger.LogDebug("Outbox polling interval reset to {Interval}s", currentInterval.TotalSeconds);
                        }
                    }
                    else
                    {
                        // Idle batch — back off.
                        currentInterval = BackOff(currentInterval);
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Outbox processing batch failed");

                if (_processorOptions.AdaptivePolling)
                    currentInterval = BackOff(currentInterval);
            }
        }
    }

    private TimeSpan BackOff(TimeSpan current)
    {
        var next = TimeSpan.FromMilliseconds(current.TotalMilliseconds * _processorOptions.IdleBackoffFactor);
        if (next > _processorOptions.MaxPollingInterval) next = _processorOptions.MaxPollingInterval;
        _logger.LogDebug("Outbox polling backing off to {Interval}s", next.TotalSeconds);
        return next;
    }
}
