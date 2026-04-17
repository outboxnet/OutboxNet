using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxNet.Interfaces;
using OutboxNet.Options;

namespace OutboxNet.Processor;

public sealed class OutboxProcessorService : BackgroundService
{
    private const int FanoutWarningThreshold = 500;

    private readonly IOutboxProcessor _processor;
    private readonly IOutboxSignal _signal;
    private readonly ProcessorOptions _processorOptions;
    private readonly OutboxOptions _outboxOptions;
    private readonly ILogger<OutboxProcessorService> _logger;

    public OutboxProcessorService(
        IOutboxProcessor processor,
        IOutboxSignal signal,
        IOptions<ProcessorOptions> processorOptions,
        IOptions<OutboxOptions> outboxOptions,
        ILogger<OutboxProcessorService> logger)
    {
        _processor = processor;
        _signal = signal;
        _processorOptions = processorOptions.Value;
        _outboxOptions = outboxOptions.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Outbox processor started. Min interval: {Interval}ms, adaptive: {Adaptive}",
            _processorOptions.PollingInterval.TotalMilliseconds, _processorOptions.AdaptivePolling);

        // Warn if the batch/concurrency product will open many simultaneous DB scopes.
        var fanout = _outboxOptions.BatchSize * _outboxOptions.MaxConcurrentDeliveries
                     * _outboxOptions.MaxConcurrentSubscriptionDeliveries;
        if (fanout >= FanoutWarningThreshold)
        {
            _logger.LogWarning(
                "OutboxNet: BatchSize ({BatchSize}) × MaxConcurrentDeliveries ({MaxMsg}) × " +
                "MaxConcurrentSubscriptionDeliveries ({MaxSub}) = {Fanout} max concurrent HTTP requests. " +
                "Reduce these values if you observe connection-pool or thread-pool exhaustion.",
                _outboxOptions.BatchSize, _outboxOptions.MaxConcurrentDeliveries,
                _outboxOptions.MaxConcurrentSubscriptionDeliveries, fanout);
        }

        // currentInterval = TimeSpan.Zero → process the first batch immediately on startup
        // without waiting for a signal or delay.
        var currentInterval = TimeSpan.Zero;

        while (!stoppingToken.IsCancellationRequested)
        {
            // Wait for either:
            //   (a) a push signal from IOutboxPublisher  → near-zero latency
            //   (b) the polling timeout to elapse        → fallback safety net
            //   (c) zero interval (saturated / startup)  → process immediately
            if (currentInterval > TimeSpan.Zero)
            {
                try
                {
                    // WaitAsync returns as soon as IOutboxSignal.Notify() is called or timeout fires.
                    await _signal.WaitAsync(currentInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }

            int processed;
            try
            {
                processed = await _processor.ProcessBatchAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Outbox processing batch failed");

                // On error: back off from at least the base interval, not from zero.
                var basis = currentInterval == TimeSpan.Zero ? _processorOptions.PollingInterval : currentInterval;
                currentInterval = _processorOptions.AdaptivePolling ? BackOff(basis) : _processorOptions.PollingInterval;
                continue;
            }

            if (!_processorOptions.AdaptivePolling)
            {
                currentInterval = _processorOptions.PollingInterval;
                continue;
            }

            if (processed >= _outboxOptions.BatchSize)
            {
                // Queue is saturated — loop again immediately with no delay.
                currentInterval = TimeSpan.Zero;
            }
            else if (processed > 0)
            {
                // Partial batch — stay at minimum interval to drain quickly.
                currentInterval = _processorOptions.PollingInterval;
            }
            else
            {
                // Idle batch — back off exponentially; we'll be woken by the signal if a
                // new message arrives before the timeout elapses.
                var basis = currentInterval == TimeSpan.Zero ? _processorOptions.PollingInterval : currentInterval;
                currentInterval = BackOff(basis);
            }
        }
    }

    private TimeSpan BackOff(TimeSpan current)
    {
        var next = TimeSpan.FromMilliseconds(current.TotalMilliseconds * _processorOptions.IdleBackoffFactor);
        if (next > _processorOptions.MaxPollingInterval) next = _processorOptions.MaxPollingInterval;
        _logger.LogDebug("Outbox polling backing off to {Interval}ms", next.TotalMilliseconds);
        return next;
    }
}
