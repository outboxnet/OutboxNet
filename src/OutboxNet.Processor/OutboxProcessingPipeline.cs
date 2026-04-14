using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxNet.Interfaces;
using OutboxNet.Models;
using OutboxNet.Observability;
using OutboxNet.Options;

namespace OutboxNet.Processor;

public sealed class OutboxProcessingPipeline : IOutboxProcessor
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IRetryPolicy _retryPolicy;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxProcessingPipeline> _logger;

    public OutboxProcessingPipeline(
        IServiceScopeFactory scopeFactory,
        IRetryPolicy retryPolicy,
        IOptions<OutboxOptions> options,
        ILogger<OutboxProcessingPipeline> logger)
    {
        _scopeFactory = scopeFactory;
        _retryPolicy = retryPolicy;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<int> ProcessBatchAsync(CancellationToken ct = default)
    {
        using var activity = OutboxActivitySource.Source.StartActivity("outbox.process_batch");
        var batchStopwatch = Stopwatch.StartNew();
        var lockedBy = _options.InstanceId;

        try
        {
            // Batch-level operations are sequential — one scope is fine.
            using var batchScope = _scopeFactory.CreateScope();
            var sp = batchScope.ServiceProvider;
            var outboxStore = sp.GetRequiredService<IOutboxStore>();
            var messagePublisher = sp.GetService<IMessagePublisher>();

            await outboxStore.ReleaseExpiredLocksAsync(ct);

            var messages = await outboxStore.LockNextBatchAsync(
                _options.BatchSize,
                _options.DefaultVisibilityTimeout,
                lockedBy,
                ct);

            if (messages.Count == 0)
                return 0;

            OutboxMetrics.BatchesProcessed.Add(1);
            OutboxMetrics.BatchSize.Record(messages.Count);
            activity?.SetTag("outbox.batch_size", messages.Count);

            _logger.LogInformation("Processing batch of {Count} outbox messages", messages.Count);

            if (_options.ProcessingMode == ProcessingMode.QueueMediated && messagePublisher is not null)
            {
                await ProcessQueueMediatedAsync(messages, lockedBy, outboxStore, messagePublisher, ct);
            }
            else
            {
                await ProcessDirectDeliveryAsync(messages, lockedBy, ct);
            }

            return messages.Count;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error processing outbox batch");
            throw;
        }
        finally
        {
            batchStopwatch.Stop();
            OutboxMetrics.ProcessingDuration.Record(batchStopwatch.Elapsed.TotalMilliseconds);
        }
    }

    private async Task ProcessDirectDeliveryAsync(IReadOnlyList<OutboxMessage> messages, string lockedBy, CancellationToken ct)
    {
        await Parallel.ForEachAsync(
            messages,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = _options.MaxConcurrentDeliveries,
                CancellationToken = ct
            },
            async (message, token) =>
            {
                // Each concurrent task gets its own DI scope so that scoped services
                // (EfCoreOutboxStore / OutboxDbContext) are not shared across threads.
                using var messageScope = _scopeFactory.CreateScope();
                var msp = messageScope.ServiceProvider;

                await ProcessSingleMessageAsync(
                    message,
                    lockedBy,
                    msp.GetRequiredService<IOutboxStore>(),
                    msp.GetRequiredService<ISubscriptionReader>(),
                    msp.GetRequiredService<IDeliveryAttemptStore>(),
                    msp.GetRequiredService<IWebhookDeliverer>(),
                    token);
            });
    }

    private async Task ProcessQueueMediatedAsync(
        IReadOnlyList<OutboxMessage> messages,
        string lockedBy,
        IOutboxStore outboxStore,
        IMessagePublisher messagePublisher,
        CancellationToken ct)
    {
        foreach (var message in messages)
        {
            try
            {
                if (!await outboxStore.IsLockHeldAsync(message.Id, lockedBy, ct))
                {
                    _logger.LogWarning("Lock lost for message {MessageId} before queue publish, skipping", message.Id);
                    continue;
                }

                await messagePublisher.PublishAsync(message, ct);

                if (await outboxStore.MarkAsProcessedAsync(message.Id, lockedBy, ct))
                {
                    OutboxMetrics.MessagesProcessed.Add(1,
                        new KeyValuePair<string, object?>("event_type", message.EventType));
                }
                else
                {
                    _logger.LogWarning("Lock lost for message {MessageId} after queue publish (duplicate possible)", message.Id);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to publish message {MessageId} to queue", message.Id);
                await HandleMessageFailureAsync(message, lockedBy, outboxStore, ex.Message, ct);
            }
        }
    }

    private async Task ProcessSingleMessageAsync(
        OutboxMessage message,
        string lockedBy,
        IOutboxStore outboxStore,
        ISubscriptionReader subscriptionReader,
        IDeliveryAttemptStore attemptStore,
        IWebhookDeliverer deliverer,
        CancellationToken ct)
    {
        try
        {
            if (!await outboxStore.IsLockHeldAsync(message.Id, lockedBy, ct))
            {
                _logger.LogWarning("Lock lost for message {MessageId}, skipping delivery", message.Id);
                return;
            }

            var subscriptions = await subscriptionReader.GetForMessageAsync(message, ct);

            if (subscriptions.Count == 0)
            {
                _logger.LogDebug("No active subscriptions for event type {EventType}, marking as processed", message.EventType);
                await outboxStore.MarkAsProcessedAsync(message.Id, lockedBy, ct);
                OutboxMetrics.MessagesProcessed.Add(1,
                    new KeyValuePair<string, object?>("event_type", message.EventType));
                return;
            }

            var allDone = true;        // true when every sub either succeeded or is exhausted
            var anyPending = false;    // true when at least one sub still has retries remaining
            string? lastError = null;

            foreach (var subscription in subscriptions)
            {
                // Skip subscriptions already successfully delivered (#7).
                if (await attemptStore.HasSuccessfulDeliveryAsync(message.Id, subscription.Id, ct))
                    continue;

                var attemptCount = await attemptStore.GetAttemptCountAsync(message.Id, subscription.Id, ct);

                // Respect per-subscription MaxRetries (#12, #15).
                // MaxRetries is the number of retries after the first attempt, so total allowed = MaxRetries + 1.
                if (attemptCount > subscription.MaxRetries)
                {
                    _logger.LogWarning(
                        "Subscription {SubscriptionId} exhausted {Max} retries for message {MessageId}, skipping",
                        subscription.Id, subscription.MaxRetries, message.Id);
                    continue;
                }

                var result = await deliverer.DeliverAsync(message, subscription, ct);

                var attempt = new DeliveryAttempt
                {
                    Id = Guid.NewGuid(),
                    OutboxMessageId = message.Id,
                    WebhookSubscriptionId = subscription.Id,
                    AttemptNumber = attemptCount + 1,
                    Status = result.Success ? DeliveryStatus.Success : DeliveryStatus.Failed,
                    HttpStatusCode = result.HttpStatusCode,
                    ResponseBody = result.ResponseBody,
                    ErrorMessage = result.ErrorMessage,
                    DurationMs = result.DurationMs,
                    AttemptedAt = DateTimeOffset.UtcNow
                };

                await attemptStore.SaveAttemptAsync(attempt, ct);

                if (!result.Success)
                {
                    allDone = false;
                    anyPending = true;
                    lastError = result.ErrorMessage;
                }
            }

            if (allDone && !anyPending)
            {
                if (await outboxStore.MarkAsProcessedAsync(message.Id, lockedBy, ct))
                {
                    OutboxMetrics.MessagesProcessed.Add(1,
                        new KeyValuePair<string, object?>("event_type", message.EventType));
                }
                else
                {
                    _logger.LogWarning("Lock lost for message {MessageId} after successful delivery", message.Id);
                }
            }
            else
            {
                await HandleMessageFailureAsync(message, lockedBy, outboxStore, lastError ?? "One or more deliveries failed", ct);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error processing message {MessageId}", message.Id);
            await HandleMessageFailureAsync(message, lockedBy, outboxStore, ex.Message, ct);
        }
    }

    private async Task HandleMessageFailureAsync(
        OutboxMessage message,
        string lockedBy,
        IOutboxStore outboxStore,
        string error,
        CancellationToken ct)
    {
        var nextDelay = _retryPolicy.GetNextDelay(message.RetryCount);

        if (nextDelay.HasValue)
        {
            var nextRetryAt = DateTimeOffset.UtcNow.Add(nextDelay.Value);

            if (await outboxStore.IncrementRetryAsync(message.Id, lockedBy, nextRetryAt, error, ct))
            {
                OutboxMetrics.MessagesFailed.Add(1,
                    new KeyValuePair<string, object?>("event_type", message.EventType));

                _logger.LogWarning("Message {MessageId} failed (retry {RetryCount}), next retry at {NextRetryAt}",
                    message.Id, message.RetryCount + 1, nextRetryAt);
            }
            else
            {
                _logger.LogWarning("Lock lost for message {MessageId} during failure handling", message.Id);
            }
        }
        else
        {
            if (await outboxStore.MarkAsDeadLetteredAsync(message.Id, lockedBy, ct))
            {
                OutboxMetrics.MessagesDeadLettered.Add(1,
                    new KeyValuePair<string, object?>("event_type", message.EventType));

                _logger.LogError("Message {MessageId} exhausted retries, moved to dead letter", message.Id);
            }
            else
            {
                _logger.LogWarning("Lock lost for message {MessageId} during dead-letter handling", message.Id);
            }
        }
    }
}
