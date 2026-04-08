using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxNet.Interfaces;
using OutboxNet.Models;
using OutboxNet.Observability;
using OutboxNet.Options;

namespace OutboxNet.Processor;

public sealed class OutboxProcessingPipeline : IOutboxProcessor
{
    private readonly IOutboxStore _outboxStore;
    private readonly ISubscriptionStore _subscriptionStore;
    private readonly IDeliveryAttemptStore _deliveryAttemptStore;
    private readonly IWebhookDeliverer _webhookDeliverer;
    private readonly IRetryPolicy _retryPolicy;
    private readonly IMessagePublisher? _messagePublisher;
    private readonly OutboxOptions _options;
    private readonly ILogger<OutboxProcessingPipeline> _logger;

    public OutboxProcessingPipeline(
        IOutboxStore outboxStore,
        ISubscriptionStore subscriptionStore,
        IDeliveryAttemptStore deliveryAttemptStore,
        IWebhookDeliverer webhookDeliverer,
        IRetryPolicy retryPolicy,
        IOptions<OutboxOptions> options,
        ILogger<OutboxProcessingPipeline> logger,
        IMessagePublisher? messagePublisher = null)
    {
        _outboxStore = outboxStore;
        _subscriptionStore = subscriptionStore;
        _deliveryAttemptStore = deliveryAttemptStore;
        _webhookDeliverer = webhookDeliverer;
        _retryPolicy = retryPolicy;
        _messagePublisher = messagePublisher;
        _options = options.Value;
        _logger = logger;
    }

    public async Task ProcessBatchAsync(CancellationToken ct = default)
    {
        using var activity = OutboxActivitySource.Source.StartActivity("outbox.process_batch");
        var batchStopwatch = Stopwatch.StartNew();
        var lockedBy = _options.InstanceId;

        try
        {
            await _outboxStore.ReleaseExpiredLocksAsync(ct);

            var messages = await _outboxStore.LockNextBatchAsync(
                _options.BatchSize,
                _options.DefaultVisibilityTimeout,
                lockedBy,
                ct);

            if (messages.Count == 0)
                return;

            OutboxMetrics.BatchesProcessed.Add(1);
            OutboxMetrics.BatchSize.Record(messages.Count);
            activity?.SetTag("outbox.batch_size", messages.Count);

            _logger.LogInformation("Processing batch of {Count} outbox messages", messages.Count);

            if (_options.ProcessingMode == ProcessingMode.QueueMediated && _messagePublisher is not null)
            {
                await ProcessQueueMediatedAsync(messages, lockedBy, ct);
            }
            else
            {
                await ProcessDirectDeliveryAsync(messages, lockedBy, ct);
            }
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
            async (message, token) => await ProcessSingleMessageAsync(message, lockedBy, token));
    }

    private async Task ProcessQueueMediatedAsync(IReadOnlyList<OutboxMessage> messages, string lockedBy, CancellationToken ct)
    {
        foreach (var message in messages)
        {
            try
            {
                if (!await _outboxStore.IsLockHeldAsync(message.Id, lockedBy, ct))
                {
                    _logger.LogWarning("Lock lost for message {MessageId} before queue publish, skipping", message.Id);
                    continue;
                }

                await _messagePublisher!.PublishAsync(message, ct);

                if (await _outboxStore.MarkAsProcessedAsync(message.Id, lockedBy, ct))
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
                await HandleMessageFailureAsync(message, lockedBy, ex.Message, ct);
            }
        }
    }

    private async Task ProcessSingleMessageAsync(OutboxMessage message, string lockedBy, CancellationToken ct)
    {
        try
        {
            // Verify we still own the lock before doing any work.
            // Protects against the case where processing the previous message in
            // this batch took long enough for the visibility timeout to expire.
            if (!await _outboxStore.IsLockHeldAsync(message.Id, lockedBy, ct))
            {
                _logger.LogWarning("Lock lost for message {MessageId}, skipping delivery", message.Id);
                return;
            }

            var subscriptions = await _subscriptionStore.GetByEventTypeAsync(message.EventType, ct);

            if (subscriptions.Count == 0)
            {
                _logger.LogDebug("No active subscriptions for event type {EventType}, marking as processed", message.EventType);
                await _outboxStore.MarkAsProcessedAsync(message.Id, lockedBy, ct);
                OutboxMetrics.MessagesProcessed.Add(1,
                    new KeyValuePair<string, object?>("event_type", message.EventType));
                return;
            }

            var allSucceeded = true;
            string? lastError = null;

            foreach (var subscription in subscriptions)
            {
                var attemptCount = await _deliveryAttemptStore.GetAttemptCountAsync(
                    message.Id, subscription.Id, ct);

                var result = await _webhookDeliverer.DeliverAsync(message, subscription, ct);

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

                await _deliveryAttemptStore.SaveAttemptAsync(attempt, ct);

                if (!result.Success)
                {
                    allSucceeded = false;
                    lastError = result.ErrorMessage;
                }
            }

            if (allSucceeded)
            {
                if (await _outboxStore.MarkAsProcessedAsync(message.Id, lockedBy, ct))
                {
                    OutboxMetrics.MessagesProcessed.Add(1,
                        new KeyValuePair<string, object?>("event_type", message.EventType));
                }
                else
                {
                    _logger.LogWarning("Lock lost for message {MessageId} after successful delivery (another instance may re-deliver)", message.Id);
                }
            }
            else
            {
                await HandleMessageFailureAsync(message, lockedBy, lastError ?? "Delivery failed", ct);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error processing message {MessageId}", message.Id);
            await HandleMessageFailureAsync(message, lockedBy, ex.Message, ct);
        }
    }

    private async Task HandleMessageFailureAsync(OutboxMessage message, string lockedBy, string error, CancellationToken ct)
    {
        var nextDelay = _retryPolicy.GetNextDelay(message.RetryCount);

        if (nextDelay.HasValue)
        {
            var nextRetryAt = DateTimeOffset.UtcNow.Add(nextDelay.Value);

            if (await _outboxStore.IncrementRetryAsync(message.Id, lockedBy, nextRetryAt, error, ct))
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
            if (await _outboxStore.MarkAsDeadLetteredAsync(message.Id, lockedBy, ct))
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
