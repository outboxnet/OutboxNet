using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using OutboxNet.Interfaces;
using OutboxNet.Models;
using OutboxNet.Options;
using OutboxNet.Processor;
using Xunit;

namespace OutboxNet.Processor.Tests;

public class OutboxProcessingPipelineTests
{
    private readonly IOutboxStore _outboxStore = Substitute.For<IOutboxStore>();
    private readonly ISubscriptionStore _subscriptionStore = Substitute.For<ISubscriptionStore>();
    private readonly IDeliveryAttemptStore _deliveryAttemptStore = Substitute.For<IDeliveryAttemptStore>();
    private readonly IWebhookDeliverer _webhookDeliverer = Substitute.For<IWebhookDeliverer>();
    private readonly IRetryPolicy _retryPolicy = Substitute.For<IRetryPolicy>();

    private OutboxProcessingPipeline CreatePipeline(OutboxOptions? options = null)
    {
        var opts = Microsoft.Extensions.Options.Options.Create(options ?? new OutboxOptions());
        return new OutboxProcessingPipeline(
            _outboxStore,
            _subscriptionStore,
            _deliveryAttemptStore,
            _webhookDeliverer,
            _retryPolicy,
            opts,
            NullLogger<OutboxProcessingPipeline>.Instance);
    }

    private void SetupLockHeld()
    {
        _outboxStore.IsLockHeldAsync(Arg.Any<Guid>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(true);
    }

    private void SetupMarkAsProcessed()
    {
        _outboxStore.MarkAsProcessedAsync(Arg.Any<Guid>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(true);
    }

    private void SetupIncrementRetry()
    {
        _outboxStore.IncrementRetryAsync(Arg.Any<Guid>(), Arg.Any<string>(), Arg.Any<DateTimeOffset>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(true);
    }

    private void SetupMarkAsDeadLettered()
    {
        _outboxStore.MarkAsDeadLetteredAsync(Arg.Any<Guid>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(true);
    }

    [Fact]
    public async Task ProcessBatchAsync_ReleasesExpiredLocks_BeforeLocking()
    {
        _outboxStore.LockNextBatchAsync(Arg.Any<int>(), Arg.Any<TimeSpan>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<OutboxMessage>());

        var pipeline = CreatePipeline();
        await pipeline.ProcessBatchAsync();

        await _outboxStore.Received(1).ReleaseExpiredLocksAsync(Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatchAsync_NoMessages_DoesNothing()
    {
        _outboxStore.LockNextBatchAsync(Arg.Any<int>(), Arg.Any<TimeSpan>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<OutboxMessage>());

        var pipeline = CreatePipeline();
        await pipeline.ProcessBatchAsync();

        await _subscriptionStore.DidNotReceive().GetByEventTypeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatchAsync_WithMessage_NoSubscriptions_MarksAsProcessed()
    {
        SetupLockHeld();
        SetupMarkAsProcessed();

        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "order.placed",
            Payload = "{}",
            Status = MessageStatus.Processing,
            CreatedAt = DateTimeOffset.UtcNow
        };

        _outboxStore.LockNextBatchAsync(Arg.Any<int>(), Arg.Any<TimeSpan>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<OutboxMessage> { message });

        _subscriptionStore.GetByEventTypeAsync("order.placed", Arg.Any<CancellationToken>())
            .Returns(new List<WebhookSubscription>());

        var pipeline = CreatePipeline();
        await pipeline.ProcessBatchAsync();

        await _outboxStore.Received(1).MarkAsProcessedAsync(message.Id, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatchAsync_LockLost_SkipsDelivery()
    {
        _outboxStore.IsLockHeldAsync(Arg.Any<Guid>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(false);

        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "order.placed",
            Payload = "{}",
            Status = MessageStatus.Processing,
            CreatedAt = DateTimeOffset.UtcNow
        };

        _outboxStore.LockNextBatchAsync(Arg.Any<int>(), Arg.Any<TimeSpan>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<OutboxMessage> { message });

        var pipeline = CreatePipeline();
        await pipeline.ProcessBatchAsync();

        await _webhookDeliverer.DidNotReceive().DeliverAsync(Arg.Any<OutboxMessage>(), Arg.Any<WebhookSubscription>(), Arg.Any<CancellationToken>());
        await _outboxStore.DidNotReceive().MarkAsProcessedAsync(Arg.Any<Guid>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatchAsync_SuccessfulDelivery_MarksAsProcessed()
    {
        SetupLockHeld();
        SetupMarkAsProcessed();

        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "order.placed",
            Payload = """{"id":1}""",
            Status = MessageStatus.Processing,
            CreatedAt = DateTimeOffset.UtcNow
        };

        var subscription = new WebhookSubscription
        {
            Id = Guid.NewGuid(),
            EventType = "order.placed",
            WebhookUrl = "https://example.com/webhook",
            Secret = "secret",
            IsActive = true
        };

        _outboxStore.LockNextBatchAsync(Arg.Any<int>(), Arg.Any<TimeSpan>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<OutboxMessage> { message });

        _subscriptionStore.GetByEventTypeAsync("order.placed", Arg.Any<CancellationToken>())
            .Returns(new List<WebhookSubscription> { subscription });

        _deliveryAttemptStore.GetAttemptCountAsync(message.Id, subscription.Id, Arg.Any<CancellationToken>())
            .Returns(0);

        _webhookDeliverer.DeliverAsync(message, subscription, Arg.Any<CancellationToken>())
            .Returns(new DeliveryResult(true, 200, "OK", null, 50));

        var pipeline = CreatePipeline();
        await pipeline.ProcessBatchAsync();

        await _outboxStore.Received(1).MarkAsProcessedAsync(message.Id, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _deliveryAttemptStore.Received(1).SaveAttemptAsync(
            Arg.Is<DeliveryAttempt>(a => a.Status == DeliveryStatus.Success && a.AttemptNumber == 1),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatchAsync_FailedDelivery_WithRetriesRemaining_IncrementsRetry()
    {
        SetupLockHeld();
        SetupIncrementRetry();

        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "order.placed",
            Payload = "{}",
            Status = MessageStatus.Processing,
            RetryCount = 0,
            CreatedAt = DateTimeOffset.UtcNow
        };

        var subscription = new WebhookSubscription
        {
            Id = Guid.NewGuid(),
            EventType = "order.placed",
            WebhookUrl = "https://example.com/webhook",
            Secret = "secret",
            IsActive = true
        };

        _outboxStore.LockNextBatchAsync(Arg.Any<int>(), Arg.Any<TimeSpan>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<OutboxMessage> { message });

        _subscriptionStore.GetByEventTypeAsync("order.placed", Arg.Any<CancellationToken>())
            .Returns(new List<WebhookSubscription> { subscription });

        _deliveryAttemptStore.GetAttemptCountAsync(message.Id, subscription.Id, Arg.Any<CancellationToken>())
            .Returns(0);

        _webhookDeliverer.DeliverAsync(message, subscription, Arg.Any<CancellationToken>())
            .Returns(new DeliveryResult(false, 500, "Error", "HTTP 500", 100));

        _retryPolicy.GetNextDelay(0).Returns(TimeSpan.FromSeconds(10));

        var pipeline = CreatePipeline();
        await pipeline.ProcessBatchAsync();

        await _outboxStore.Received(1).IncrementRetryAsync(
            message.Id,
            Arg.Any<string>(),
            Arg.Any<DateTimeOffset>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatchAsync_FailedDelivery_RetriesExhausted_DeadLetters()
    {
        SetupLockHeld();
        SetupMarkAsDeadLettered();

        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "order.placed",
            Payload = "{}",
            Status = MessageStatus.Processing,
            RetryCount = 5,
            CreatedAt = DateTimeOffset.UtcNow
        };

        var subscription = new WebhookSubscription
        {
            Id = Guid.NewGuid(),
            EventType = "order.placed",
            WebhookUrl = "https://example.com/webhook",
            Secret = "secret",
            IsActive = true
        };

        _outboxStore.LockNextBatchAsync(Arg.Any<int>(), Arg.Any<TimeSpan>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<OutboxMessage> { message });

        _subscriptionStore.GetByEventTypeAsync("order.placed", Arg.Any<CancellationToken>())
            .Returns(new List<WebhookSubscription> { subscription });

        _deliveryAttemptStore.GetAttemptCountAsync(message.Id, subscription.Id, Arg.Any<CancellationToken>())
            .Returns(5);

        _webhookDeliverer.DeliverAsync(message, subscription, Arg.Any<CancellationToken>())
            .Returns(new DeliveryResult(false, 500, "Error", "HTTP 500", 100));

        _retryPolicy.GetNextDelay(5).Returns((TimeSpan?)null);

        var pipeline = CreatePipeline();
        await pipeline.ProcessBatchAsync();

        await _outboxStore.Received(1).MarkAsDeadLetteredAsync(message.Id, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ProcessBatchAsync_MultipleSubscriptions_AllMustSucceed()
    {
        SetupLockHeld();
        SetupIncrementRetry();

        var message = new OutboxMessage
        {
            Id = Guid.NewGuid(),
            EventType = "order.placed",
            Payload = "{}",
            Status = MessageStatus.Processing,
            CreatedAt = DateTimeOffset.UtcNow
        };

        var sub1 = new WebhookSubscription { Id = Guid.NewGuid(), EventType = "order.placed", WebhookUrl = "https://a.com", Secret = "s1", IsActive = true };
        var sub2 = new WebhookSubscription { Id = Guid.NewGuid(), EventType = "order.placed", WebhookUrl = "https://b.com", Secret = "s2", IsActive = true };

        _outboxStore.LockNextBatchAsync(Arg.Any<int>(), Arg.Any<TimeSpan>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<OutboxMessage> { message });

        _subscriptionStore.GetByEventTypeAsync("order.placed", Arg.Any<CancellationToken>())
            .Returns(new List<WebhookSubscription> { sub1, sub2 });

        _deliveryAttemptStore.GetAttemptCountAsync(Arg.Any<Guid>(), Arg.Any<Guid>(), Arg.Any<CancellationToken>()).Returns(0);

        // sub1 succeeds, sub2 fails
        _webhookDeliverer.DeliverAsync(message, sub1, Arg.Any<CancellationToken>())
            .Returns(new DeliveryResult(true, 200, "OK", null, 50));
        _webhookDeliverer.DeliverAsync(message, sub2, Arg.Any<CancellationToken>())
            .Returns(new DeliveryResult(false, 503, "Unavailable", "HTTP 503", 100));

        _retryPolicy.GetNextDelay(0).Returns(TimeSpan.FromSeconds(5));

        var pipeline = CreatePipeline();
        await pipeline.ProcessBatchAsync();

        // Should NOT mark as processed because sub2 failed
        await _outboxStore.DidNotReceive().MarkAsProcessedAsync(message.Id, Arg.Any<string>(), Arg.Any<CancellationToken>());
        // Should increment retry
        await _outboxStore.Received(1).IncrementRetryAsync(message.Id, Arg.Any<string>(), Arg.Any<DateTimeOffset>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }
}
