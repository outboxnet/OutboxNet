namespace OutboxNet.Models;

public class DeliveryAttempt
{
    public Guid Id { get; set; }
    public Guid OutboxMessageId { get; set; }
    public Guid WebhookSubscriptionId { get; set; }
    public int AttemptNumber { get; set; }
    public DeliveryStatus Status { get; set; }
    public int? HttpStatusCode { get; set; }
    public string? ResponseBody { get; set; }
    public string? ErrorMessage { get; set; }
    public long DurationMs { get; set; }
    public DateTimeOffset AttemptedAt { get; set; }
    public DateTimeOffset? NextRetryAt { get; set; }

    public OutboxMessage OutboxMessage { get; set; } = default!;
    public WebhookSubscription WebhookSubscription { get; set; } = default!;
}
