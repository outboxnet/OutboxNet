using OutboxNet.Models;

namespace OutboxNet.Interfaces;

public interface IWebhookDeliverer
{
    Task<DeliveryResult> DeliverAsync(
        OutboxMessage message,
        WebhookSubscription subscription,
        CancellationToken ct = default);
}
