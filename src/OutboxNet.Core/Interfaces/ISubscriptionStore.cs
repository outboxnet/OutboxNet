using OutboxNet.Models;

namespace OutboxNet.Interfaces;

public interface ISubscriptionStore
{
    Task<WebhookSubscription> AddAsync(WebhookSubscription subscription, CancellationToken ct = default);
    Task<WebhookSubscription?> GetByIdAsync(Guid id, CancellationToken ct = default);
    Task<IReadOnlyList<WebhookSubscription>> GetByEventTypeAsync(string eventType, CancellationToken ct = default);
    Task UpdateAsync(WebhookSubscription subscription, CancellationToken ct = default);
    Task DeactivateAsync(Guid id, CancellationToken ct = default);
}
