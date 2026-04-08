using OutboxNet.Models;

namespace OutboxNet.Interfaces;

public interface IDeliveryAttemptStore
{
    Task SaveAttemptAsync(DeliveryAttempt attempt, CancellationToken ct = default);

    Task<IReadOnlyList<DeliveryAttempt>> GetByMessageIdAsync(Guid messageId, CancellationToken ct = default);

    Task<IReadOnlyList<DeliveryAttempt>> GetBySubscriptionIdAsync(
        Guid subscriptionId,
        int limit = 50,
        CancellationToken ct = default);

    Task<int> GetAttemptCountAsync(Guid messageId, Guid subscriptionId, CancellationToken ct = default);
}
