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

    /// <summary>
    /// Returns <c>true</c> if there is at least one successful delivery attempt
    /// for the given message + subscription pair.
    /// Used to skip re-delivering to subscriptions that already succeeded on a previous attempt.
    /// </summary>
    Task<bool> HasSuccessfulDeliveryAsync(Guid messageId, Guid subscriptionId, CancellationToken ct = default);

    /// <summary>
    /// Deletes delivery attempt records whose <c>AttemptedAt</c> is older than
    /// <paramref name="olderThan"/>. Returns the number of rows deleted.
    /// Call periodically (e.g. nightly) to prevent unbounded table growth.
    /// </summary>
    Task<int> PurgeOldAttemptsAsync(DateTimeOffset olderThan, CancellationToken ct = default);
}
