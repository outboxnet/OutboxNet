using OutboxNet.Models;

namespace OutboxNet.Interfaces;

public interface IOutboxStore
{
    Task SaveMessageAsync(OutboxMessage message, CancellationToken ct = default);

    Task<IReadOnlyList<OutboxMessage>> LockNextBatchAsync(
        int batchSize,
        TimeSpan visibilityTimeout,
        string lockedBy,
        CancellationToken ct = default);

    /// <summary>
    /// Marks a message as delivered. Only succeeds if <paramref name="lockedBy"/>
    /// still owns the lock. Returns <c>false</c> if the lock was stolen.
    /// </summary>
    Task<bool> MarkAsProcessedAsync(Guid messageId, string lockedBy, CancellationToken ct = default);

    /// <summary>
    /// Marks a message as permanently failed. Only succeeds if <paramref name="lockedBy"/>
    /// still owns the lock. Returns <c>false</c> if the lock was stolen.
    /// </summary>
    Task<bool> MarkAsFailedAsync(Guid messageId, string lockedBy, string error, CancellationToken ct = default);

    /// <summary>
    /// Increments the retry count and schedules the next attempt. Only succeeds if
    /// <paramref name="lockedBy"/> still owns the lock. Returns <c>false</c> if the lock was stolen.
    /// </summary>
    Task<bool> IncrementRetryAsync(Guid messageId, string lockedBy, DateTimeOffset nextRetryAt, string? error = null, CancellationToken ct = default);

    /// <summary>
    /// Moves a message to the dead-letter state. Only succeeds if <paramref name="lockedBy"/>
    /// still owns the lock. Returns <c>false</c> if the lock was stolen.
    /// </summary>
    Task<bool> MarkAsDeadLetteredAsync(Guid messageId, string lockedBy, CancellationToken ct = default);

    /// <summary>
    /// Checks whether the given lock is still held for a message.
    /// Returns <c>true</c> if the message exists with <c>Status == Processing</c>,
    /// the matching <paramref name="lockedBy"/>, and a <c>LockedUntil</c> in the future.
    /// </summary>
    Task<bool> IsLockHeldAsync(Guid messageId, string lockedBy, CancellationToken ct = default);

    Task ReleaseExpiredLocksAsync(CancellationToken ct = default);
}
