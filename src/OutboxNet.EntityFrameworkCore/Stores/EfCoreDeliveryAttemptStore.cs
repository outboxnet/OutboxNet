using Microsoft.EntityFrameworkCore;
using OutboxNet.Interfaces;
using OutboxNet.Models;

namespace OutboxNet.EntityFrameworkCore.Stores;

internal sealed class EfCoreDeliveryAttemptStore : IDeliveryAttemptStore
{
    private readonly OutboxDbContext _dbContext;

    public EfCoreDeliveryAttemptStore(OutboxDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public async Task SaveAttemptAsync(DeliveryAttempt attempt, CancellationToken ct = default)
    {
        _dbContext.DeliveryAttempts.Add(attempt);
        await _dbContext.SaveChangesAsync(ct);
    }

    /// <summary>
    /// Persists all attempts in a single <c>SaveChangesAsync</c> call (one round-trip).
    /// </summary>
    public async Task SaveAttemptsAsync(IReadOnlyList<DeliveryAttempt> attempts, CancellationToken ct = default)
    {
        if (attempts.Count == 0) return;
        _dbContext.DeliveryAttempts.AddRange(attempts);
        await _dbContext.SaveChangesAsync(ct);
    }

    public async Task<IReadOnlyList<DeliveryAttempt>> GetByMessageIdAsync(Guid messageId, CancellationToken ct = default)
    {
        return await _dbContext.DeliveryAttempts
            .Where(d => d.OutboxMessageId == messageId)
            .OrderBy(d => d.AttemptNumber)
            .ToListAsync(ct);
    }

    public async Task<IReadOnlyList<DeliveryAttempt>> GetBySubscriptionIdAsync(
        Guid subscriptionId,
        int limit = 50,
        CancellationToken ct = default)
    {
        return await _dbContext.DeliveryAttempts
            .Where(d => d.WebhookSubscriptionId == subscriptionId)
            .OrderByDescending(d => d.AttemptedAt)
            .Take(limit)
            .ToListAsync(ct);
    }

    public async Task<IReadOnlyDictionary<Guid, SubscriptionDeliveryState>> GetDeliveryStatesAsync(
        Guid messageId,
        IReadOnlyList<Guid> subscriptionIds,
        CancellationToken ct = default)
    {
        if (subscriptionIds.Count == 0)
            return new Dictionary<Guid, SubscriptionDeliveryState>();

        var rows = await _dbContext.DeliveryAttempts
            .Where(d => d.OutboxMessageId == messageId && subscriptionIds.Contains(d.WebhookSubscriptionId))
            .GroupBy(d => d.WebhookSubscriptionId)
            .Select(g => new
            {
                SubscriptionId = g.Key,
                AttemptCount = g.Count(),
                HasSuccess = g.Any(d => d.Status == DeliveryStatus.Success)
            })
            .ToListAsync(ct);

        return rows.ToDictionary(
            r => r.SubscriptionId,
            r => new SubscriptionDeliveryState(r.AttemptCount, r.HasSuccess));
    }

    public async Task<int> PurgeOldAttemptsAsync(DateTimeOffset olderThan, CancellationToken ct = default)
    {
        return await _dbContext.DeliveryAttempts
            .Where(d => d.AttemptedAt < olderThan)
            .ExecuteDeleteAsync(ct);
    }
}
