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

    public async Task<int> GetAttemptCountAsync(Guid messageId, Guid subscriptionId, CancellationToken ct = default)
    {
        return await _dbContext.DeliveryAttempts
            .CountAsync(d => d.OutboxMessageId == messageId
                          && d.WebhookSubscriptionId == subscriptionId, ct);
    }

    public async Task<bool> HasSuccessfulDeliveryAsync(Guid messageId, Guid subscriptionId, CancellationToken ct = default)
    {
        return await _dbContext.DeliveryAttempts
            .AnyAsync(d => d.OutboxMessageId == messageId
                        && d.WebhookSubscriptionId == subscriptionId
                        && d.Status == DeliveryStatus.Success, ct);
    }

    public async Task<int> PurgeOldAttemptsAsync(DateTimeOffset olderThan, CancellationToken ct = default)
    {
        return await _dbContext.DeliveryAttempts
            .Where(d => d.AttemptedAt < olderThan)
            .ExecuteDeleteAsync(ct);
    }
}
