using Microsoft.EntityFrameworkCore;
using OutboxNet.Interfaces;
using OutboxNet.Models;

namespace OutboxNet.EntityFrameworkCore.Stores;

internal sealed class EfCoreSubscriptionStore : ISubscriptionStore
{
    private readonly OutboxDbContext _dbContext;

    public EfCoreSubscriptionStore(OutboxDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public async Task<WebhookSubscription> AddAsync(WebhookSubscription subscription, CancellationToken ct = default)
    {
        subscription.CreatedAt = DateTimeOffset.UtcNow;
        _dbContext.WebhookSubscriptions.Add(subscription);
        await _dbContext.SaveChangesAsync(ct);
        return subscription;
    }

    public async Task<WebhookSubscription?> GetByIdAsync(Guid id, CancellationToken ct = default)
    {
        return await _dbContext.WebhookSubscriptions
            .FirstOrDefaultAsync(s => s.Id == id, ct);
    }

    public async Task<IReadOnlyList<WebhookSubscription>> GetByEventTypeAsync(string eventType, CancellationToken ct = default)
    {
        return await _dbContext.WebhookSubscriptions
            .Where(s => s.IsActive && (s.EventType == eventType || s.EventType == "*"))
            .ToListAsync(ct);
    }

    public async Task UpdateAsync(WebhookSubscription subscription, CancellationToken ct = default)
    {
        subscription.UpdatedAt = DateTimeOffset.UtcNow;
        _dbContext.WebhookSubscriptions.Update(subscription);
        await _dbContext.SaveChangesAsync(ct);
    }

    public async Task DeactivateAsync(Guid id, CancellationToken ct = default)
    {
        await _dbContext.WebhookSubscriptions
            .Where(s => s.Id == id)
            .ExecuteUpdateAsync(s => s
                .SetProperty(x => x.IsActive, false)
                .SetProperty(x => x.UpdatedAt, DateTimeOffset.UtcNow), ct);
    }
}
