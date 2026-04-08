using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OutboxNet.Interfaces;
using OutboxNet.Models;
using OutboxNet.Options;

namespace OutboxNet.EntityFrameworkCore.Stores;

internal sealed class EfCoreOutboxStore : IOutboxStore
{
    private readonly OutboxDbContext _dbContext;
    private readonly OutboxOptions _options;
    private readonly ILogger<EfCoreOutboxStore> _logger;

    public EfCoreOutboxStore(
        OutboxDbContext dbContext,
        IOptions<OutboxOptions> options,
        ILogger<EfCoreOutboxStore> logger)
    {
        _dbContext = dbContext;
        _options = options.Value;
        _logger = logger;
    }

    public async Task SaveMessageAsync(OutboxMessage message, CancellationToken ct = default)
    {
        _dbContext.OutboxMessages.Add(message);
        await _dbContext.SaveChangesAsync(ct);
    }

    public async Task<IReadOnlyList<OutboxMessage>> LockNextBatchAsync(
        int batchSize,
        TimeSpan visibilityTimeout,
        string lockedBy,
        CancellationToken ct = default)
    {
        var lockedUntil = DateTimeOffset.UtcNow.Add(visibilityTimeout);
        var now = DateTimeOffset.UtcNow;

        var updated = await _dbContext.OutboxMessages
            .Where(m => (m.Status == MessageStatus.Pending || m.Status == MessageStatus.Processing)
                     && (m.LockedUntil == null || m.LockedUntil < now)
                     && (m.NextRetryAt == null || m.NextRetryAt <= now))
            .OrderBy(m => m.CreatedAt)
            .Take(batchSize)
            .ExecuteUpdateAsync(s => s
                .SetProperty(m => m.Status, MessageStatus.Processing)
                .SetProperty(m => m.LockedUntil, lockedUntil)
                .SetProperty(m => m.LockedBy, lockedBy), ct);

        if (updated == 0)
            return [];

        var messages = await _dbContext.OutboxMessages
            .Where(m => m.LockedBy == lockedBy && m.LockedUntil == lockedUntil)
            .AsNoTracking()
            .ToListAsync(ct);

        _logger.LogDebug("Locked {Count} outbox messages for processing by {LockedBy}", messages.Count, lockedBy);
        return messages;
    }

    public async Task<bool> MarkAsProcessedAsync(Guid messageId, string lockedBy, CancellationToken ct = default)
    {
        var affected = await _dbContext.OutboxMessages
            .Where(m => m.Id == messageId && m.LockedBy == lockedBy)
            .ExecuteUpdateAsync(s => s
                .SetProperty(m => m.Status, MessageStatus.Delivered)
                .SetProperty(m => m.ProcessedAt, DateTimeOffset.UtcNow)
                .SetProperty(m => m.LockedUntil, (DateTimeOffset?)null)
                .SetProperty(m => m.LockedBy, (string?)null), ct);

        return affected > 0;
    }

    public async Task<bool> MarkAsFailedAsync(Guid messageId, string lockedBy, string error, CancellationToken ct = default)
    {
        var affected = await _dbContext.OutboxMessages
            .Where(m => m.Id == messageId && m.LockedBy == lockedBy)
            .ExecuteUpdateAsync(s => s
                .SetProperty(m => m.Status, MessageStatus.Failed)
                .SetProperty(m => m.LastError, error)
                .SetProperty(m => m.LockedUntil, (DateTimeOffset?)null)
                .SetProperty(m => m.LockedBy, (string?)null), ct);

        return affected > 0;
    }

    public async Task<bool> IncrementRetryAsync(Guid messageId, string lockedBy, DateTimeOffset nextRetryAt, string? error = null, CancellationToken ct = default)
    {
        var affected = await _dbContext.OutboxMessages
            .Where(m => m.Id == messageId && m.LockedBy == lockedBy)
            .ExecuteUpdateAsync(s => s
                .SetProperty(m => m.Status, MessageStatus.Pending)
                .SetProperty(m => m.RetryCount, m => m.RetryCount + 1)
                .SetProperty(m => m.NextRetryAt, nextRetryAt)
                .SetProperty(m => m.LastError, error)
                .SetProperty(m => m.LockedUntil, (DateTimeOffset?)null)
                .SetProperty(m => m.LockedBy, (string?)null), ct);

        return affected > 0;
    }

    public async Task<bool> MarkAsDeadLetteredAsync(Guid messageId, string lockedBy, CancellationToken ct = default)
    {
        var affected = await _dbContext.OutboxMessages
            .Where(m => m.Id == messageId && m.LockedBy == lockedBy)
            .ExecuteUpdateAsync(s => s
                .SetProperty(m => m.Status, MessageStatus.DeadLettered)
                .SetProperty(m => m.LockedUntil, (DateTimeOffset?)null)
                .SetProperty(m => m.LockedBy, (string?)null), ct);

        return affected > 0;
    }

    public async Task<bool> IsLockHeldAsync(Guid messageId, string lockedBy, CancellationToken ct = default)
    {
        return await _dbContext.OutboxMessages
            .AnyAsync(m => m.Id == messageId
                        && m.LockedBy == lockedBy
                        && m.Status == MessageStatus.Processing
                        && m.LockedUntil != null
                        && m.LockedUntil > DateTimeOffset.UtcNow, ct);
    }

    public async Task ReleaseExpiredLocksAsync(CancellationToken ct = default)
    {
        var released = await _dbContext.OutboxMessages
            .Where(m => m.Status == MessageStatus.Processing
                     && m.LockedUntil != null
                     && m.LockedUntil < DateTimeOffset.UtcNow)
            .ExecuteUpdateAsync(s => s
                .SetProperty(m => m.Status, MessageStatus.Pending)
                .SetProperty(m => m.LockedUntil, (DateTimeOffset?)null)
                .SetProperty(m => m.LockedBy, (string?)null), ct);

        if (released > 0)
            _logger.LogWarning("Released {Count} expired message locks", released);
    }
}
