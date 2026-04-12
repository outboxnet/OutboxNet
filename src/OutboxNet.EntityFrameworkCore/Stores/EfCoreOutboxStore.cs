using Microsoft.Data.SqlClient;
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
        var schema = _options.SchemaName;
        var lockedUntil = DateTimeOffset.UtcNow.Add(visibilityTimeout);

        var sql = $"""
            UPDATE TOP (@batchSize) m
            SET
                m.[Status] = @processingStatus,
                m.[LockedUntil] = @lockedUntil,
                m.[LockedBy] = @lockedBy
            OUTPUT
                INSERTED.[Id],
                INSERTED.[EventType],
                INSERTED.[Payload],
                INSERTED.[CorrelationId],
                INSERTED.[TraceId],
                INSERTED.[Status],
                INSERTED.[RetryCount],
                INSERTED.[CreatedAt],
                INSERTED.[ProcessedAt],
                INSERTED.[LockedUntil],
                INSERTED.[LockedBy],
                INSERTED.[NextRetryAt],
                INSERTED.[LastError],
                INSERTED.[Headers]
            FROM [{schema}].[OutboxMessages] m WITH (UPDLOCK, READPAST)
            WHERE m.[Status] IN (@pendingStatus, @processingStatus)
              AND (m.[LockedUntil] IS NULL OR m.[LockedUntil] < SYSDATETIMEOFFSET())
              AND (m.[NextRetryAt] IS NULL OR m.[NextRetryAt] <= SYSDATETIMEOFFSET())
            ORDER BY m.[CreatedAt]
            """;

        var messages = await _dbContext.OutboxMessages
            .FromSqlRaw(
                sql,
                new SqlParameter("@batchSize", batchSize),
                new SqlParameter("@processingStatus", (int)MessageStatus.Processing),
                new SqlParameter("@pendingStatus", (int)MessageStatus.Pending),
                new SqlParameter("@lockedUntil", lockedUntil),
                new SqlParameter("@lockedBy", lockedBy))
            .AsNoTracking()
            .ToListAsync(ct);

        _logger.LogDebug(
            "Locked {Count} outbox messages for processing by {LockedBy}",
            messages.Count,
            lockedBy);

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
