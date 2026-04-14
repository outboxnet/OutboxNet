using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using OutboxNet.Models;

namespace OutboxNet.EntityFrameworkCore.Configurations;

public class OutboxMessageConfiguration : IEntityTypeConfiguration<OutboxMessage>
{
    public void Configure(EntityTypeBuilder<OutboxMessage> builder)
    {
        builder.ToTable("OutboxMessages");

        builder.HasKey(m => m.Id);
        builder.Property(m => m.Id).HasDefaultValueSql("NEWSEQUENTIALID()");

        builder.Property(m => m.EventType).IsRequired().HasMaxLength(256);
        builder.Property(m => m.Payload).IsRequired();
        builder.Property(m => m.CorrelationId).HasMaxLength(128);
        builder.Property(m => m.TraceId).HasMaxLength(128);
        builder.Property(m => m.Status).IsRequired();
        builder.Property(m => m.RetryCount).IsRequired().HasDefaultValue(0);
        builder.Property(m => m.CreatedAt).IsRequired().HasDefaultValueSql("SYSDATETIMEOFFSET()").HasPrecision(3);
        builder.Property(m => m.ProcessedAt).HasPrecision(3);
        builder.Property(m => m.LockedUntil).HasPrecision(3);
        builder.Property(m => m.LockedBy).HasMaxLength(256);
        builder.Property(m => m.NextRetryAt).HasPrecision(3);

        builder.Property(m => m.Headers).HasConversion(
            v => v == null ? null : JsonSerializer.Serialize(v, (JsonSerializerOptions?)null),
            v => v == null ? null : JsonSerializer.Deserialize<Dictionary<string, string>>(v, (JsonSerializerOptions?)null));

        builder.Property(m => m.TenantId).HasMaxLength(256);
        builder.Property(m => m.UserId).HasMaxLength(256);
        builder.Property(m => m.EntityId).HasMaxLength(256);

        builder.HasIndex(m => new { m.Status, m.NextRetryAt })
            .HasDatabaseName("IX_OutboxMessages_Status_NextRetryAt");

        // Supports ReleaseExpiredLocksAsync: WHERE Status = Processing AND LockedUntil < now
        builder.HasIndex(m => new { m.Status, m.LockedUntil })
            .HasDatabaseName("IX_OutboxMessages_Status_LockedUntil")
            .HasFilter("[LockedUntil] IS NOT NULL");

        builder.HasIndex(m => m.CreatedAt)
            .HasDatabaseName("IX_OutboxMessages_CreatedAt");

        builder.HasIndex(m => m.EventType)
            .HasDatabaseName("IX_OutboxMessages_EventType");

        // Partial index for ordered-processing lookups — only covers rows that have at least one key set.
        builder.HasIndex(m => new { m.TenantId, m.UserId, m.EntityId })
            .HasDatabaseName("IX_OutboxMessages_PartitionKey")
            .HasFilter("[TenantId] IS NOT NULL OR [UserId] IS NOT NULL OR [EntityId] IS NOT NULL");
    }
}
