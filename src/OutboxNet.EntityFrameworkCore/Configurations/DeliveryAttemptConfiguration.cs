using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using OutboxNet.Models;

namespace OutboxNet.EntityFrameworkCore.Configurations;

public class DeliveryAttemptConfiguration : IEntityTypeConfiguration<DeliveryAttempt>
{
    public void Configure(EntityTypeBuilder<DeliveryAttempt> builder)
    {
        builder.ToTable("DeliveryAttempts");

        builder.HasKey(d => d.Id);
        builder.Property(d => d.Id).HasDefaultValueSql("NEWSEQUENTIALID()");

        builder.Property(d => d.OutboxMessageId).IsRequired();
        builder.Property(d => d.WebhookSubscriptionId).IsRequired();
        builder.Property(d => d.AttemptNumber).IsRequired();
        builder.Property(d => d.Status).IsRequired().HasDefaultValue(DeliveryStatus.Pending);
        builder.Property(d => d.ResponseBody).HasMaxLength(4000);
        builder.Property(d => d.DurationMs).IsRequired().HasDefaultValue(0L);
        builder.Property(d => d.AttemptedAt).IsRequired().HasDefaultValueSql("SYSDATETIMEOFFSET()").HasPrecision(3);
        builder.Property(d => d.NextRetryAt).HasPrecision(3);

        builder.HasOne(d => d.OutboxMessage)
            .WithMany()
            .HasForeignKey(d => d.OutboxMessageId)
            .OnDelete(DeleteBehavior.Cascade);

        builder.HasOne(d => d.WebhookSubscription)
            .WithMany()
            .HasForeignKey(d => d.WebhookSubscriptionId)
            .OnDelete(DeleteBehavior.Cascade);

        builder.HasIndex(d => d.OutboxMessageId)
            .HasDatabaseName("IX_DeliveryAttempts_MessageId");

        builder.HasIndex(d => new { d.WebhookSubscriptionId, d.Status })
            .HasDatabaseName("IX_DeliveryAttempts_SubscriptionId_Status");
    }
}
