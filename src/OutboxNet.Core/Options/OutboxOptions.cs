namespace OutboxNet.Options;

public class OutboxOptions
{
    public string SchemaName { get; set; } = "outbox";
    public int BatchSize { get; set; } = 50;
    public TimeSpan DefaultVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(60);
    public string InstanceId { get; set; } = $"{Environment.MachineName}-{Guid.NewGuid():N}";
    public int MaxConcurrentDeliveries { get; set; } = 10;
    public ProcessingMode ProcessingMode { get; set; } = ProcessingMode.DirectDelivery;

    /// <summary>
    /// When true (default), messages that share the same (TenantId, UserId, EntityId) partition key
    /// are processed strictly in order — a message for a given partition is not picked up until
    /// any previously locked message in that partition has finished. Messages with no partition
    /// keys set are unaffected and retain concurrent processing behaviour.
    /// Set to false to disable the ordering guarantee globally.
    /// </summary>
    public bool EnableOrderedProcessing { get; set; } = true;

    /// <summary>
    /// When set, only messages whose <c>TenantId</c> matches this value are picked up by
    /// <c>LockNextBatchAsync</c>. Use this to shard processing across multiple processor
    /// instances — each instance handles a dedicated tenant (or set of tenants).
    /// <c>null</c> (default) means all tenants are processed by this instance.
    /// </summary>
    public string? TenantFilter { get; set; }
}

public enum ProcessingMode
{
    DirectDelivery,
    QueueMediated
}
