namespace OutboxNet.Options;

public class OutboxOptions
{
    public string SchemaName { get; set; } = "outbox";
    public int BatchSize { get; set; } = 50;
    public TimeSpan DefaultVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(60);
    public string InstanceId { get; set; } = $"{Environment.MachineName}-{Guid.NewGuid():N}";
    public int MaxConcurrentDeliveries { get; set; } = 10;
    public ProcessingMode ProcessingMode { get; set; } = ProcessingMode.DirectDelivery;
}

public enum ProcessingMode
{
    DirectDelivery,
    QueueMediated
}
