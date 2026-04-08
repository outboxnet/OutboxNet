namespace OutboxNet.Interfaces;

public interface IOutboxPublisher
{
    Task PublishAsync(
        string eventType,
        object payload,
        string? correlationId = null,
        Dictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default);
}
