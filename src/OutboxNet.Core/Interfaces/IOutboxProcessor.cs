namespace OutboxNet.Interfaces;

public interface IOutboxProcessor
{
    Task ProcessBatchAsync(CancellationToken ct = default);
}
