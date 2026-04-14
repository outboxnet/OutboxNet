namespace OutboxNet.Interfaces;

public interface IOutboxProcessor
{
    /// <summary>
    /// Processes the next batch of outbox messages. Returns the number of messages
    /// that were picked up for processing (0 means the queue was idle).
    /// </summary>
    Task<int> ProcessBatchAsync(CancellationToken ct = default);
}
