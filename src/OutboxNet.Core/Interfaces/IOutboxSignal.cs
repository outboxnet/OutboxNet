namespace OutboxNet.Interfaces;

/// <summary>
/// Lightweight signal used to wake the processor immediately after a message is published,
/// eliminating the polling interval latency for the first message after an idle period.
/// </summary>
public interface IOutboxSignal
{
    /// <summary>
    /// Signals that one or more outbox messages are ready to be processed.
    /// Thread-safe; safe to call from publisher scopes concurrently.
    /// </summary>
    void Notify();

    /// <summary>
    /// Waits until a signal is received or <paramref name="timeout"/> elapses.
    /// Returns <c>true</c> if a signal was received, <c>false</c> if it timed out.
    /// </summary>
    ValueTask<bool> WaitAsync(TimeSpan timeout, CancellationToken ct);
}
