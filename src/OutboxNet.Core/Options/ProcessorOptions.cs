namespace OutboxNet.Options;

public class ProcessorOptions
{
    /// <summary>
    /// Fixed polling interval. When <see cref="AdaptivePolling"/> is enabled this becomes
    /// the minimum (reset-to) interval used immediately after a non-empty batch.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// When true, the processor backs off exponentially during idle periods and resets to
    /// <see cref="PollingInterval"/> as soon as a non-empty batch is found.
    /// </summary>
    public bool AdaptivePolling { get; set; } = true;

    /// <summary>Maximum back-off cap when no messages are found.</summary>
    public TimeSpan MaxPollingInterval { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Multiplier applied to the current interval after each empty batch.</summary>
    public double IdleBackoffFactor { get; set; } = 1.5;
}
