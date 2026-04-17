namespace OutboxNet.Options;

public class ProcessorOptions
{
    /// <summary>
    /// Minimum polling interval used after a partial (non-saturating) batch.
    /// When the queue is saturated (full batch returned) the processor loops with
    /// zero delay. Default: 1 second.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

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
