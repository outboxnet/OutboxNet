namespace OutboxNet.Options;

public class ProcessorOptions
{
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(10);
}
