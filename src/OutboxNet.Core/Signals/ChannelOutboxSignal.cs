using System.Threading.Channels;
using OutboxNet.Interfaces;

namespace OutboxNet.Signals;

/// <summary>
/// Bounded channel with capacity=1 and DropWrite overflow mode.
/// Any number of concurrent <c>Notify()</c> calls collapse into a single pending signal —
/// the processor wakes once and drains whatever is in the queue, without blocking publishers.
/// </summary>
internal sealed class ChannelOutboxSignal : IOutboxSignal
{
    // Capacity 1: if a signal is already pending, additional Notify() calls are silently
    // dropped. The processor will drain everything in the next batch anyway.
    private readonly Channel<bool> _channel = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(capacity: 1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleWriter = false,
            SingleReader = true
        });

    public void Notify() => _channel.Writer.TryWrite(true);

    public async ValueTask<bool> WaitAsync(TimeSpan timeout, CancellationToken ct)
    {
        if (timeout <= TimeSpan.Zero)
            return _channel.Reader.TryRead(out _);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try
        {
            await _channel.Reader.WaitToReadAsync(cts.Token).ConfigureAwait(false);
            _channel.Reader.TryRead(out _); // consume the token
            return true;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Timed out (not externally cancelled) — return false so caller polls normally.
            return false;
        }
    }
}
