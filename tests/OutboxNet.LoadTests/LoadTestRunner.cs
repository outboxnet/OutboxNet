using System.Collections.Concurrent;
using System.Net.Http;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OutboxNet.EntityFrameworkCore;
using OutboxNet.EntityFrameworkCore.Extensions;
using OutboxNet.Extensions;
using OutboxNet.Interfaces;
using OutboxNet.Options;
using OutboxNet.Processor.Extensions;
using OutboxNet.Delivery.Extensions;

namespace OutboxNet.LoadTests;

/// <summary>
/// Orchestrates the full load test lifecycle:
///   1. Setup  — create DB schema, start embedded webhook receiver
///   2. Warmup — publish 100 messages to prime connection pools
///   3. Load   — concurrent publish phase, track publish timestamps
///   4. Drain  — wait for processor to deliver all messages
///   5. Verify — correctness check (lost, duplicates, HMAC)
///   6. Report — compute and return <see cref="LoadTestResult"/>
/// </summary>
public sealed class LoadTestRunner
{
    private readonly LoadTestConfig _config;

    // correlationId → timestamp immediately after tx.CommitAsync() returns
    private readonly ConcurrentDictionary<string, DateTimeOffset> _published = new();

    private DateTimeOffset _publishStart;
    private DateTimeOffset _publishEnd;

    public LoadTestRunner(LoadTestConfig config) => _config = config;

    // ─────────────────────────────────────────────────────────────────────────

    public async Task<LoadTestResult> RunAsync(CancellationToken ct = default)
    {
        // ── PHASE 1: Start receiver ───────────────────────────────────────────
        Console.WriteLine("[1/6] Starting webhook receiver...");
        var receiver = new WebhookReceiver(_config.WebhookSecret, _config.WebhookFailureRate, _config.VerifyHmac);
        await receiver.StartAsync(_config.ReceiverPort, ct);
        await WaitForReceiverAsync(_config.ReceiverPort, ct);
        Console.WriteLine($"      http://localhost:{_config.ReceiverPort}/webhook  (failure rate: {_config.WebhookFailureRate:P0})");

        // ── PHASE 2: Build + start OutboxNet host ─────────────────────────────
        Console.WriteLine("[2/6] Building OutboxNet processing host...");
        var outboxHost = BuildOutboxHost();

        Console.WriteLine("[3/6] Creating database schema...");
        await SetupDatabaseAsync(outboxHost, ct);

        Console.WriteLine("      Starting background processor...");
        await outboxHost.StartAsync(ct);

        try
        {
            // ── PHASE 3: Warmup ───────────────────────────────────────────────
            Console.WriteLine("[4/6] Warming up (100 messages)...");
            await PublishBatchAsync(outboxHost, 100, warmup: true, ct);
            await DrainAsync(receiver, _published.Count, TimeSpan.FromSeconds(30), label: "warmup", ct);
            var warmupDelivered = receiver.UniqueDelivered;
            _published.Clear();
            // Reset the receiver too — otherwise warmup deliveries (and any that
            // arrive *after* this point) show up as "unexpected" in the final
            // correctness check because their correlation IDs aren't in _published.
            receiver.Reset();
            Console.WriteLine($"      Warmup done — {warmupDelivered} delivered.");

            // ── PHASE 4: Load ─────────────────────────────────────────────────
            Console.WriteLine($"[5/6] Publishing {_config.TotalMessages:N0} messages" +
                              $" ({_config.PublisherConcurrency} concurrent threads)...");
            _publishStart = DateTimeOffset.UtcNow;
            await PublishBatchAsync(outboxHost, _config.TotalMessages, warmup: false, ct);
            _publishEnd = DateTimeOffset.UtcNow;

            var publishRate = _published.Count / Math.Max((_publishEnd - _publishStart).TotalSeconds, 0.001);
            Console.WriteLine($"      Published {_published.Count:N0} messages in " +
                              $"{(_publishEnd - _publishStart).TotalSeconds:F1}s  ({publishRate:F0} msg/s)");

            // ── PHASE 5: Drain ────────────────────────────────────────────────
            Console.WriteLine($"[6/6] Draining (timeout: {_config.DrainTimeout.TotalSeconds:F0}s)...");
            var drainEnd = await DrainAsync(receiver, _published.Count, _config.DrainTimeout, label: "load", ct);
            Console.WriteLine();

            // ── PHASE 6: Compute result ───────────────────────────────────────
            return ComputeResult(receiver, drainEnd);
        }
        finally
        {
            await outboxHost.StopAsync(CancellationToken.None);
            await receiver.StopAsync();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Publishing
    // ─────────────────────────────────────────────────────────────────────────

    private async Task PublishBatchAsync(IHost host, int count, bool warmup, CancellationToken ct)
    {
        await Parallel.ForEachAsync(
            Enumerable.Range(0, count),
            new ParallelOptions
            {
                MaxDegreeOfParallelism = _config.PublisherConcurrency,
                CancellationToken      = ct
            },
            async (i, token) =>
            {
                var correlationId = Guid.NewGuid().ToString("N");

                // Each publish needs its own DI scope: EfCoreOutboxPublisher and
                // OutboxDbContext are registered as Scoped.
                await using var scope     = host.Services.CreateAsyncScope();
                var db                    = scope.ServiceProvider.GetRequiredService<LoadTestDbContext>();
                var publisher             = scope.ServiceProvider.GetRequiredService<IOutboxPublisher>();

                // Begin a transaction on LoadTestDbContext so the publisher can
                // enlist OutboxDbContext in the same connection/transaction —
                // making the outbox INSERT atomic with the domain write.
                await using var tx = await db.Database.BeginTransactionAsync(token);

                await publisher.PublishAsync(
                    eventType:      "load.test.event",
                    payload:        new { Seq = i, Cid = correlationId },
                    correlationId:  correlationId,
                    cancellationToken: token);

                await tx.CommitAsync(token);

                // Record after commit: this is the "published at" timestamp
                // that defines the start of end-to-end latency measurement.
                if (!warmup)
                    _published.TryAdd(correlationId, DateTimeOffset.UtcNow);
            });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Drain
    // ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Polls until <paramref name="expectedCount"/> unique deliveries are recorded
    /// or <paramref name="timeout"/> expires. Prints a live progress line.
    /// Returns the timestamp when polling stopped.
    /// </summary>
    private static async Task<DateTimeOffset> DrainAsync(
        WebhookReceiver receiver,
        int expectedCount,
        TimeSpan timeout,
        string label,
        CancellationToken ct)
    {
        var deadline        = DateTimeOffset.UtcNow.Add(timeout);
        var lastCount       = -1;
        var lastProgressAt  = DateTimeOffset.UtcNow;

        // Tolerate slow cold-path polls: only give up if we've been idle
        // (no new deliveries) for this long *and* the wall-clock timeout expired.
        // This means a ColdPollingInterval of 3s won't cause a premature "lost" verdict.
        var idleTolerance   = TimeSpan.FromSeconds(15);

        while (receiver.UniqueDelivered < expectedCount)
        {
            var current = receiver.UniqueDelivered;
            if (current != lastCount)
            {
                lastCount      = current;
                lastProgressAt = DateTimeOffset.UtcNow;
                var pct        = expectedCount > 0 ? (double)current / expectedCount * 100 : 100;
                var remain     = (deadline - DateTimeOffset.UtcNow).TotalSeconds;
                Console.Write($"\r      [{label}] {current:N0}/{expectedCount:N0}  ({pct:F1}%)  timeout in {Math.Max(remain, 0):F0}s   ");
            }

            var now      = DateTimeOffset.UtcNow;
            var timedOut = now >= deadline;
            var idleTooLong = (now - lastProgressAt) >= idleTolerance;

            // Exit only when BOTH the wall-clock deadline has passed AND we've
            // seen no progress for idleTolerance. This gives slow pollers a
            // chance to finish what they've started.
            if (timedOut && idleTooLong) break;

            await Task.Delay(200, ct);
        }

        return DateTimeOffset.UtcNow;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Infrastructure
    // ─────────────────────────────────────────────────────────────────────────

    private IHost BuildOutboxHost()
    {
        return Host.CreateDefaultBuilder()
            .ConfigureLogging(logging =>
            {
                // Suppress chatty debug output during the load; operators can
                // pipe to a file and inspect later. Warnings/errors still show.
                logging.SetMinimumLevel(LogLevel.Warning);
            })
            .ConfigureServices((_, services) =>
            {
                // User DbContext — no entities, used only for transaction management.
                services.AddDbContext<LoadTestDbContext>(opts =>
                    opts.UseSqlServer(_config.ConnectionString));

                services
                    .AddOutboxNet(opts =>
                    {
                        opts.BatchSize                    = _config.BatchSize;
                        opts.MaxConcurrentDeliveries      = _config.MaxConcurrentDeliveries;
                        opts.DefaultVisibilityTimeout     = TimeSpan.FromMinutes(2);
                        // Ordered processing adds a NOT EXISTS subquery per row — disable
                        // for the load test so we measure raw throughput without the overhead.
                        opts.EnableOrderedProcessing      = false;
                    })
                    // EF Core stores + EfCoreOutboxPublisher<LoadTestDbContext>
                    .UseSqlServerContext<LoadTestDbContext>(_config.ConnectionString)
                    // Hot-path Channel + cold-path poll loop
                    .AddBackgroundProcessor(opts =>
                    {
                        opts.ColdPollingInterval = _config.ColdPollingInterval;
                    })
                    // HTTP delivery with HMAC-SHA256 signing
                    .AddWebhookDelivery(opts =>
                    {
                        opts.HttpTimeout         = TimeSpan.FromSeconds(10);
                        opts.Retry.MaxRetries    = 3;
                        opts.Retry.BaseDelay     = TimeSpan.FromSeconds(1);
                        opts.Retry.MaxDelay      = TimeSpan.FromSeconds(30);
                        opts.Retry.JitterFactor  = 0.1;
                    })
                    // Config-driven subscription: all messages → local receiver
                    .UseConfigWebhooks(opts =>
                    {
                        opts.Mode   = WebhookMode.Global;
                        opts.Global = new WebhookEndpointConfig
                        {
                            Url            = $"http://localhost:{_config.ReceiverPort}/webhook",
                            Secret         = _config.WebhookSecret,
                            MaxRetries     = 3,
                            TimeoutSeconds = 10,
                        };
                    });
            })
            .Build();
    }

    private async Task SetupDatabaseAsync(IHost host, CancellationToken ct)
    {
        await using var scope   = host.Services.CreateAsyncScope();
        var outboxDb            = scope.ServiceProvider.GetRequiredService<OutboxDbContext>();

        // Creates the database + outbox schema ([outbox].[OutboxMessages], etc.)
        // from the EF Core model. No-op if the schema already exists.
        await outboxDb.Database.EnsureCreatedAsync(ct);
        Console.WriteLine("      Schema ready.");

        if (_config.TruncateBeforeRun)
        {
            // Remove any leftover data from a previous run so latencies aren't
            // skewed by rows the processor hasn't caught up on.
            // Delete DeliveryAttempts first due to FK → OutboxMessages.
            var deletedAttempts  = await outboxDb.DeliveryAttempts.ExecuteDeleteAsync(ct);
            var deletedMessages  = await outboxDb.OutboxMessages.ExecuteDeleteAsync(ct);
            Console.WriteLine($"      Cleared {deletedMessages:N0} messages and {deletedAttempts:N0} attempts from previous runs.");
        }
    }

    /// <summary>
    /// Sends a GET /health to the receiver and retries until it responds, so we
    /// don't start publishing before the server is ready.
    /// </summary>
    private static async Task WaitForReceiverAsync(int port, CancellationToken ct)
    {
        using var http = new HttpClient { BaseAddress = new Uri($"http://localhost:{port}") };
        for (var attempt = 0; attempt < 20; attempt++)
        {
            try
            {
                var resp = await http.GetAsync("/health", ct);
                if (resp.IsSuccessStatusCode) return;
            }
            catch { /* not ready yet */ }
            await Task.Delay(100, ct);
        }
        throw new InvalidOperationException($"Webhook receiver on port {port} did not become ready.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Result computation
    // ─────────────────────────────────────────────────────────────────────────

    private LoadTestResult ComputeResult(WebhookReceiver receiver, DateTimeOffset drainEnd)
    {
        var publishedIds  = new HashSet<string>(_published.Keys);
        var receivedIds   = new HashSet<string>(receiver.FirstReceivedAt.Keys);

        var lostCount       = publishedIds.Except(receivedIds).Count();
        var unexpectedCount = receivedIds.Except(publishedIds).Count();

        // End-to-end latency: time from tx.CommitAsync() returning to the first
        // successful webhook receipt for the same message.
        var latencies = _published
            .Where(kvp => receiver.FirstReceivedAt.TryGetValue(kvp.Key, out _))
            .Select(kvp => (receiver.FirstReceivedAt[kvp.Key] - kvp.Value).TotalMilliseconds)
            .Where(ms => ms >= 0)   // guard: clock skew / same-millisecond deliver
            .OrderBy(ms => ms)
            .ToArray();

        var publishDuration  = (_publishEnd - _publishStart).TotalSeconds;
        var totalDuration    = (drainEnd    - _publishStart).TotalSeconds;

        // Retry deliveries = total POST calls to receiver − unique successful deliveries.
        var retryDeliveries  = (int)(receiver.TotalRequests - receiver.UniqueDelivered);

        return new LoadTestResult
        {
            Config              = _config,
            TotalPublished      = _published.Count,
            TotalDelivered      = receiver.UniqueDelivered,
            TotalRetryDeliveries= Math.Max(retryDeliveries, 0),
            PublishThroughput   = _published.Count / Math.Max(publishDuration, 0.001),
            DeliveryThroughput  = receiver.UniqueDelivered / Math.Max(totalDuration, 0.001),
            PublishDurationSec  = publishDuration,
            TotalDurationSec    = totalDuration,
            LostCount           = lostCount,
            DuplicateCount      = receiver.DuplicateDeliveries,
            UnexpectedCount     = unexpectedCount,
            HmacRejections      = receiver.HmacRejections,
            FailuresInjected    = receiver.FailuresInjected,
            LatenciesSorted     = latencies,
        };
    }
}
