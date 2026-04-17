# OutboxNet

[![CI](https://github.com/outboxnet/OutboxNet/actions/workflows/ci.yml/badge.svg)](https://github.com/outboxnet/OutboxNet/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/OutboxNet.Core.svg)](https://www.nuget.org/packages/OutboxNet.Core)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A transactional outbox library for .NET that guarantees reliable webhook delivery in distributed systems. OutboxNet ensures that when your application writes data and needs to notify external systems, either **both happen or neither does** — eliminating the class of bugs where your database commits but the notification is silently lost.

## The Problem

In distributed systems, the **dual-write problem** occurs when a service needs to update two different systems (e.g., a database and a webhook endpoint) with no built-in guarantee that both succeed or both fail.

```
1. Save order to database      ✅ succeeds
2. Send webhook to payment svc  ❌ app crashes / network timeout / partial failure
→ Order exists but payment never initiated
```

This isn't an edge case — in production systems handling thousands of requests these failures happen daily.

## The Solution: Transactional Outbox Pattern

```
1. BEGIN TRANSACTION
2.   Save order to database
3.   Write outbox message to OutboxMessages table (same DB, same transaction)
4. COMMIT TRANSACTION
5. Background processor picks up outbox messages and delivers webhooks
6. On success → mark delivered | On failure → retry with backoff | On exhaustion → dead-letter
```

By writing the outbox message in the *same database transaction* as your domain data, you get atomicity for free. The background processor guarantees **at-least-once** delivery with idempotency headers so consumers can safely deduplicate retries.

## Key Features

- **Transactional guarantee** — outbox writes participate in your existing database transaction
- **Sub-second latency** — publisher signals the processor on commit; no polling delay for the first delivery after idle
- **Duplicate-safe delivery** — deterministic `X-Outbox-Delivery-Id` per attempt; processor tracks per-subscription success to skip already-delivered subscriptions on retry
- **Multi-instance safe** — visibility-timeout locking prevents two instances processing the same message
- **Parallel delivery** — configurable concurrency at both the message and subscription level
- **HMAC-SHA256 webhook signing** — receivers can verify payload authenticity
- **Adaptive exponential backoff** — idle queue backs off automatically; saturated queue drains with zero delay
- **Dead-letter queue** — exhausted messages are preserved for manual review
- **Per-subscription settings** — independent retry limit, timeout, and custom headers per endpoint
- **Multi-tenant** — per-tenant webhook routing, per-tenant secrets, ambient `TenantId`/`UserId` from HTTP context
- **Config-driven subscriptions** — define routes in `appsettings.json` without a database table
- **Ordered processing** — partition-key ordering ensures causality within a `(TenantId, UserId, EntityId)` group
- **Observability** — built-in OpenTelemetry `ActivitySource` and `System.Diagnostics.Metrics`
- **Two SQL Server providers** — EF Core for convenience, direct ADO.NET for minimal overhead
- **Azure Functions support** — timer-trigger variant for serverless hosting

## Packages

| Package | Description |
|---|---|
| `OutboxNet.Core` | Core contracts, models, options, observability |
| `OutboxNet.EntityFrameworkCore` | EF Core + SQL Server stores and publisher |
| `OutboxNet.SqlServer` | Direct ADO.NET SQL Server stores and publisher (no EF dependency) |
| `OutboxNet.Processor` | Background hosted service for outbox processing |
| `OutboxNet.Delivery` | HTTP webhook delivery with HMAC-SHA256 signing and retry |
| `OutboxNet.AzureStorageQueue` | Azure Storage Queue publisher for queue-mediated processing |
| `OutboxNet.AzureFunctions` | Azure Functions timer trigger for serverless processing |

## Getting Started

### Step 1: Install packages

**EF Core app (most common):**
```bash
dotnet add package OutboxNet.Core
dotnet add package OutboxNet.EntityFrameworkCore
dotnet add package OutboxNet.Processor
dotnet add package OutboxNet.Delivery
```

**Direct ADO.NET / Dapper app:**
```bash
dotnet add package OutboxNet.Core
dotnet add package OutboxNet.SqlServer
dotnet add package OutboxNet.Processor
dotnet add package OutboxNet.Delivery
```

**Azure Functions (serverless):**
```bash
dotnet add package OutboxNet.Core
dotnet add package OutboxNet.EntityFrameworkCore  # or OutboxNet.SqlServer
dotnet add package OutboxNet.AzureFunctions
dotnet add package OutboxNet.Delivery
```

### Step 2: Configure services

**Option A: Entity Framework Core**

```csharp
// Program.cs
builder.Services
    .AddOutboxNet(options =>
    {
        options.SchemaName = "outbox";
        options.BatchSize = 50;
        options.DefaultVisibilityTimeout = TimeSpan.FromMinutes(5);
        options.MaxConcurrentDeliveries = 10;
        options.MaxConcurrentSubscriptionDeliveries = 4;
    })
    .UseSqlServerContext<AppDbContext>(
        builder.Configuration.GetConnectionString("Default"),
        sql => sql.MigrationsAssembly = "MyApp")
    .AddBackgroundProcessor()
    .AddWebhookDelivery();
```

**Option B: Direct SQL Server (no EF Core)**

```csharp
// Program.cs
builder.Services
    .AddOutboxNet(options =>
    {
        options.SchemaName = "outbox";
        options.BatchSize = 50;
    })
    .UseDirectSqlServer(builder.Configuration.GetConnectionString("Default"))
    .AddBackgroundProcessor()
    .AddWebhookDelivery();

// Implement and register ISqlTransactionAccessor so the publisher
// can enlist in your ADO.NET transaction.
builder.Services.AddScoped<ISqlTransactionAccessor, MySqlTransactionAccessor>();
```

**Option C: Azure Functions**

```csharp
// Program.cs (Functions host)
var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services
            .AddOutboxNet()
            .UseSqlServerContext<AppDbContext>(connectionString)
            .AddAzureFunctionsProcessor()
            .AddWebhookDelivery();
    })
    .Build();
```

Set `Outbox:TimerCron` in `local.settings.json` (or App Settings) to control the timer interval:
```json
{ "Outbox:TimerCron": "*/30 * * * * *" }
```

### Step 3: Set up the database

**EF Core — apply outbox table configurations in your DbContext:**

```csharp
public class AppDbContext : DbContext
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyOutboxConfigurations(schema: "outbox");
        // ... your own entity configurations
    }
}
```

Then generate and apply migrations:
```bash
dotnet ef migrations add AddOutbox --context AppDbContext
dotnet ef database update
```

**Direct SQL — generate the schema from a temporary EF Core migration** or write it manually. The three tables are `OutboxMessages`, `WebhookSubscriptions`, and `DeliveryAttempts`. See `OutboxNet.EntityFrameworkCore/Configurations/` for exact column definitions.

### Step 4: Register webhook subscriptions

**Option A: Database-backed (dynamic)**

Insert rows into `WebhookSubscriptions`. Key columns:

| Column | Example | Notes |
|---|---|---|
| `EventType` | `order.placed` | Routing key |
| `WebhookUrl` | `https://payment-svc/webhooks` | Target endpoint |
| `Secret` | `whsec_abc123` | Used for HMAC-SHA256 signing |
| `TenantId` | `tenant-a` or `null` | `null` = global (applies to all tenants) |
| `IsActive` | `true` | |
| `MaxRetries` | `5` | Per-subscription retry limit |
| `TimeoutSeconds` | `30` | Per-request timeout |

**Option B: Config-driven (static)**

```csharp
// Global endpoint (all tenants)
builder.Services
    .AddOutboxNet()
    .UseConfigWebhooks(builder.Configuration);
```

```json
// appsettings.json
{
  "Outbox": {
    "Webhooks": {
      "Mode": "Global",
      "Global": {
        "Url": "https://example.com/webhook",
        "Secret": "whsec_abc123",
        "MaxRetries": 5,
        "TimeoutSeconds": 30
      }
    }
  }
}
```

Or configure per-tenant routing:

```json
{
  "Outbox": {
    "Webhooks": {
      "Mode": "PerTenant",
      "Tenants": {
        "tenant-a": { "Url": "https://tenant-a.example.com/hook", "Secret": "s1" },
        "tenant-b": { "Url": "https://tenant-b.example.com/hook", "Secret": "s2" },
        "default":  { "Url": "https://fallback.example.com/hook", "Secret": "s3" }
      }
    }
  }
}
```

### Step 5: Publish outbox messages

**EF Core publisher — writes in the same transaction as your domain data:**

```csharp
public class PlaceOrderHandler
{
    private readonly AppDbContext _db;
    private readonly IOutboxPublisher _outbox;

    public PlaceOrderHandler(AppDbContext db, IOutboxPublisher outbox)
    {
        _db = db;
        _outbox = outbox;
    }

    public async Task Handle(PlaceOrderCommand cmd, CancellationToken ct)
    {
        await using var tx = await _db.Database.BeginTransactionAsync(ct);

        var order = new Order { /* ... */ };
        _db.Orders.Add(order);
        await _db.SaveChangesAsync(ct);

        // This INSERT goes into the SAME transaction — atomic with the order write.
        await _outbox.PublishAsync(
            eventType: "order.placed",
            payload: new { order.Id, order.Total, order.CustomerId },
            correlationId: cmd.CorrelationId,
            entityId: order.Id.ToString(),   // optional: used for ordered processing
            cancellationToken: ct);

        await tx.CommitAsync(ct);
        // If commit fails → both the order AND the outbox message are rolled back.
        // If commit succeeds → the background processor delivers the webhook.
        // After commit, the publisher signals the processor for near-zero latency.
    }
}
```

**Direct SQL publisher — uses `ISqlTransactionAccessor`:**

```csharp
public class MySqlTransactionAccessor : ISqlTransactionAccessor
{
    public SqlConnection Connection { get; set; } = default!;
    public SqlTransaction Transaction { get; set; } = default!;
}

public class PlaceOrderHandler
{
    private readonly IOutboxPublisher _outbox;
    private readonly MySqlTransactionAccessor _txAccessor;
    private readonly string _connectionString;

    public async Task Handle(PlaceOrderCommand cmd, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await using var tx = conn.BeginTransaction();

        await conn.ExecuteAsync("INSERT INTO Orders ...", new { /* ... */ }, tx);

        // Provide the connection/transaction before publishing.
        _txAccessor.Connection = conn;
        _txAccessor.Transaction = tx;

        await _outbox.PublishAsync(
            eventType: "order.placed",
            payload: new { cmd.OrderId, cmd.Total },
            cancellationToken: ct);

        await tx.CommitAsync(ct);
    }
}
```

### Step 6: Handle webhooks on the receiver side

Every delivery includes these headers:

| Header | Value | Purpose |
|---|---|---|
| `X-Outbox-Signature` | `sha256={hex}` | HMAC-SHA256 of the raw payload body |
| `X-Outbox-Event` | `order.placed` | Event type |
| `X-Outbox-Message-Id` | UUID | Stable across all retries of the same message |
| `X-Outbox-Delivery-Id` | UUID | Unique per attempt (deterministic — same attempt always sends the same ID) |
| `X-Outbox-Subscription-Id` | UUID | Identifies which subscription matched |
| `X-Outbox-Timestamp` | Unix seconds | Time the delivery was attempted |
| `X-Outbox-Correlation-Id` | string | Forwarded from `PublishAsync` if provided |

**Verifying the signature:**
```csharp
[HttpPost("/webhooks")]
public IActionResult Receive()
{
    using var reader = new StreamReader(Request.Body);
    var rawBody = reader.ReadToEnd();

    var expected = "sha256=" + Convert.ToHexString(
        HMACSHA256.HashData(
            Encoding.UTF8.GetBytes(webhookSecret),
            Encoding.UTF8.GetBytes(rawBody)));

    var received = Request.Headers["X-Outbox-Signature"].ToString();

    if (!CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(expected),
            Encoding.UTF8.GetBytes(received)))
        return Unauthorized();

    // Process event...
    return Ok();
}
```

**Deduplicating retries:**

Use `X-Outbox-Message-Id` + `X-Outbox-Subscription-Id` as a composite idempotency key. OutboxNet provides at-least-once delivery, so your receiver should be idempotent:

```csharp
var idempotencyKey = $"{Request.Headers["X-Outbox-Message-Id"]}:{Request.Headers["X-Outbox-Subscription-Id"]}";

if (await _cache.ExistsAsync(idempotencyKey))
    return Ok(); // already processed

await ProcessEventAsync(payload);
await _cache.SetAsync(idempotencyKey, true, TimeSpan.FromDays(7));
return Ok();
```

## How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Your Application                             │
│                                                                     │
│  ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │ Domain Logic  │───>│ IOutboxPublisher  │───>│   SQL Server DB  │  │
│  │ (e.g. Order)  │    │ (same transaction)│    │  ┌────────────┐ │  │
│  └──────────────┘    └────────┬─────────┘    │  │  Orders     │ │  │
│                               │ Notify()      │  │  OutboxMsgs │ │  │
│                               ▼              │  └────────────┘ │  │
│                        IOutboxSignal          └────────┬────────┘  │
│                               │                        │           │
│  ┌────────────────────────────▼───────────────────────┐│           │
│  │           OutboxProcessorService                    ││           │
│  │  WaitAsync(signal or timeout)                       ││           │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────┐  ││           │
│  │  │ LockNextBatch │─>│ Deliver (par)│─>│ Bookkeep │  ││           │
│  │  │ per batch     │  │ per sub (par)│  │ + Decide │  ││           │
│  │  └──────────────┘  └──────────────┘  └──────────┘  ││           │
│  └─────────────────────────────────────────────────────┘│           │
│                                                          │           │
└──────────────────────────────────────────────────────────┘           │
                               │
                    ┌──────────▼──────────┐
                    │  External Webhooks   │
                    │  • Payment Service   │
                    │  • Inventory Service │
                    │  • Analytics         │
                    └─────────────────────┘
```

### Processing pipeline (per message)

1. **Lock** — `LockNextBatchAsync` claims messages atomically with a visibility timeout
2. **Subscription pre-fetch** — one query per unique `(EventType, TenantId)` pair in the batch (not per message)
3. **Delivery state pre-fetch** — `GetDeliveryStatesAsync` loads `(attemptCount, hasSuccess)` per subscription in one query; already-succeeded subscriptions are skipped
4. **Parallel delivery** — subscriptions are delivered concurrently up to `MaxConcurrentSubscriptionDeliveries`; each attempt gets a deterministic `deliveryId` derived from `SHA256(MessageId + SubscriptionId + AttemptNumber)`
5. **Batch bookkeeping** — all `DeliveryAttempt` records saved in a single `SaveAttemptsAsync` call; if save fails after a successful delivery, the processor logs CRITICAL and lets the lock expire (does NOT immediately retry to avoid duplicate delivery)
6. **Decision** — all succeeded → `MarkAsProcessed`; any failed → `IncrementRetry` with backoff; all exhausted without success → `MarkAsDeadLettered`

## Configuration Reference

### `AddOutboxNet(options => ...)` — core options

```csharp
builder.Services.AddOutboxNet(options =>
{
    // Database schema for outbox tables. Default: "outbox"
    options.SchemaName = "outbox";

    // Messages locked per processing cycle. Default: 50
    options.BatchSize = 50;

    // How long a message is locked before being eligible for re-processing.
    // Must exceed worst-case time to deliver one full batch.
    // Default: 5 minutes
    options.DefaultVisibilityTimeout = TimeSpan.FromMinutes(5);

    // Unique identifier for this processor instance (for lock ownership).
    // Default: "{MachineName}-{Guid}" — auto-generated, usually leave as default.
    options.InstanceId = "my-instance-1";

    // Max messages processed concurrently within a batch. Default: 10
    options.MaxConcurrentDeliveries = 10;

    // Max subscriptions delivered concurrently per message. Default: 4
    // Fanout = BatchSize × MaxConcurrentDeliveries × MaxConcurrentSubscriptionDeliveries
    // Keep fanout under ~200 to avoid connection pool exhaustion.
    options.MaxConcurrentSubscriptionDeliveries = 4;

    // DirectDelivery (default): processor calls webhook directly.
    // QueueMediated: processor publishes to IMessagePublisher (e.g. Azure Storage Queue).
    options.ProcessingMode = ProcessingMode.DirectDelivery;

    // Enforce causal ordering within a (TenantId, UserId, EntityId) partition.
    // Default: true — a partition's messages are processed in creation order.
    options.EnableOrderedProcessing = true;

    // Optional: only process messages for this tenant (for sharded multi-instance deployments).
    // Default: null (process all tenants).
    options.TenantFilter = "tenant-a";
});
```

### `.AddBackgroundProcessor(options => ...)` — polling behavior

```csharp
.AddBackgroundProcessor(options =>
{
    // Minimum interval between polling cycles (also used as first backoff step).
    // Default: 1 second
    options.PollingInterval = TimeSpan.FromSeconds(1);

    // Back off exponentially when queue is idle; process immediately when saturated.
    // Default: true
    options.AdaptivePolling = true;

    // Maximum backoff cap when idle. Default: 60 seconds
    options.MaxPollingInterval = TimeSpan.FromSeconds(60);

    // Exponential multiplier per empty batch. Default: 1.5
    options.IdleBackoffFactor = 1.5;
});
```

### `.AddWebhookDelivery(options => ...)` — HTTP delivery and retry

```csharp
.AddWebhookDelivery(options =>
{
    // Global HTTP client timeout. Default: 30 seconds
    options.HttpTimeout = TimeSpan.FromSeconds(30);

    // Retry policy (applied to the global retry counter on OutboxMessage,
    // separate from per-subscription MaxRetries).
    options.Retry.MaxRetries = 5;
    options.Retry.BaseDelay = TimeSpan.FromSeconds(5);
    options.Retry.MaxDelay = TimeSpan.FromMinutes(5);
    options.Retry.JitterFactor = 0.2; // ±20% jitter
});
```

### `.UseHttpContextAccessor(options => ...)` — ambient tenant/user context

Extracts `TenantId` and `UserId` from the current HTTP request's claims and makes them available to the publisher. Required for automatic per-tenant partitioning and routing.

```csharp
.UseHttpContextAccessor(options =>
{
    options.TenantIdClaimType = "tid";   // claim type for TenantId
    options.UserIdClaimType   = "sub";   // claim type for UserId
});
```

### `.UseTenantSecretRetriever(options => ...)` — per-tenant HMAC secrets

Resolves per-tenant webhook secrets from `IConfiguration` at delivery time. Because `IConfiguration` is provider-agnostic, this works transparently with Azure Key Vault, AWS Secrets Manager, environment variables, or `appsettings.json`.

```csharp
.UseTenantSecretRetriever(options =>
{
    // Key pattern for IConfiguration lookup. {tenantId} is replaced at runtime.
    // When Azure Key Vault is configured, Key Vault secrets are auto-resolved.
    // Default: "Outbox:Secrets:{tenantId}:WebhookSecret"
    options.KeyPattern = "Outbox:Secrets:{tenantId}:WebhookSecret";

    // Cache duration for resolved secrets. TimeSpan.Zero disables caching.
    // Default: 5 minutes
    options.SecretCacheTtl = TimeSpan.FromMinutes(5);
});
```

Or plug in a custom retriever:

```csharp
.UseTenantSecretRetriever<MyVaultSecretRetriever>();
```

## Advanced Usage

### Multi-tenant setup

```csharp
builder.Services
    .AddOutboxNet()
    .UseSqlServerContext<AppDbContext>(connectionString)
    .UseHttpContextAccessor(opts =>
    {
        opts.TenantIdClaimType = "tid";
        opts.UserIdClaimType   = "sub";
    })
    .UseTenantSecretRetriever(opts =>
    {
        opts.KeyPattern = "Outbox:Secrets:{tenantId}:WebhookSecret";
    })
    .AddBackgroundProcessor()
    .AddWebhookDelivery();
```

With `UseHttpContextAccessor`, every call to `PublishAsync` automatically stamps `TenantId` and `UserId` onto the outbox message from the current HTTP request's claims. The processor then routes each message to the correct per-tenant subscription and signs with the per-tenant secret.

### Publishing with partition key for ordered processing

```csharp
await _outbox.PublishAsync(
    eventType: "order.updated",
    payload: new { orderId, status },
    entityId: orderId.ToString(),  // all events for the same order processed in order
    cancellationToken: ct);
```

When `EnableOrderedProcessing = true` (default), messages with the same `(TenantId, UserId, EntityId)` are processed in creation order using a `NOT EXISTS` SQL guard in `LockNextBatchAsync`.

### Custom subscription reader

Implement `ISubscriptionReader` to route messages from any source (database, service registry, feature flag, etc.):

```csharp
public class MyCustomSubscriptionReader : ISubscriptionReader
{
    public Task<IReadOnlyList<WebhookSubscription>> GetForMessageAsync(
        OutboxMessage message, CancellationToken ct)
    {
        // Return subscriptions for this message's event type / tenant.
    }
}

builder.Services.AddSingleton<ISubscriptionReader, MyCustomSubscriptionReader>();
```

### Custom retry policy

```csharp
public class LinearRetryPolicy : IRetryPolicy
{
    public TimeSpan? GetNextDelay(int retryCount) =>
        retryCount < 10 ? TimeSpan.FromSeconds(30) : null; // null = dead-letter
}

builder.Services.AddSingleton<IRetryPolicy, LinearRetryPolicy>();
```

### Azure Storage Queue (queue-mediated mode)

```csharp
builder.Services
    .AddOutboxNet(opts => opts.ProcessingMode = ProcessingMode.QueueMediated)
    .UseSqlServerContext<AppDbContext>(connectionString)
    .UseAzureStorageQueue(opts =>
    {
        opts.ConnectionString = storageConnectionString;
        opts.QueueName = "outbox-messages";
    })
    .AddBackgroundProcessor()
    .AddWebhookDelivery();
```

In queue-mediated mode the processor publishes locked messages to Azure Storage Queue rather than delivering webhooks directly. A separate consumer (e.g., another Azure Functions instance) reads from the queue and handles delivery.

## Observability

OutboxNet emits OpenTelemetry signals out of the box.

### Traces

Register the activity source in your telemetry setup:
```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource("OutboxNet"));
```

Activities emitted: `outbox.publish`, `outbox.process_batch`, `outbox.deliver_webhook`.

### Metrics

Register the meter:
```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("OutboxNet"));
```

| Metric | Type | Tags |
|---|---|---|
| `outbox.messages.published` | Counter | `event_type` |
| `outbox.messages.processed` | Counter | `event_type` |
| `outbox.messages.failed` | Counter | `event_type` |
| `outbox.messages.dead_lettered` | Counter | `event_type` |
| `outbox.delivery.attempts` | Counter | `event_type` |
| `outbox.delivery.successes` | Counter | `event_type` |
| `outbox.delivery.failures` | Counter | `event_type` |
| `outbox.delivery.duration_ms` | Histogram | `event_type` |
| `outbox.batches.processed` | Counter | — |
| `outbox.batch.size` | Histogram | — |
| `outbox.processing.duration_ms` | Histogram | — |

## Which SQL Server Package?

| If you... | Use |
|---|---|
| Already use EF Core and want migrations + DbContext integration | `OutboxNet.EntityFrameworkCore` |
| Use Dapper, raw ADO.NET, or want zero EF Core overhead | `OutboxNet.SqlServer` |
| Need outbox writes in the same transaction as your EF Core `SaveChangesAsync` | `OutboxNet.EntityFrameworkCore` |
| Need outbox writes in the same transaction as a raw `SqlTransaction` | `OutboxNet.SqlServer` |

## Project Structure

```
OutboxNet/
├── src/
│   ├── OutboxNet.Core/                    # Contracts, models, options, observability
│   ├── OutboxNet.EntityFrameworkCore/     # EF Core + SQL Server stores & publisher
│   ├── OutboxNet.SqlServer/               # Direct ADO.NET SQL Server stores & publisher
│   ├── OutboxNet.Processor/               # Background processing hosted service
│   ├── OutboxNet.Delivery/                # HTTP webhook delivery + HMAC + retry
│   ├── OutboxNet.AzureStorageQueue/       # Azure Storage Queue transport
│   └── OutboxNet.AzureFunctions/          # Azure Functions timer trigger
├── tests/
│   ├── OutboxNet.Core.Tests/
│   ├── OutboxNet.Delivery.Tests/
│   └── OutboxNet.Processor.Tests/
├── OutboxNet.SampleApp/                   # Full ASP.NET Core sample application
├── Directory.Build.props                  # Shared build + NuGet package properties
├── Directory.Packages.props               # Centralized package version management
└── .github/workflows/
    ├── ci.yml                             # Build + test on every push/PR
    └── publish.yml                        # Publish to NuGet on GitHub release
```

## Publishing to NuGet

### Automated (GitHub Actions)

1. Add your NuGet API key as a repository secret named `NUGET_API_KEY` (Settings → Secrets → Actions)
2. Create a GitHub release with a version tag (e.g. `1.0.0` or `1.2.0-preview.1`)
3. The workflow builds, tests, packs all packages with the release tag as the version, and pushes to nuget.org

### Manual

```bash
dotnet pack -c Release -o ./nupkgs /p:Version=1.0.0
dotnet nuget push ./nupkgs/*.nupkg \
    --api-key YOUR_API_KEY \
    --source https://api.nuget.org/v3/index.json \
    --skip-duplicate
```

The version is controlled by `<Version>` in `Directory.Build.props`. All packages share the same version.

## License

[MIT](LICENSE)
