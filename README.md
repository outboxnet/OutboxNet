# OutboxNet

[![CI](https://github.com/undefined-exception/OutboxNet/actions/workflows/ci.yml/badge.svg)](https://github.com/undefined-exception/OutboxNet/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/OutboxNet.Core.svg)](https://www.nuget.org/packages/OutboxNet.Core)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A transactional outbox library for .NET that guarantees reliable webhook delivery in distributed systems. OutboxNet ensures that when your application writes data and needs to notify external systems, either **both happen or neither does** — eliminating the class of bugs where your database commits but the notification is silently lost.

## The Problem

In distributed systems, the **dual-write problem** occurs when a service needs to update two different systems (e.g., a database and a webhook endpoint) and there is no built-in way to guarantee both succeed or both fail.

```
1. Save order to database      ✅ succeeds
2. Send webhook to payment svc  ❌ app crashes / network timeout / partial failure
→ Order exists but payment never initiated
```

This isn't an edge case — in production systems handling thousands of requests, these failures happen daily.

## The Solution: Transactional Outbox Pattern

```
1. BEGIN TRANSACTION
2.   Save order to database
3.   Write outbox message to OutboxMessages table (same DB, same transaction)
4. COMMIT TRANSACTION
5. Background processor picks up outbox messages and delivers webhooks
6. On success → mark delivered | On failure → retry with backoff | On exhaustion → dead-letter
```

By writing the outbox message in the *same database transaction* as your domain data, you get atomicity for free.

## Packages

| Package | Description |
|---|---|
| [`OutboxNet.Core`](https://www.nuget.org/packages/OutboxNet.Core) | Core contracts, models, options, observability |
| [`OutboxNet.EntityFrameworkCore`](https://www.nuget.org/packages/OutboxNet.EntityFrameworkCore) | EF Core + SQL Server stores and publisher |
| [`OutboxNet.SqlServer`](https://www.nuget.org/packages/OutboxNet.SqlServer) | Direct ADO.NET SQL Server stores and publisher (no EF dependency) |
| [`OutboxNet.Processor`](https://www.nuget.org/packages/OutboxNet.Processor) | Background hosted service for outbox processing |
| [`OutboxNet.Delivery`](https://www.nuget.org/packages/OutboxNet.Delivery) | HTTP webhook delivery with HMAC-SHA256 signing and retry |
| [`OutboxNet.AzureStorageQueue`](https://www.nuget.org/packages/OutboxNet.AzureStorageQueue) | Azure Storage Queue publisher for queue-mediated processing |
| [`OutboxNet.AzureFunctions`](https://www.nuget.org/packages/OutboxNet.AzureFunctions) | Azure Functions timer trigger for serverless processing |

## Getting Started

### Step 1: Install packages

Choose your persistence provider and install the required packages.

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
var connectionString = builder.Configuration.GetConnectionString("Default");

builder.Services.AddOutboxNet(options =>
{
    options.SchemaName = "outbox";
    options.BatchSize = 50;
    options.DefaultVisibilityTimeout = TimeSpan.FromSeconds(60);
    options.MaxConcurrentDeliveries = 10;
})
.UseSqlServer<AppDbContext>(connectionString, sql =>
{
    sql.MigrationsAssembly = "MyApp.Migrations";
})
.AddProcessor()
.AddWebhookDelivery();
```

**Option B: Direct SQL Server**

```csharp
// Program.cs
var connectionString = builder.Configuration.GetConnectionString("Default");

builder.Services.AddOutboxNet(options =>
{
    options.SchemaName = "outbox";
    options.BatchSize = 50;
})
.UseDirectSqlServer(connectionString)
.AddProcessor()
.AddWebhookDelivery();

// Register your transaction accessor for the publisher
builder.Services.AddScoped<ISqlTransactionAccessor, MySqlTransactionAccessor>();
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

**Direct SQL — create the tables manually:**

OutboxNet uses three tables: `OutboxMessages`, `WebhookSubscriptions`, and `DeliveryAttempts`. See the EF Core entity configurations in `OutboxNet.EntityFrameworkCore/Configurations/` for the exact column definitions, or generate the SQL from a temporary EF Core migration.

### Step 4: Register webhook subscriptions

Insert subscription rows into the `WebhookSubscriptions` table for each event type and endpoint:

| Column | Example |
|---|---|
| `EventType` | `order.placed` |
| `WebhookUrl` | `https://payment-svc.internal/webhooks` |
| `Secret` | `whsec_abc123...` (used for HMAC signing) |
| `IsActive` | `true` |

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
        await using var transaction = await _db.Database.BeginTransactionAsync(ct);

        var order = new Order { /* ... */ };
        _db.Orders.Add(order);
        await _db.SaveChangesAsync(ct);

        // This INSERT goes into the SAME transaction
        await _outbox.PublishAsync(
            eventType: "order.placed",
            payload: new { order.Id, order.Total, order.CustomerId },
            correlationId: cmd.CorrelationId,
            cancellationToken: ct);

        await transaction.CommitAsync(ct);
        // If commit fails → both order AND outbox message are rolled back
        // If commit succeeds → background processor delivers the webhook
    }
}
```

**Direct SQL publisher — uses ISqlTransactionAccessor:**

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
        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);
        await using var transaction = connection.BeginTransaction();

        // Your domain write (Dapper, raw ADO.NET, etc.)
        await connection.ExecuteAsync(
            "INSERT INTO Orders ...",
            new { /* ... */ },
            transaction);

        // Provide connection/transaction to the outbox publisher
        _txAccessor.Connection = connection;
        _txAccessor.Transaction = transaction;

        await _outbox.PublishAsync(
            eventType: "order.placed",
            payload: new { cmd.OrderId, cmd.Total },
            cancellationToken: ct);

        await transaction.CommitAsync(ct);
    }
}
```

### Step 6: Run the processor

The background processor starts automatically as a hosted service when you call `.AddProcessor()`. It polls for pending outbox messages, locks them, delivers webhooks, and handles retries.

No additional configuration is required — the processor runs as long as your application is running.

## How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Your Application                            │
│                                                                     │
│  ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │ Domain Logic  │───>│ IOutboxPublisher  │───>│   SQL Server DB  │  │
│  │ (e.g. Order)  │    │ (same transaction)│    │  ┌────────────┐ │  │
│  └──────────────┘    └──────────────────┘    │  │  Orders     │ │  │
│                                               │  │  OutboxMsgs │ │  │
│                                               │  └────────────┘ │  │
│                                               └────────┬────────┘  │
│                                                         │          │
│  ┌─────────────────────────────────────────────────────┐│          │
│  │           Background Processor                       ││          │
│  │  ┌────────────┐  ┌──────────────┐  ┌─────────────┐ ││          │
│  │  │ IOutboxStore│─>│ Delivery     │─>│ Retry Policy│ ││          │
│  │  │ (lock batch)│  │ (HTTP+HMAC)  │  │ (exp backoff│ ││          │
│  │  └────────────┘  └──────┬───────┘  └─────────────┘ ││          │
│  └──────────────────────────┼──────────────────────────┘│          │
│                              │                           │          │
└──────────────────────────────┼───────────────────────────┘          │
                               │                                      │
                    ┌──────────▼──────────┐                           │
                    │  External Webhooks   │                           │
                    │  • Payment Service   │                           │
                    │  • Inventory Service │                           │
                    │  • Analytics         │                           │
                    └─────────────────────┘
```

## Key Features

- **Transactional guarantee** — outbox writes participate in your existing database transaction
- **Multi-instance safe** — visibility timeout + instance-level locking prevents duplicate processing
- **HMAC-SHA256 webhook signing** — receivers can verify payload authenticity
- **Exponential backoff retries** — with jitter to avoid thundering herd
- **Dead-letter queue** — exhausted messages are dead-lettered for manual review
- **Per-subscription settings** — independent timeout, retry, and header configuration per endpoint
- **Observability** — built-in OpenTelemetry `ActivitySource` and `System.Diagnostics.Metrics`
- **Two SQL Server providers** — EF Core for convenience, direct ADO.NET for minimal overhead

## Configuration Reference

```csharp
builder.Services.AddOutboxNet(options =>
{
    options.SchemaName = "outbox";                                // SQL schema name (default: "outbox")
    options.BatchSize = 50;                                       // Messages per processing cycle
    options.DefaultVisibilityTimeout = TimeSpan.FromSeconds(60);  // Lock duration per message
    options.MaxConcurrentDeliveries = 10;                         // Parallel webhook deliveries
    options.ProcessingMode = ProcessingMode.DirectDelivery;       // or QueueMediated
    // InstanceId is auto-generated as "{MachineName}-{Guid}" for uniqueness
});
```

## Which SQL Server Package?

| If you... | Use |
|---|---|
| Already use EF Core and want migrations + DbContext integration | `OutboxNet.EntityFrameworkCore` |
| Use Dapper, raw ADO.NET, or want zero EF Core overhead | `OutboxNet.SqlServer` |
| Want the lightest possible dependency footprint | `OutboxNet.SqlServer` |
| Need outbox writes in the same transaction as your EF Core `SaveChangesAsync` | `OutboxNet.EntityFrameworkCore` |

## Publishing to NuGet

### Automated (GitHub Actions)

This repository includes a GitHub Actions workflow that automatically publishes all packages to NuGet when you create a GitHub release.

**One-time setup:**

1. Get your NuGet API key from [nuget.org/account/apikeys](https://www.nuget.org/account/apikeys)
   - Select **Push** and **Push new packages and package versions** scopes
   - Set the glob pattern to `OutboxNet.*`
2. Add the key as a repository secret:
   - Go to your repo **Settings > Secrets and variables > Actions**
   - Create a secret named `NUGET_API_KEY` with your API key

**To publish a new version:**

1. Create a GitHub release with a tag matching the desired version (e.g., `1.0.0`, `1.2.0-preview.1`)
2. The workflow automatically:
   - Builds and tests
   - Packs all 7 packages with the release tag as the version
   - Pushes `.nupkg` and `.snupkg` (symbols) to nuget.org

### Manual (local)

```bash
# Pack all projects
dotnet pack -c Release -o ./nupkgs /p:Version=1.0.0

# Push to NuGet (replace YOUR_API_KEY)
dotnet nuget push ./nupkgs/*.nupkg --api-key YOUR_API_KEY --source https://api.nuget.org/v3/index.json --skip-duplicate
```

### Versioning

The version is set in `Directory.Build.props` (`<Version>1.0.0</Version>`). All packages share the same version. When publishing via GitHub Actions, the release tag overrides this version.

## Project Structure

```
OutboxNet/
├── src/
│   ├── OutboxNet.Core/                    # Contracts, models, options, observability
│   ├── OutboxNet.EntityFrameworkCore/     # EF Core + SQL Server stores & publisher
│   ├── OutboxNet.SqlServer/              # Direct ADO.NET SQL Server stores & publisher
│   ├── OutboxNet.Processor/              # Background processing hosted service
│   ├── OutboxNet.Delivery/               # HTTP webhook delivery + HMAC + retry
│   ├── OutboxNet.AzureStorageQueue/      # Azure Storage Queue transport
│   └── OutboxNet.AzureFunctions/         # Azure Functions timer trigger
├── tests/
│   ├── OutboxNet.Core.Tests/
│   ├── OutboxNet.Delivery.Tests/
│   └── OutboxNet.Processor.Tests/
├── docs/
│   └── architecture.md                    # Detailed architecture documentation
├── Directory.Build.props                  # Shared build + NuGet package properties
├── Directory.Packages.props               # Centralized package version management
└── .github/workflows/
    ├── ci.yml                             # Build + test on every push/PR
    └── publish.yml                        # Publish to NuGet on GitHub release
```

## License

[MIT](LICENSE)
