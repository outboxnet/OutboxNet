using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using OutboxNet.Interfaces;
using OutboxNet.Models;
using OutboxNet.Options;

namespace OutboxNet.SqlServer.Stores;

internal sealed class DirectSqlSubscriptionStore : ISubscriptionStore
{
    private readonly string _connectionString;
    private readonly string _schema;

    public DirectSqlSubscriptionStore(
        IOptions<DirectSqlOptions> directSqlOptions,
        IOptions<OutboxOptions> options)
    {
        _connectionString = directSqlOptions.Value.ConnectionString;
        _schema = options.Value.SchemaName;
    }

    public async Task<WebhookSubscription> AddAsync(WebhookSubscription subscription, CancellationToken ct = default)
    {
        subscription.CreatedAt = DateTimeOffset.UtcNow;

        if (subscription.Id == Guid.Empty)
            subscription.Id = Guid.NewGuid();

        var sql = $"""
            INSERT INTO [{_schema}].[WebhookSubscriptions]
                ([Id], [EventType], [WebhookUrl], [Secret], [IsActive], [MaxRetries], [TimeoutSeconds], [CreatedAt], [UpdatedAt], [CustomHeaders])
            VALUES
                (@Id, @EventType, @WebhookUrl, @Secret, @IsActive, @MaxRetries, @TimeoutSeconds, @CreatedAt, @UpdatedAt, @CustomHeaders)
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new SqlParameter("@Id", SqlDbType.UniqueIdentifier) { Value = subscription.Id });
        command.Parameters.Add(new SqlParameter("@EventType", SqlDbType.NVarChar, 256) { Value = subscription.EventType });
        command.Parameters.Add(new SqlParameter("@WebhookUrl", SqlDbType.NVarChar, 2048) { Value = subscription.WebhookUrl });
        command.Parameters.Add(new SqlParameter("@Secret", SqlDbType.NVarChar, 512) { Value = subscription.Secret });
        command.Parameters.Add(new SqlParameter("@IsActive", SqlDbType.Bit) { Value = subscription.IsActive });
        command.Parameters.Add(new SqlParameter("@MaxRetries", SqlDbType.Int) { Value = subscription.MaxRetries });
        command.Parameters.Add(new SqlParameter("@TimeoutSeconds", SqlDbType.Int) { Value = (int)subscription.Timeout.TotalSeconds });
        command.Parameters.Add(new SqlParameter("@CreatedAt", SqlDbType.DateTimeOffset) { Value = subscription.CreatedAt });
        command.Parameters.Add(new SqlParameter("@UpdatedAt", SqlDbType.DateTimeOffset) { Value = (object?)subscription.UpdatedAt ?? DBNull.Value });
        command.Parameters.Add(new SqlParameter("@CustomHeaders", SqlDbType.NVarChar, -1)
        {
            Value = subscription.CustomHeaders != null
                ? (object)JsonSerializer.Serialize(subscription.CustomHeaders)
                : DBNull.Value
        });

        await command.ExecuteNonQueryAsync(ct);
        return subscription;
    }

    public async Task<WebhookSubscription?> GetByIdAsync(Guid id, CancellationToken ct = default)
    {
        var sql = $"""
            SELECT [Id], [EventType], [WebhookUrl], [Secret], [IsActive], [MaxRetries], [TimeoutSeconds], [CreatedAt], [UpdatedAt], [CustomHeaders]
            FROM [{_schema}].[WebhookSubscriptions]
            WHERE [Id] = @Id
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new SqlParameter("@Id", SqlDbType.UniqueIdentifier) { Value = id });

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
            return OutboxMessageMapper.MapSubscriptionFromReader(reader);

        return null;
    }

    public async Task<IReadOnlyList<WebhookSubscription>> GetByEventTypeAsync(string eventType, CancellationToken ct = default)
    {
        var sql = $"""
            SELECT [Id], [EventType], [WebhookUrl], [Secret], [IsActive], [MaxRetries], [TimeoutSeconds], [CreatedAt], [UpdatedAt], [CustomHeaders]
            FROM [{_schema}].[WebhookSubscriptions]
            WHERE [IsActive] = 1 AND ([EventType] = @EventType OR [EventType] = '*')
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new SqlParameter("@EventType", SqlDbType.NVarChar, 256) { Value = eventType });

        var subscriptions = new List<WebhookSubscription>();

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            subscriptions.Add(OutboxMessageMapper.MapSubscriptionFromReader(reader));
        }

        return subscriptions;
    }

    public async Task UpdateAsync(WebhookSubscription subscription, CancellationToken ct = default)
    {
        subscription.UpdatedAt = DateTimeOffset.UtcNow;

        var sql = $"""
            UPDATE [{_schema}].[WebhookSubscriptions]
            SET [EventType] = @EventType,
                [WebhookUrl] = @WebhookUrl,
                [Secret] = @Secret,
                [IsActive] = @IsActive,
                [MaxRetries] = @MaxRetries,
                [TimeoutSeconds] = @TimeoutSeconds,
                [UpdatedAt] = @UpdatedAt,
                [CustomHeaders] = @CustomHeaders
            WHERE [Id] = @Id
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new SqlParameter("@Id", SqlDbType.UniqueIdentifier) { Value = subscription.Id });
        command.Parameters.Add(new SqlParameter("@EventType", SqlDbType.NVarChar, 256) { Value = subscription.EventType });
        command.Parameters.Add(new SqlParameter("@WebhookUrl", SqlDbType.NVarChar, 2048) { Value = subscription.WebhookUrl });
        command.Parameters.Add(new SqlParameter("@Secret", SqlDbType.NVarChar, 512) { Value = subscription.Secret });
        command.Parameters.Add(new SqlParameter("@IsActive", SqlDbType.Bit) { Value = subscription.IsActive });
        command.Parameters.Add(new SqlParameter("@MaxRetries", SqlDbType.Int) { Value = subscription.MaxRetries });
        command.Parameters.Add(new SqlParameter("@TimeoutSeconds", SqlDbType.Int) { Value = (int)subscription.Timeout.TotalSeconds });
        command.Parameters.Add(new SqlParameter("@UpdatedAt", SqlDbType.DateTimeOffset) { Value = subscription.UpdatedAt });
        command.Parameters.Add(new SqlParameter("@CustomHeaders", SqlDbType.NVarChar, -1)
        {
            Value = subscription.CustomHeaders != null
                ? (object)JsonSerializer.Serialize(subscription.CustomHeaders)
                : DBNull.Value
        });

        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task DeactivateAsync(Guid id, CancellationToken ct = default)
    {
        var sql = $"""
            UPDATE [{_schema}].[WebhookSubscriptions]
            SET [IsActive] = 0,
                [UpdatedAt] = SYSDATETIMEOFFSET()
            WHERE [Id] = @Id
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new SqlParameter("@Id", SqlDbType.UniqueIdentifier) { Value = id });

        await command.ExecuteNonQueryAsync(ct);
    }
}
