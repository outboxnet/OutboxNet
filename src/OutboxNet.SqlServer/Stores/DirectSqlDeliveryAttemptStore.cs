using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using OutboxNet.Interfaces;
using OutboxNet.Models;
using OutboxNet.Options;

namespace OutboxNet.SqlServer.Stores;

internal sealed class DirectSqlDeliveryAttemptStore : IDeliveryAttemptStore
{
    private readonly string _connectionString;
    private readonly string _schema;

    public DirectSqlDeliveryAttemptStore(
        IOptions<DirectSqlOptions> directSqlOptions,
        IOptions<OutboxOptions> options)
    {
        _connectionString = directSqlOptions.Value.ConnectionString;
        _schema = options.Value.SchemaName;
    }

    public async Task SaveAttemptAsync(DeliveryAttempt attempt, CancellationToken ct = default)
    {
        if (attempt.Id == Guid.Empty)
            attempt.Id = Guid.NewGuid();

        var sql = $"""
            INSERT INTO [{_schema}].[DeliveryAttempts]
                ([Id], [OutboxMessageId], [WebhookSubscriptionId], [AttemptNumber], [Status], [HttpStatusCode], [ResponseBody], [ErrorMessage], [DurationMs], [AttemptedAt], [NextRetryAt])
            VALUES
                (@Id, @OutboxMessageId, @WebhookSubscriptionId, @AttemptNumber, @Status, @HttpStatusCode, @ResponseBody, @ErrorMessage, @DurationMs, @AttemptedAt, @NextRetryAt)
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new SqlParameter("@Id", SqlDbType.UniqueIdentifier) { Value = attempt.Id });
        command.Parameters.Add(new SqlParameter("@OutboxMessageId", SqlDbType.UniqueIdentifier) { Value = attempt.OutboxMessageId });
        command.Parameters.Add(new SqlParameter("@WebhookSubscriptionId", SqlDbType.UniqueIdentifier) { Value = attempt.WebhookSubscriptionId });
        command.Parameters.Add(new SqlParameter("@AttemptNumber", SqlDbType.Int) { Value = attempt.AttemptNumber });
        command.Parameters.Add(new SqlParameter("@Status", SqlDbType.Int) { Value = (int)attempt.Status });
        command.Parameters.Add(new SqlParameter("@HttpStatusCode", SqlDbType.Int) { Value = (object?)attempt.HttpStatusCode ?? DBNull.Value });
        command.Parameters.Add(new SqlParameter("@ResponseBody", SqlDbType.NVarChar, 4000) { Value = (object?)attempt.ResponseBody ?? DBNull.Value });
        command.Parameters.Add(new SqlParameter("@ErrorMessage", SqlDbType.NVarChar, -1) { Value = (object?)attempt.ErrorMessage ?? DBNull.Value });
        command.Parameters.Add(new SqlParameter("@DurationMs", SqlDbType.BigInt) { Value = attempt.DurationMs });
        command.Parameters.Add(new SqlParameter("@AttemptedAt", SqlDbType.DateTimeOffset) { Value = attempt.AttemptedAt == default ? DateTimeOffset.UtcNow : attempt.AttemptedAt });
        command.Parameters.Add(new SqlParameter("@NextRetryAt", SqlDbType.DateTimeOffset) { Value = (object?)attempt.NextRetryAt ?? DBNull.Value });

        await command.ExecuteNonQueryAsync(ct);
    }

    public async Task<IReadOnlyList<DeliveryAttempt>> GetByMessageIdAsync(Guid messageId, CancellationToken ct = default)
    {
        var sql = $"""
            SELECT [Id], [OutboxMessageId], [WebhookSubscriptionId], [AttemptNumber], [Status], [HttpStatusCode], [ResponseBody], [ErrorMessage], [DurationMs], [AttemptedAt], [NextRetryAt]
            FROM [{_schema}].[DeliveryAttempts]
            WHERE [OutboxMessageId] = @OutboxMessageId
            ORDER BY [AttemptNumber]
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new SqlParameter("@OutboxMessageId", SqlDbType.UniqueIdentifier) { Value = messageId });

        var attempts = new List<DeliveryAttempt>();

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            attempts.Add(OutboxMessageMapper.MapDeliveryAttemptFromReader(reader));
        }

        return attempts;
    }

    public async Task<IReadOnlyList<DeliveryAttempt>> GetBySubscriptionIdAsync(
        Guid subscriptionId,
        int limit = 50,
        CancellationToken ct = default)
    {
        var sql = $"""
            SELECT TOP (@Limit) [Id], [OutboxMessageId], [WebhookSubscriptionId], [AttemptNumber], [Status], [HttpStatusCode], [ResponseBody], [ErrorMessage], [DurationMs], [AttemptedAt], [NextRetryAt]
            FROM [{_schema}].[DeliveryAttempts]
            WHERE [WebhookSubscriptionId] = @WebhookSubscriptionId
            ORDER BY [AttemptedAt] DESC
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new SqlParameter("@WebhookSubscriptionId", SqlDbType.UniqueIdentifier) { Value = subscriptionId });
        command.Parameters.Add(new SqlParameter("@Limit", SqlDbType.Int) { Value = limit });

        var attempts = new List<DeliveryAttempt>();

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            attempts.Add(OutboxMessageMapper.MapDeliveryAttemptFromReader(reader));
        }

        return attempts;
    }

    public async Task<int> GetAttemptCountAsync(Guid messageId, Guid subscriptionId, CancellationToken ct = default)
    {
        var sql = $"""
            SELECT COUNT(*)
            FROM [{_schema}].[DeliveryAttempts]
            WHERE [OutboxMessageId] = @OutboxMessageId
              AND [WebhookSubscriptionId] = @WebhookSubscriptionId
            """;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync(ct);

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.Add(new SqlParameter("@OutboxMessageId", SqlDbType.UniqueIdentifier) { Value = messageId });
        command.Parameters.Add(new SqlParameter("@WebhookSubscriptionId", SqlDbType.UniqueIdentifier) { Value = subscriptionId });

        return (int)(await command.ExecuteScalarAsync(ct))!;
    }
}
