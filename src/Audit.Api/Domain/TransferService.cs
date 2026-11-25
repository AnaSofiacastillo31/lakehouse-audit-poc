using System.Text.Json;
using Audit.Api.Data;
using Dapper;

namespace Audit.Api.Domain;

/// <summary>
/// Transactional Outbox: el cambio de negocio y el evento de auditoría se confirman juntos o no se confirman.
/// La request NO habla con Kafka ni con el Lakehouse: solo hace un INSERT extra en una tabla angosta.
/// </summary>
public sealed class TransferService(SqlConnectionFactory factory)
{
    private const string InsertTransferSql = """
        INSERT INTO dbo.Transfers (SourceAccount, TargetAccount, Amount, Currency, Channel, CreatedAt)
        OUTPUT INSERTED.Id
        VALUES (@SourceAccount, @TargetAccount, @Amount, @Currency, @Channel, @CreatedAt);
        """;

    private const string InsertOutboxSql = """
        INSERT INTO audit.OutboxEvents (EventId, AggregateType, AggregateId, EventType, SchemaVersion, Payload, OccurredAt)
        VALUES (@EventId, @AggregateType, @AggregateId, @EventType, @SchemaVersion, @Payload, @OccurredAt);
        """;

    private static readonly string[] Channels = ["WEB", "MOBILE", "BRANCH", "API"];
    private static readonly string[] Currencies = ["COP", "COP", "COP", "USD"];

    public async Task<long> CreateAsync(TransferRequest request, CancellationToken ct)
    {
        await using var conn = factory.Create();
        await conn.OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var now = DateTime.UtcNow;

        var transferId = await conn.ExecuteScalarAsync<long>(new CommandDefinition(
            InsertTransferSql,
            new
            {
                request.SourceAccount,
                request.TargetAccount,
                request.Amount,
                Currency = request.Currency.ToUpperInvariant(),
                request.Channel,
                CreatedAt = now
            },
            tx,
            cancellationToken: ct));

        var schemaVersion = request.Channel is null ? 1 : 2;
        var payload = schemaVersion == 1
            ? JsonSerializer.Serialize(new
            {
                transferId,
                sourceAccount = request.SourceAccount,
                targetAccount = request.TargetAccount,
                amount = request.Amount,
                currency = request.Currency.ToUpperInvariant()
            })
            : JsonSerializer.Serialize(new
            {
                transferId,
                sourceAccount = request.SourceAccount,
                targetAccount = request.TargetAccount,
                amount = request.Amount,
                currency = request.Currency.ToUpperInvariant(),
                channel = request.Channel
            });

        await conn.ExecuteAsync(new CommandDefinition(
            InsertOutboxSql,
            new
            {
                EventId = Guid.NewGuid(),
                AggregateType = "Transfer",
                AggregateId = transferId.ToString(),
                EventType = "TransferCreated",
                SchemaVersion = schemaVersion,
                Payload = payload,
                OccurredAt = now
            },
            tx,
            cancellationToken: ct));

        await tx.CommitAsync(ct);
        return transferId;
    }

    public async Task<int> SimulateAsync(int count, CancellationToken ct)
    {
        var random = Random.Shared;
        for (var i = 0; i < count; i++)
        {
            var source = $"ACC-{random.Next(1, 200):D4}";
            var target = $"ACC-{random.Next(200, 400):D4}";
            var amount = Math.Round((decimal)(random.NextDouble() * 5_000_000) + 1000m, 2);
            var currency = Currencies[random.Next(Currencies.Length)];
            // ~40 % de eventos v1 (sin canal) y ~60 % v2 (con canal): conviven ambas versiones del esquema.
            var channel = random.NextDouble() < 0.4 ? null : Channels[random.Next(Channels.Length)];

            await CreateAsync(new TransferRequest(source, target, amount, currency, channel), ct);
        }

        return count;
    }
}
