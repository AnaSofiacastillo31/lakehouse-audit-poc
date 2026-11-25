using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace Audit.OutboxRelay;

/// <summary>
/// Lee el outbox por lotes y publica a Kafka/Redpanda. Garantía: at-least-once.
/// - UPDLOCK + READPAST + ROWLOCK: varias réplicas del relay pueden trabajar en paralelo sin bloquearse
///   ni tomar las mismas filas, y los INSERT de la API nunca esperan al relay.
/// - Si Kafka falla, la transacción se revierte y el lote se reintenta: puede haber duplicados,
///   que se eliminan aguas abajo por event_id (MERGE en la capa silver).
/// </summary>
public sealed class OutboxRelayService(
    IConfiguration configuration,
    IProducer<string, string> producer,
    IOptions<KafkaOptions> kafkaOptions,
    IOptions<OutboxOptions> outboxOptions,
    ILogger<OutboxRelayService> logger) : BackgroundService
{
    private const string SelectBatchSql = """
        SELECT TOP (@BatchSize) Id, EventId, AggregateType, AggregateId, EventType, SchemaVersion, Payload, OccurredAt
        FROM audit.OutboxEvents WITH (UPDLOCK, READPAST, ROWLOCK)
        WHERE PublishedAt IS NULL
        ORDER BY Id;
        """;

    private const string MarkPublishedSql = """
        UPDATE audit.OutboxEvents SET PublishedAt = SYSUTCDATETIME() WHERE Id IN @Ids;
        """;

    private readonly string _connectionString = configuration.GetConnectionString("Core")
        ?? throw new InvalidOperationException("Falta ConnectionStrings:Core");

    private readonly KafkaOptions _kafka = kafkaOptions.Value;
    private readonly OutboxOptions _outbox = outboxOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Outbox relay iniciado. Topic={Topic} Batch={Batch}", _kafka.Topic, _outbox.BatchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var published = await PublishBatchAsync(stoppingToken);
                if (published == 0)
                {
                    await Task.Delay(_outbox.PollIntervalMs, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // SQL Server o Redpanda aún arrancando, o caída transitoria: se reintenta sin perder eventos.
                logger.LogWarning("Error publicando lote, se reintenta en 5 s: {Message}", ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    private async Task<int> PublishBatchAsync(CancellationToken ct)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        var rows = (await conn.QueryAsync<OutboxRow>(new CommandDefinition(
            SelectBatchSql, new { _outbox.BatchSize }, tx, cancellationToken: ct))).AsList();

        if (rows.Count == 0)
        {
            await tx.CommitAsync(ct);
            return 0;
        }

        // Clave = AggregateId: todos los eventos de una misma entidad van a la misma partición (orden por entidad).
        var deliveries = rows.Select(row => producer.ProduceAsync(_kafka.Topic, new Message<string, string>
        {
            Key = row.AggregateId,
            Value = BuildEnvelope(row),
            Headers = new Headers
            {
                { "event_id", Encoding.UTF8.GetBytes(row.EventId.ToString()) },
                { "schema_version", Encoding.UTF8.GetBytes(row.SchemaVersion.ToString()) }
            }
        }, ct));

        await Task.WhenAll(deliveries);

        await conn.ExecuteAsync(new CommandDefinition(
            MarkPublishedSql, new { Ids = rows.Select(r => r.Id).ToArray() }, tx, cancellationToken: ct));

        await tx.CommitAsync(ct);

        logger.LogInformation("Publicados {Count} eventos (Id {First}..{Last})", rows.Count, rows[0].Id, rows[^1].Id);
        return rows.Count;
    }

    private static string BuildEnvelope(OutboxRow row) => JsonSerializer.Serialize(new
    {
        event_id = row.EventId,
        aggregate_type = row.AggregateType,
        aggregate_id = row.AggregateId,
        event_type = row.EventType,
        schema_version = row.SchemaVersion,
        occurred_at = DateTime.SpecifyKind(row.OccurredAt, DateTimeKind.Utc).ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'"),
        // El payload viaja como string JSON: bronze lo guarda crudo y silver lo interpreta según schema_version.
        payload = row.Payload
    });
}
