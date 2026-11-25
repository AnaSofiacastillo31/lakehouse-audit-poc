using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;

namespace Audit.OutboxRelay;

/// <summary>
/// Ciclo de vida en SQL Server: purga las filas YA publicadas más antiguas que la ventana de retención.
/// - DELETE TOP (N) en bucle: transacciones cortas, sin escalamiento de bloqueos ni log gigante.
/// - Nunca toca filas pendientes (PublishedAt IS NULL), así que no hay pérdida de eventos.
/// En producción la ventana sería de días y se validaría contra la marca de agua del Lakehouse
/// (máximo offset confirmado en bronze) antes de borrar.
/// </summary>
public sealed class OutboxRetentionService(
    IConfiguration configuration,
    IOptions<RetentionOptions> options,
    ILogger<OutboxRetentionService> logger) : BackgroundService
{
    private const string PurgeSql = """
        DELETE TOP (@DeleteBatchSize) FROM audit.OutboxEvents
        WHERE PublishedAt IS NOT NULL
          AND PublishedAt < DATEADD(MINUTE, -@PublishedRetentionMinutes, SYSUTCDATETIME());
        """;

    private readonly string _connectionString = configuration.GetConnectionString("Core")
        ?? throw new InvalidOperationException("Falta ConnectionStrings:Core");

    private readonly RetentionOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Retención del outbox: {Minutes} min, lotes de {Batch}",
            _options.PublishedRetentionMinutes, _options.DeleteBatchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var total = 0;
                int deleted;
                do
                {
                    await using var conn = new SqlConnection(_connectionString);
                    deleted = await conn.ExecuteAsync(new CommandDefinition(PurgeSql, new
                    {
                        _options.DeleteBatchSize,
                        _options.PublishedRetentionMinutes
                    }, cancellationToken: stoppingToken));
                    total += deleted;

                    if (deleted == _options.DeleteBatchSize)
                    {
                        await Task.Delay(200, stoppingToken); // deja respirar al core entre lotes
                    }
                } while (deleted == _options.DeleteBatchSize);

                if (total > 0)
                {
                    logger.LogInformation("Purgadas {Total} filas publicadas del outbox", total);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning("Error en la purga del outbox: {Message}", ex.Message);
            }

            await Task.Delay(TimeSpan.FromSeconds(_options.IntervalSeconds), stoppingToken);
        }
    }
}
