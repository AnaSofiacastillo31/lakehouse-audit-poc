using Dapper;
using Microsoft.Data.SqlClient;

namespace Audit.Api.Data;

/// <summary>
/// Crea la base y el modelo mínimo del core transaccional.
/// En un sistema real esto serían migraciones versionadas (EF Core migrations / DbUp).
/// </summary>
public static class DatabaseInitializer
{
    private static readonly string[] SchemaStatements =
    [
        "IF SCHEMA_ID(N'audit') IS NULL EXEC(N'CREATE SCHEMA audit');",

        """
        IF OBJECT_ID(N'dbo.Transfers', N'U') IS NULL
        CREATE TABLE dbo.Transfers (
            Id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_Transfers PRIMARY KEY CLUSTERED,
            SourceAccount VARCHAR(20)   NOT NULL,
            TargetAccount VARCHAR(20)   NOT NULL,
            Amount        DECIMAL(18,2) NOT NULL,
            Currency      CHAR(3)       NOT NULL,
            Channel       VARCHAR(20)   NULL,
            CreatedAt     DATETIME2(3)  NOT NULL
        );
        """,

        // Outbox: tabla angosta y de vida corta. Es la ÚNICA huella de auditoría que queda en SQL Server;
        // el histórico vive en el Lakehouse. Las filas publicadas se purgan por lotes (OutboxRetentionService).
        """
        IF OBJECT_ID(N'audit.OutboxEvents', N'U') IS NULL
        CREATE TABLE audit.OutboxEvents (
            Id            BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_OutboxEvents PRIMARY KEY CLUSTERED,
            EventId       UNIQUEIDENTIFIER NOT NULL CONSTRAINT UQ_OutboxEvents_EventId UNIQUE,
            AggregateType VARCHAR(50)   NOT NULL,
            AggregateId   VARCHAR(50)   NOT NULL,
            EventType     VARCHAR(100)  NOT NULL,
            SchemaVersion INT           NOT NULL,
            Payload       NVARCHAR(MAX) NOT NULL,
            OccurredAt    DATETIME2(3)  NOT NULL,
            PublishedAt   DATETIME2(3)  NULL
        );
        """,

        // Índice filtrado: el relay solo recorre pendientes, sin escanear lo ya publicado.
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_OutboxEvents_Pending')
        CREATE NONCLUSTERED INDEX IX_OutboxEvents_Pending
            ON audit.OutboxEvents (Id) WHERE PublishedAt IS NULL;
        """,

        // Índice filtrado para la purga por antigüedad de publicación.
        """
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_OutboxEvents_PublishedAt')
        CREATE NONCLUSTERED INDEX IX_OutboxEvents_PublishedAt
            ON audit.OutboxEvents (PublishedAt) WHERE PublishedAt IS NOT NULL;
        """
    ];

    public static async Task InitializeAsync(string connectionString, ILogger logger, CancellationToken ct)
    {
        var target = new SqlConnectionStringBuilder(connectionString);
        var databaseName = target.InitialCatalog;
        var master = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master" };

        const int maxAttempts = 30;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using (var conn = new SqlConnection(master.ConnectionString))
                {
                    await conn.OpenAsync(ct);
                    await conn.ExecuteAsync($"IF DB_ID(N'{databaseName}') IS NULL CREATE DATABASE [{databaseName}];");

                    // RCSI: los lectores no bloquean a los escritores (y viceversa) en el core transaccional.
                    var rcsiOn = await conn.ExecuteScalarAsync<bool>(
                        "SELECT is_read_committed_snapshot_on FROM sys.databases WHERE name = @databaseName;",
                        new { databaseName });
                    if (!rcsiOn)
                    {
                        await conn.ExecuteAsync(
                            $"ALTER DATABASE [{databaseName}] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;");
                    }
                }

                await using (var conn = new SqlConnection(connectionString))
                {
                    await conn.OpenAsync(ct);
                    foreach (var statement in SchemaStatements)
                    {
                        await conn.ExecuteAsync(statement);
                    }
                }

                logger.LogInformation("Base de datos {Database} lista", databaseName);
                return;
            }
            catch (SqlException ex) when (attempt < maxAttempts)
            {
                logger.LogWarning("SQL Server aún no disponible (intento {Attempt}/{Max}): {Message}",
                    attempt, maxAttempts, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
            }
        }
    }
}
