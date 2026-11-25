using Dapper;

namespace Audit.Api.Data;

public sealed class OutboxStats
{
    public long Transfers { get; set; }
    public long OutboxRows { get; set; }
    public long Pending { get; set; }
    public long Published { get; set; }
}

public static class OutboxStatsQuery
{
    private const string Sql = """
        SELECT
            (SELECT COUNT_BIG(*) FROM dbo.Transfers)                                        AS Transfers,
            COUNT_BIG(*)                                                                     AS OutboxRows,
            CAST(ISNULL(SUM(CASE WHEN PublishedAt IS NULL     THEN 1 ELSE 0 END), 0) AS BIGINT) AS Pending,
            CAST(ISNULL(SUM(CASE WHEN PublishedAt IS NOT NULL THEN 1 ELSE 0 END), 0) AS BIGINT) AS Published
        FROM audit.OutboxEvents;
        """;

    public static async Task<OutboxStats> GetAsync(SqlConnectionFactory factory, CancellationToken ct)
    {
        await using var conn = factory.Create();
        return await conn.QuerySingleAsync<OutboxStats>(new CommandDefinition(Sql, cancellationToken: ct));
    }
}
