namespace Audit.OutboxRelay;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:19092";
    public string Topic { get; set; } = "audit.events";
}

public sealed class OutboxOptions
{
    public int BatchSize { get; set; } = 500;
    public int PollIntervalMs { get; set; } = 1000;
}

public sealed class RetentionOptions
{
    /// <summary>Minutos que una fila YA publicada permanece en SQL Server antes de purgarse.</summary>
    public int PublishedRetentionMinutes { get; set; } = 10;

    /// <summary>Por debajo de 5000 filas para evitar el escalamiento de bloqueos a nivel de tabla.</summary>
    public int DeleteBatchSize { get; set; } = 4000;

    public int IntervalSeconds { get; set; } = 60;
}

public sealed class OutboxRow
{
    public long Id { get; set; }
    public Guid EventId { get; set; }
    public string AggregateType { get; set; } = "";
    public string AggregateId { get; set; } = "";
    public string EventType { get; set; } = "";
    public int SchemaVersion { get; set; }
    public string Payload { get; set; } = "";
    public DateTime OccurredAt { get; set; }
}
