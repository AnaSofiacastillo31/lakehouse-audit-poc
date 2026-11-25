using Audit.OutboxRelay;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<OutboxOptions>(builder.Configuration.GetSection("Outbox"));
builder.Services.Configure<RetentionOptions>(builder.Configuration.GetSection("Retention"));

builder.Services.AddSingleton(sp =>
{
    var kafka = sp.GetRequiredService<IOptions<KafkaOptions>>().Value;
    var config = new ProducerConfig
    {
        BootstrapServers = kafka.BootstrapServers,
        // Productor idempotente + acks=all: sin duplicados por reintentos de red y orden garantizado por partición.
        EnableIdempotence = true,
        Acks = Acks.All,
        LingerMs = 5,
        CompressionType = CompressionType.Zstd
    };
    return new ProducerBuilder<string, string>(config).Build();
});

builder.Services.AddHostedService<OutboxRelayService>();
builder.Services.AddHostedService<OutboxRetentionService>();

builder.Build().Run();
