using Audit.Api.Data;
using Audit.Api.Domain;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Core")
    ?? throw new InvalidOperationException("Falta ConnectionStrings:Core");

builder.Services.AddSingleton(new SqlConnectionFactory(connectionString));
builder.Services.AddSingleton<TransferService>();

var app = builder.Build();

// Crea la base, los esquemas y activa READ_COMMITTED_SNAPSHOT (idempotente, con reintentos).
await DatabaseInitializer.InitializeAsync(connectionString, app.Logger, app.Lifetime.ApplicationStopping);

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

// Operación de negocio: la transferencia y su evento de auditoría se escriben en la MISMA transacción.
app.MapPost("/api/transfers", async (TransferRequest request, TransferService service, CancellationToken ct) =>
{
    var errors = request.Validate();
    if (errors.Count > 0)
    {
        return Results.ValidationProblem(errors);
    }

    var id = await service.CreateAsync(request, ct);
    return Results.Created($"/api/transfers/{id}", new { id });
});

// Generador de carga para la demo: N transferencias, mezclando eventos v1 y v2 (evolución de esquema).
app.MapPost("/api/simulate", async (int? count, TransferService service, CancellationToken ct) =>
{
    var total = Math.Clamp(count ?? 100, 1, 5000);
    var created = await service.SimulateAsync(total, ct);
    return Results.Ok(new { created });
});

// Estado del outbox: pendientes vs. publicados (los publicados se purgan según la retención).
app.MapGet("/api/outbox/stats", async (SqlConnectionFactory db, CancellationToken ct) =>
    Results.Ok(await OutboxStatsQuery.GetAsync(db, ct)));

app.Run();
