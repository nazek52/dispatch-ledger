using DispatchLedger.Api;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("Postgres")
    ?? throw new InvalidOperationException("ConnectionStrings:Postgres is required.");
var kafkaBootstrap = builder.Configuration["Kafka:BootstrapServers"]
    ?? throw new InvalidOperationException("Kafka:BootstrapServers is required.");

builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.AddScoped<ShipmentCommands>();
builder.Services.AddScoped<ShipmentQueries>();
builder.Services.AddHostedService(sp => new OutboxPublisher(
    sp.GetRequiredService<NpgsqlDataSource>(), kafkaBootstrap,
    sp.GetRequiredService<ILogger<OutboxPublisher>>()));

var app = builder.Build();

app.MapGet("/health", async (NpgsqlDataSource db, CancellationToken ct) =>
{
    try
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand("SELECT 1", connection);
        await command.ExecuteScalarAsync(ct);
        return Results.Ok(new { status = "ready" });
    }
    catch (NpgsqlException)
    {
        return Results.StatusCode(503);
    }
});

app.MapPost("/shipments", async (CreateShipmentRequest request, ShipmentCommands commands, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.TrackingCode) || request.TrackingCode.Length > 64)
        return Results.BadRequest(new { error = "TrackingCode must contain 1-64 characters." });

    try
    {
        var shipment = await commands.CreateAsync(request.TrackingCode.Trim(), ct);
        return Results.Created($"/shipments/{shipment.Id}", shipment);
    }
    catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
    {
        return Results.Conflict(new { error = "TrackingCode already exists." });
    }
});

app.MapPost("/shipments/{id:guid}/dispatch", async (Guid id, DispatchShipmentRequest request,
    ShipmentCommands commands, CancellationToken ct) =>
{
    if (request.ExpectedVersion < 1)
        return Results.BadRequest(new { error = "ExpectedVersion must be positive." });

    var result = await commands.DispatchAsync(id, request.ExpectedVersion, ct);
    return result switch
    {
        DispatchResult.Success success => Results.Ok(success.Shipment),
        DispatchResult.NotFound => Results.NotFound(),
        DispatchResult.InvalidState => Results.Conflict(new { error = "Shipment is already dispatched." }),
        _ => Results.Conflict(new { error = "Version conflict. Reload and retry." })
    };
});

app.MapGet("/shipments/{id:guid}", async (Guid id, ShipmentQueries queries, CancellationToken ct) =>
{
    var shipment = await queries.GetAsync(id, ct);
    return shipment is null ? Results.NotFound() : Results.Ok(shipment);
});

app.Run();

public partial class Program;
