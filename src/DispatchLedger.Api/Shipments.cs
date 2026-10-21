using System.Text.Json;
using Npgsql;

namespace DispatchLedger.Api;

public record CreateShipmentRequest(string TrackingCode);
public record DispatchShipmentRequest(int ExpectedVersion);
public record ShipmentView(Guid Id, string TrackingCode, string Status, int Version);

public abstract record DispatchResult
{
    public sealed record Success(ShipmentView Shipment) : DispatchResult;
    public sealed record NotFound : DispatchResult;
    public sealed record InvalidState : DispatchResult;
    public sealed record VersionConflict : DispatchResult;
}

// Write side: state changes and their integration events share one DB transaction.
public sealed class ShipmentCommands(NpgsqlDataSource db)
{
    public async Task<ShipmentView> CreateAsync(string trackingCode, CancellationToken ct)
    {
        var shipment = new ShipmentView(Guid.NewGuid(), trackingCode, "created", 1);
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        await using (var insert = new NpgsqlCommand(
            "INSERT INTO shipments (id, tracking_code, status, version) VALUES (@id, @code, @status, @version)",
            connection, transaction))
        {
            insert.Parameters.AddWithValue("id", shipment.Id);
            insert.Parameters.AddWithValue("code", shipment.TrackingCode);
            insert.Parameters.AddWithValue("status", shipment.Status);
            insert.Parameters.AddWithValue("version", shipment.Version);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await AddOutboxAsync(connection, transaction, shipment, "shipment.created", ct);
        await transaction.CommitAsync(ct);
        return shipment;
    }

    public async Task<DispatchResult> DispatchAsync(Guid id, int expectedVersion, CancellationToken ct)
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        ShipmentView? updated;
        await using (var command = new NpgsqlCommand("""
            UPDATE shipments SET status = 'dispatched', version = version + 1
            WHERE id = @id AND version = @version AND status = 'created'
            RETURNING id, tracking_code, status, version
            """, connection, transaction))
        {
            command.Parameters.AddWithValue("id", id);
            command.Parameters.AddWithValue("version", expectedVersion);
            await using var reader = await command.ExecuteReaderAsync(ct);
            updated = await reader.ReadAsync(ct)
                ? new ShipmentView(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetInt32(3))
                : null;
        }

        if (updated is not null)
        {
            await AddOutboxAsync(connection, transaction, updated, "shipment.dispatched", ct);
            await transaction.CommitAsync(ct);
            return new DispatchResult.Success(updated);
        }

        await using var check = new NpgsqlCommand("SELECT status, version FROM shipments WHERE id = @id", connection, transaction);
        check.Parameters.AddWithValue("id", id);
        await using var current = await check.ExecuteReaderAsync(ct);
        if (!await current.ReadAsync(ct)) return new DispatchResult.NotFound();
        if (current.GetString(0) != "created") return new DispatchResult.InvalidState();
        return new DispatchResult.VersionConflict();
    }

    private static async Task AddOutboxAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        ShipmentView shipment, string eventType, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("""
            INSERT INTO outbox (id, aggregate_id, event_type, payload)
            VALUES (@id, @aggregate, @event, @payload::jsonb)
            """, connection, transaction);
        command.Parameters.AddWithValue("id", Guid.NewGuid());
        command.Parameters.AddWithValue("aggregate", shipment.Id);
        command.Parameters.AddWithValue("event", eventType);
        command.Parameters.AddWithValue("payload", JsonSerializer.Serialize(new
        {
            shipment.Id, shipment.TrackingCode, shipment.Status, shipment.Version
        }));
        await command.ExecuteNonQueryAsync(ct);
    }
}

// Read side is independent of command decisions and exposes a stable projection.
public sealed class ShipmentQueries(NpgsqlDataSource db)
{
    public async Task<ShipmentView?> GetAsync(Guid id, CancellationToken ct)
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(
            "SELECT id, tracking_code, status, version FROM shipments WHERE id = @id", connection);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct)
            ? new ShipmentView(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetInt32(3))
            : null;
    }
}
