using Confluent.Kafka;
using Npgsql;

namespace DispatchLedger.Api;

public sealed class OutboxPublisher(NpgsqlDataSource db, string kafkaBootstrap,
    ILogger<OutboxPublisher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = kafkaBootstrap,
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var published = await PublishOneAsync(producer, stoppingToken);
                if (!published) await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Outbox publication failed; retrying");
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }

    private async Task<bool> PublishOneAsync(IProducer<string, string> producer, CancellationToken ct)
    {
        await using var connection = await db.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        Guid eventId;
        Guid aggregateId;
        string eventType;
        string payload;
        await using (var select = new NpgsqlCommand("""
            SELECT id, aggregate_id, event_type, payload::text
            FROM outbox WHERE published_at IS NULL
            ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED
            """, connection, transaction))
        await using (var reader = await select.ExecuteReaderAsync(ct))
        {
            if (!await reader.ReadAsync(ct)) return false;
            eventId = reader.GetGuid(0);
            aggregateId = reader.GetGuid(1);
            eventType = reader.GetString(2);
            payload = reader.GetString(3);
        }

        // Kafka may accept this message before the DB commit. Consumers must deduplicate event-id.
        await producer.ProduceAsync("shipment-events", new Message<string, string>
        {
            Key = aggregateId.ToString(),
            Value = payload,
            Headers = new Headers
            {
                { "event-id", System.Text.Encoding.UTF8.GetBytes(eventId.ToString()) },
                { "event-type", System.Text.Encoding.UTF8.GetBytes(eventType) }
            }
        }, ct);

        await using var update = new NpgsqlCommand(
            "UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE id = @id", connection, transaction);
        update.Parameters.AddWithValue("id", eventId);
        await update.ExecuteNonQueryAsync(ct);
        await transaction.CommitAsync(ct);
        logger.LogInformation("Published {EventType} event {EventId}", eventType, eventId);
        return true;
    }
}
