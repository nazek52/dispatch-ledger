# Dispatch Ledger

A small ASP.NET Core service demonstrating command/query separation, PostgreSQL transactions and a Kafka integration through a transactional outbox. Built as a personal portfolio exercise for backend roles.

## Run and verify

Requires Docker Compose. From this directory:

```sh
docker compose up -d --build
python scripts/smoke.py
docker compose down
```

The API is available only on `http://127.0.0.1:8088`. The smoke test creates a shipment, rejects a duplicate tracking code, dispatches it, rejects a stale command, queries the new state and waits until both outbox rows have been published to Kafka. Data persists in a named PostgreSQL volume across `down` and `up`.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | PostgreSQL readiness |
| POST | `/shipments` | Create with `{ "trackingCode": "ABC-123" }` |
| POST | `/shipments/{id}/dispatch` | Dispatch with `{ "expectedVersion": 1 }` |
| GET | `/shipments/{id}` | Read shipment projection |

Duplicate tracking codes and invalid state/version changes return HTTP 409. The database unique constraint and conditional update provide the final concurrency guard.

## Architecture and guarantees

`ShipmentCommands` owns state transitions; `ShipmentQueries` reads a projection without command logic. This is CQRS at the application layer with one database, not separate read and write stores. Each command commits its state change and an outbox event in the same PostgreSQL transaction. The background publisher selects one pending event using `FOR UPDATE SKIP LOCKED`, publishes it to the `shipment-events` Kafka topic, then marks it published.

Delivery is **at least once**. A crash after Kafka accepts an event but before PostgreSQL commits the published marker can send the event again. Consumers must deduplicate using the `event-id` header. This demo holds a database transaction open during broker publication; a production service would use short leases and retry/backoff policy to avoid long-held locks. The local Docker credentials are for this demo only. The API has no authentication and must not be exposed publicly.

## Development

```sh
dotnet build src/DispatchLedger.Api/DispatchLedger.Api.csproj
```

GitHub Actions builds the service and runs the same Docker Compose smoke scenario. The project is intentionally narrow so the transaction boundary, optimistic concurrency and delivery semantics are easy to review.
