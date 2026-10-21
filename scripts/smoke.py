"""Exercise command, query, concurrency, outbox and Kafka delivery through Docker Compose."""
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid

BASE = "http://127.0.0.1:8088"


def request(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error) if error.headers.get_content_type() == "application/json" else None


for _ in range(90):
    try:
        if request("GET", "/health")[0] == 200:
            break
    except OSError:
        pass
    time.sleep(2)
else:
    raise SystemExit("API did not become ready")

code = "demo-" + uuid.uuid4().hex[:12]
status, created = request("POST", "/shipments", {"trackingCode": code})
assert status == 201 and created["version"] == 1, (status, created)
shipment_id = created["id"]
assert request("POST", "/shipments", {"trackingCode": code})[0] == 409
status, dispatched = request("POST", f"/shipments/{shipment_id}/dispatch", {"expectedVersion": 1})
assert status == 200 and dispatched["status"] == "dispatched" and dispatched["version"] == 2
assert request("POST", f"/shipments/{shipment_id}/dispatch", {"expectedVersion": 1})[0] == 409
assert request("GET", f"/shipments/{shipment_id}")[1] == dispatched

for _ in range(30):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "db", "psql", "-U", "dispatchledger", "-d", "dispatchledger", "-Atc",
         f"SELECT count(*) FROM outbox WHERE aggregate_id = '{shipment_id}' AND published_at IS NOT NULL"],
        capture_output=True, text=True, check=True,
    )
    if result.stdout.strip() == "2":
        break
    time.sleep(2)
else:
    raise SystemExit("Outbox messages were not published")

print("Smoke test passed: create, duplicate, dispatch, stale command, query, two Kafka publications")
