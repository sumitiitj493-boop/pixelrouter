"""
Same simulation as A3's test, extended to verify GCS result upload.
Run AFTER docker compose up (processor + redis running), with a
real test image already uploaded to your GCS bucket's uploads/ path.

  python test_gcs_result.py
"""
import redis
import uuid
import time
from google.cloud import storage

r = redis.from_url("redis://localhost:6379", decode_responses=True)

BUCKET = "pixelrouter-images-yourname"  # your actual bucket name
job_id = f"job_{uuid.uuid4().hex[:8]}"

r.hset(f"job:{job_id}", mapping={
    "status": "pending",
    "image_url": f"gs://{BUCKET}/test/sample.jpg",
    "created_at": str(int(time.time())),
})

# NEW — push to the specific processor you're testing:
TARGET_PROCESSOR = "processor-1"
r.lpush(f"queue:{TARGET_PROCESSOR}", job_id)
print(f"Pushed job: {job_id}")

# Poll Redis until done
for _ in range(30):
    data = r.hgetall(f"job:{job_id}")
    status = data.get("status")
    print(f"  status={status} stage={data.get('stage','-')}")
    if status in ("done", "failed"):
        break
    time.sleep(1)

if status != "done":
    print(f"\nJob did not complete successfully: {data}")
    raise SystemExit(1)

result_url = data.get("result_url")
print(f"\nJob done. result_url = {result_url}")

# Verify the PNG and JSON actually exist in GCS
client = storage.Client()
bucket = client.bucket(BUCKET)

png_blob = bucket.blob(f"results/{job_id}.png")
json_blob = bucket.blob(f"results/{job_id}.json")

assert png_blob.exists(), "Result PNG missing in GCS!"
assert json_blob.exists(), "Result JSON missing in GCS!"
print(f"✓ results/{job_id}.png exists ({png_blob.size} bytes)")
print(f"✓ results/{job_id}.json exists")

import json
metadata = json.loads(json_blob.download_as_text())
print(f"✓ Metadata: {metadata}")
print("\n=== A4 GCS INTEGRATION VERIFIED ===")
