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

BUCKET = "pixelrouter-images-sumit"  # your actual bucket name
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
print(f"\nSuccess! Go check Google Cloud to see your image at {result_url}")
print("=== A4 GCS INTEGRATION VERIFIED ===")