"""
Simulates what upload-service (B3) will do.
Run this AFTER `docker compose up` (processor + redis running).
Requires: a real image already uploaded to your GCS bucket at
the path below (use the GCS console or gsutil to upload one test file).

  python test_worker_local.py
"""
import redis
import uuid
import time

# Connect to local Redis
r = redis.from_url("redis://localhost:6379", decode_responses=True)

job_id = f"job_{uuid.uuid4().hex[:8]}"

# 1. Create job hash — mimics B3's upload-service
r.hset(f"job:{job_id}", mapping={
    "status": "pending",
    # TODO: Replace this with the path to your actual GCS test image
    "image_url": "gs://pixelrouter-images-yourname/test/sample.jpg",
    "created_at": str(int(time.time())),
})

# 2. Push to queue — processor's worker_loop is BRPOP-ing this
TARGET_PROCESSOR = "processor-1"  # must match PROCESSOR_ID env var of the running container
r.lpush(f"queue:{TARGET_PROCESSOR}", job_id)

print(f"Pushed job: {job_id} to queue:{TARGET_PROCESSOR}")
print(f"Watch processor logs — should see 'Picked up job: {job_id}'")
print(f"Poll result with: redis-cli HGETALL job:{job_id}")

# 3. Poll for completion
for _ in range(30):
    data = r.hgetall(f"job:{job_id}")
    status = data.get("status")
    stage = data.get("stage", "-")
    progress = data.get("progress", "-")
    print(f"  status={status} stage={stage} progress={progress}")
    if status in ("done", "failed"):
        break
    time.sleep(1)

print("\nFinal job hash:")
for k, v in r.hgetall(f"job:{job_id}").items():
    print(f"  {k}: {v}")