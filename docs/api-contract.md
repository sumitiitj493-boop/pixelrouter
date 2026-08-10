# API Contract & System Design Decisions

## 1. Job Hash (Redis)
Key: `job:{job_id}`
Type: Hash

### Fields:
- `status`: "pending" | "processing" | "done" | "failed"
- `image_url`: gs:// path to the uploaded image (or original filename)
- `processor_id`: ID of the processor assigned to handle this job
- `created_at`: Unix timestamp of creation
- `stage`: Current processing stage
- `progress`: 0-100 progress indicator
- `caption`: Final generated caption
- `error`: Error message if status is "failed"

## 2. Queue Naming Convention
We are using a **Per-Processor Queue** approach.
The Load Balancer computes the `processor_id` and the Upload Service pushes the job to a specific queue.

Key: `queue:{processor_id}`
Type: List
Value: `job_id`

### Flow:
1. Upload Service calls Load Balancer `/route`.
2. Load Balancer returns `{"processor_id": "processor-1", ...}`.
3. Upload Service sets `processor_id: "processor-1"` in `job:{job_id}` hash.
4. Upload Service executes `LPUSH queue:processor-1 <job_id>`.
5. Processor 1 executes `BRPOP queue:processor-1` to claim and process the job.
