# PixelRouter

> Scalable Hybrid Cloud Image Processing Platform

PixelRouter is an early-stage distributed image processing platform. It is being
designed to accept image uploads, route work across local processors and future
cloud processors, run AI image tasks such as background removal and captioning,
and expose system health through a Streamlit dashboard.

## Architecture

| Service | Port | Tech |
|---------|------|------|
| Upload Service | 8000 | FastAPI, Redis, GCS |
| Load Balancer | 8001 | FastAPI, Redis, httpx, Docker SDK planned |
| Processor (x2 local) | 8002, 8003 | FastAPI, rembg, BLIP, psutil |
| Processor (GCP) | Cloud Run | Same image, cloud deployment planned |
| Dashboard | 8501 | Streamlit, Plotly |
| Redis | 6379 | Job state, queue, metrics |

## Quick Start

```bash
cp .env.example .env          # fill in your GCP credentials if needed
make build                    # build all images
make up                       # start all services
make logs                     # follow logs
```

To view only the current dashboard scaffold:

```bash
docker compose up dashboard redis
```

Then open `http://localhost:8501`.

## Current Load Balancer Behavior

- Reads live processor metrics from Redis keys like `metrics:processor-1:cpu`
  and `metrics:processor-1:pending`.
- Maintains a Redis processor registry with processor ID, URL, type, status,
  creation time, and last metrics timestamp.
- Bootstraps the registry from `PROCESSOR_URLS` and optional
  `CLOUD_RUN_PROCESSOR_URL`, so future autoscaled processors can join the same
  routing path.
- Refreshes registered local processors before routing and marks failed metric
  polls as `stale`.
- Excludes Cloud Run from normal metric refresh until cloud fallback is used.
- Ignores processors that do not have live CPU metrics.
- Detects overload only across live local processors. If at least one live local
  processor is below `MAX_CPU_THRESHOLD`, routing stays local and no scaling is
  requested.
- Selects the processor with the lowest pending job count; CPU percentage is the
  tiebreaker.
- Does not increment `pending_jobs` when `/route` is called. Pending count should
  increase only after a processor actually accepts or claims the job.
- Uses a thread-safe `update_pending_count()` helper with Redis `INCRBY`, clamps
  negative counts to `0`, and refreshes the metrics TTL.
- If all live processors exceed `MAX_CPU_THRESHOLD`, sets an
  `autoscale:requested` Redis flag. Actual Docker SDK / Cloud Run scaling is
  still planned.

## Load Balancer Config

| Variable | Default | Purpose |
|----------|---------|---------|
| `REDIS_URL` | `redis://redis:6379` | Redis connection used for metrics and routing state |
| `PROCESSOR_URLS` | `http://processor-1:8002,http://processor-2:8003` | Initial local processor pool |
| `MAX_CPU_THRESHOLD` | `80` | CPU percentage where a live processor is considered overloaded |
| `METRICS_REFRESH_TIMEOUT_SECONDS` | `2` | Timeout for polling each processor's `/metrics` endpoint |
| `LOCAL_AUTOSCALE_ENABLED` | `true` | Feature flag for future Docker SDK local autoscaling |
| `MAX_PROCESSORS` | `5` | Maximum local processor containers allowed |
| `PROCESSOR_BASE_PORT` | `8002` | First local processor port for dynamic processor naming/ports |
| `PROCESSOR_IMAGE` | `pixelrouter-processor:latest` | Docker image future autoscaling should launch |
| `PROCESSOR_NETWORK` | `pixelrouter_pixelrouter-network` | Docker network future autoscaled processors should join |
| `CLOUD_RUN_PROCESSOR_URL` | empty | Cloud Run processor fallback endpoint |

## Key Features

- CPU-aware load balancer routing based on live processor utilization
- Redis-backed job state, queue, and processor metrics
- Local Docker Compose processor pool
- Planned hybrid cloud support with GCP Cloud Run
- Planned image processing pipeline using rembg and BLIP
- Streamlit dashboard scaffold for future real-time monitoring

## Tech Stack

Python - FastAPI - Docker - Docker Compose - Redis - GCP Cloud Run -
Google Cloud Storage - rembg - BLIP - Streamlit - Plotly - psutil

## Status

Under active development.

- [x] Project scaffolded
- [x] Load balancer CPU-aware router selection
- [x] Redis-backed processor registry
- [x] Thread-safe pending count helper
- [x] Autoscale request signal
- [ ] Processor job claim flow increments/decrements pending counts
- [ ] Upload service file handling and GCS storage
- [ ] Processor rembg + BLIP pipeline
- [ ] Dashboard real-time monitoring
- [ ] GCP Cloud Run deployment

## Tests

```bash
pip install -r requirements-dev.txt
make test
```

Runtime dependencies are kept inside each service folder. The root
`requirements-dev.txt` is only for local development and test tooling.
