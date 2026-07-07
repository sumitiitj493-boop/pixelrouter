# PixelRouter

> Scalable Hybrid Cloud Image Processing Platform

PixelRouter is a distributed image processing platform designed for high availability and elastic scaling. It accepts image uploads, routes workloads across a fleet of local and cloud processors, and executes AI-driven image tasks (background removal, captioning).

## Architecture

| Service | Port | Tech Stack |
|---------|------|------------|
| Upload Service | 8000 | FastAPI, Redis, GCS |
| Load Balancer | 8001 | FastAPI, Redis, httpx, Docker SDK |
| Processor (Local) | 8002+ | FastAPI, rembg, BLIP, psutil |
| Processor (Cloud) | Cloud Run | GCP Cloud Run |
| Dashboard | 8501 | Streamlit, Plotly |
| Redis | 6379 | State Management, Queueing, Metrics |

## Quick Start

```bash
cp .env.example .env          # Configure GCP credentials & environment
make build                    # Build Docker images
make up                       # Start local environment
```
Access the dashboard at `http://localhost:8501`.

## Key Features

- **CPU-Aware Routing:** Distributes jobs dynamically based on live processor utilization (lowest pending jobs, then lowest CPU usage) to prevent bottlenecks.
- **Fault Tolerance & Auto-Recovery:** 
  - Actively polls processor health via `/metrics`. 
  - Marks processors as `unhealthy` after `MAX_FAILED_POLLS` consecutive failures.
  - Automatically and asynchronously requeues orphaned jobs from dead processors to healthy ones without blocking incoming traffic.
  - Processors automatically recover to `active` status once they become responsive again.
- **Hybrid Autoscaling:** Seamlessly scales up local Docker processors under heavy load, and falls back to GCP Cloud Run when local capacity reaches `MAX_PROCESSORS`.
- **Robust Storage:** Securely uploads assets to Google Cloud Storage (GCS) using deterministic keys and short-lived signed URLs.
- **Redis-Backed State:** Utilizes Redis for a centralized processor registry, atomic counter updates (`INCRBY`), job state management, and real-time metrics.

## Configuration Highlights (Load Balancer)

| Variable | Default | Purpose |
|----------|---------|---------|
| `MAX_CPU_THRESHOLD` | `80` | CPU utilization threshold triggering overload protocols |
| `MAX_PROCESSORS` | `5` | Maximum allowed local processor containers |
| `MAX_FAILED_POLLS` | `3` | Consecutive failed health checks before a processor is marked unhealthy |
| `CLOUD_RUN_PROCESSOR_URL` | empty | GCP Cloud Run endpoint for cloud fallback |

## Project Status

Under active development.

- [x] Load balancer CPU-aware routing & Redis registry
- [x] Docker SDK local autoscaling & Cloud fallback
- [x] Upload service GCS integration & signed URLs
- [x] Processor fault tolerance & asynchronous orphan requeuing
- [ ] Processor image pipeline (rembg + BLIP)
- [ ] Dashboard real-time monitoring

## Testing

Unit tests extensively mock the Docker SDK and Redis to validate routing, autoscaling, and requeue logic without requiring a container stack.
```bash
pip install -r requirements-dev.txt
make test
make smoke-test # Validates real processor registration against running containers
```
