# PixelRouter 

> Scalable Hybrid Cloud Image Processing Platform

A production-grade distributed system that processes images using AI
(background removal + captioning) and routes jobs intelligently across
local Docker containers and GCP Cloud Run using a custom CPU-aware
load balancer — all monitored via a real-time admin dashboard.

## Architecture

| Service | Port | Tech |
|---------|------|------|
| Upload Service | 8000 | FastAPI, Redis, GCS |
| Load Balancer | 8001 | FastAPI, Redis, Docker SDK |
| Processor (×2 local) | 8002, 8003 | FastAPI, rembg, BLIP, psutil |
| Processor (GCP) | Cloud Run | Same image, cloud deployment |
| Dashboard | 8501 | Streamlit, Plotly |
| Redis | 6379 | Job state, queue, metrics |

## Quick Start

```bash
cp .env.example .env          # fill in your GCP credentials
make build                    # build all images
make up                       # start all services
make logs                     # follow logs
```

## Key Features

- CPU-aware load balancer routing jobs based on live utilization
- Hybrid cloud: local Docker + GCP Cloud Run in same routing pool
- Real-time WebSocket progress streaming per job
- Redis job queue with Dead Letter Queue for failed jobs
- Auto-scaling via Docker SDK when CPU exceeds 80%
- Live monitoring dashboard with per-processor metrics

## Tech Stack

Python · FastAPI · Docker · Docker Compose · Redis ·
GCP Cloud Run · Google Cloud Storage · rembg · BLIP ·
Streamlit · Plotly · psutil · locust

## Status

   Under active development
- [x] Project scaffolded
- [ ] Upload service — file handling + GCS
- [ ] Load balancer — CPU-aware routing
- [ ] Processor — rembg + BLIP pipeline
- [ ] Dashboard — real-time monitoring
- [ ] GCP Cloud Run deployment
