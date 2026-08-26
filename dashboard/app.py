# PixelRouter — Admin Dashboard
# Responsibility: Real-time monitoring of all processor instances,
#                 job queue status, throughput charts,
#                 manual job routing override.
#
# Tech: Streamlit + Plotly + Redis + httpx
# Polls /metrics on each processor every 2 seconds via st_autorefresh
#
# TODO: Implement CPU/RAM live charts
# TODO: Implement job queue visualization
# TODO: Implement manual routing override

import os
import httpx
import redis
import streamlit as st
from streamlit_autorefresh import st_autorefresh
st.set_page_config(
    page_title="PixelRouter Dashboard",
    layout="wide"
)

st.title(" PixelRouter — Admin Dashboard")
st.caption("Real-time monitoring for hybrid cloud image processing")

# Configure page auto-refresh (2000ms interval) for live updates.
st_autorefresh(interval=2000, limit=None, key="dashboard_autorefresh")

LOAD_BALANCER_URL = os.getenv("LOAD_BALANCER_URL", "http://load-balancer:8001")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

@st.cache_resource
def get_redis_client() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)

redis_client = get_redis_client()


def fetch_processor_status() -> dict:
    try:
        response = httpx.get(f"{LOAD_BALANCER_URL}/processors/status", timeout=2.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as exc:
        st.error(f"Failed to fetch processor status: {exc}")
        return {"processors": []}


def fetch_scaling_status() -> dict:
    try:
        response = httpx.get(f"{LOAD_BALANCER_URL}/scaling/status", timeout=2.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as exc:
        st.error(f"Failed to fetch scaling status: {exc}")
        return {}


def fetch_jobs(r: redis.Redis) -> list[dict]:
    jobs = []
    # Safely iterate over keys without blocking the Redis event loop
    for key in r.scan_iter(match="job:*"):
        job_data = r.hgetall(key)
        if job_data:
            job_data["job_id"] = key.split(":", 1)[1]
            jobs.append(job_data)
    return jobs

# Fetch live data
processor_status_data = fetch_processor_status()
scaling_status_data = fetch_scaling_status()

processors = processor_status_data.get("processors", [])
active_processors = sum(1 for p in processors if p.get("status") == "active")
total_pending_jobs = sum(int(p.get("pending_jobs", 0)) for p in processors)

live_cpus = [float(p.get("cpu_percent")) for p in processors if p.get("cpu_percent") not in ("unknown", None)]
avg_cpu = sum(live_cpus) / len(live_cpus) if live_cpus else 0.0

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Active Processors", f"{active_processors} / {scaling_status_data.get('max_processors', '?')}")
with col2:
    st.metric("Jobs in Queue", str(total_pending_jobs))
with col3:
    st.metric("Avg CPU", f"{avg_cpu:.1f}%")

def get_status_emoji(status: str) -> str:
    if status == "active":
        return "🟢 Active"
    elif status == "stale":
        return "🟡 Stale"
    elif status == "unhealthy":
        return "🔴 Unhealthy"
    return f"⚪ {status.capitalize()}"

st.subheader("Processor Status")
if processors:
    # Build dynamic markdown table
    md_table = "| Processor | CPU % | Pending Jobs | Status |\n|---|---|---|---|\n"
    for p in processors:
        status_display = get_status_emoji(p.get("status", "unknown"))
        md_table += f"| {p.get('processor_id')} | {p.get('cpu_percent')}% | {p.get('pending_jobs')} | {status_display} |\n"
    st.markdown(md_table)
else:
    st.info("No processors registered yet.")