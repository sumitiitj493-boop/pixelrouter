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
import pandas as pd
import plotly.express as px
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

st.subheader("CPU Utilization")
if processors:
    # Enforce numeric types for Plotly to render the Y-axis correctly
    df_processors = pd.DataFrame(processors)
    df_processors["cpu_percent"] = pd.to_numeric(df_processors["cpu_percent"], errors="coerce").fillna(0)
    
    fig = px.bar(
        df_processors,
        x="processor_id",
        y="cpu_percent",
        color="status",
        title="Live Processor CPU Load",
        labels={"cpu_percent": "CPU Usage (%)", "processor_id": "Processor Node"},
        range_y=[0, 100]
    )
    
    # Overlay the load balancer scaling threshold for visual context
    max_threshold = scaling_status_data.get("max_cpu_threshold", 80.0)
    fig.add_hline(
        y=max_threshold,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Autoscale Threshold ({max_threshold}%)"
    )
    
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No CPU telemetry available.")

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

st.subheader("Real-time Job Queue")
jobs_data = fetch_jobs(redis_client)

if jobs_data:
    df_jobs = pd.DataFrame(jobs_data)
    
    # Ensure all required display columns exist to avoid KeyError if Redis hash is partial
    for col in ["job_id", "status", "processor_id", "route_reason", "created_at"]:
        if col not in df_jobs.columns:
            df_jobs[col] = "—"
            
    # Surface actionable tasks by sorting pending/processing jobs to the top,
    # followed by the most recently created jobs.
    df_jobs["status_priority"] = df_jobs["status"].map({
        "pending": 0,
        "processing": 1,
        "completed": 2,
        "failed": 3
    }).fillna(99)
    
    df_jobs["created_at_num"] = pd.to_numeric(df_jobs["created_at"], errors="coerce").fillna(0.0)
    df_jobs = df_jobs.sort_values(
        by=["status_priority", "created_at_num"], 
        ascending=[True, False]
    )
    
    # Format cleanly for the UI
    display_df = df_jobs[["job_id", "status", "processor_id", "route_reason"]].rename(columns={
        "job_id": "Job ID",
        "status": "Status",
        "processor_id": "Target Processor",
        "route_reason": "Reason"
    })
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)
else:
    st.info("No active or historical jobs found in the queue.")