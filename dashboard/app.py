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

import streamlit as st

st.set_page_config(
    page_title="PixelRouter Dashboard",
    layout="wide"
)

st.title(" PixelRouter — Admin Dashboard")
st.caption("Real-time monitoring for hybrid cloud image processing")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Active Processors", "2", delta="1 on GCP")
with col2:
    st.metric("Jobs in Queue", "—", delta=None)
with col3:
    st.metric("Avg CPU", "—", delta=None)

st.info("Dashboard implementation begins on Upcoming Days. "
        "Infrastructure and services will be ready by then.")

st.subheader("Processor Status")
st.markdown("| Processor     | CPU % | Pending Jobs | Status     |")
st.markdown("|---------------|-------|--------------|------------|")
st.markdown("| processor-1   | —     | —            | Starting   |")
st.markdown("| processor-2   | —     | —            | Starting   |")
st.markdown("| GCP Cloud Run | —     | —            | Cloud      |")