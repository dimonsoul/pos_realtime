"""
Task 3: Live Analytics Dashboard & Secure Deployment
Streamlit dashboard สำหรับ pos_transactions:
 - แสดงยอดขายรวม / จำนวนธุรกรรม / จำนวน anomaly
 - คำนวณ Moving Average (ยอดขายเฉลี่ยเคลื่อนที่) เป็นกราฟเทียบกับยอดขายจริง
 - ไฮไลต์ธุรกรรมที่เป็น Anomaly (negative amount / duplicate id ที่ระบบตรวจพบ)
"""
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="POS Real-Time Anomaly Dashboard", layout="wide", page_icon="🛒")


def _get_db_url() -> str:
    # บน Streamlit Community Cloud ให้ตั้งค่าผ่าน st.secrets["POS_DB_URL"]
    # บนเครื่อง local ใช้ environment variable POS_DB_URL ได้เช่นกัน
    try:
        if "POS_DB_URL" in st.secrets:
            return st.secrets["POS_DB_URL"]
    except Exception:
        pass
    return os.environ.get(
        "POS_DB_URL",
        "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow",
    )


DB_URL = _get_db_url()
MOVING_AVG_WINDOW = 5  # จำนวนแถวที่ใช้คำนวณ moving average


@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


def load_data() -> pd.DataFrame:
    engine = get_engine()
    query = """
        SELECT transaction_id, store_id, event_timestamp, ingested_at,
               product_name, quantity, unit_price, total_amount,
               is_anomaly, anomaly_reason, source_file
        FROM pos_transactions
        ORDER BY event_timestamp
    """
    try:
        df = pd.read_sql(query, engine)
    except Exception as exc:
        st.error(f"เชื่อมต่อฐานข้อมูลไม่สำเร็จ: {exc}")
        return pd.DataFrame()
    if not df.empty:
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
        df['moving_avg'] = df['total_amount'].rolling(window=MOVING_AVG_WINDOW, min_periods=1).mean()
    return df


st.title("🛒 Real-Time POS Anomaly Detection Dashboard")
st.caption("ข้อมูลจาก Airflow Resilient Ingestion Pipeline (pos_transactions) — รีเฟรชอัตโนมัติทุก 15 วินาที")

st_autorefresh(interval=15_000, key="pos_dashboard_refresh")

df = load_data()

if df.empty:
    st.warning("ยังไม่มีข้อมูลใน pos_transactions — รอ Airflow DAG (pos_resilient_pipeline) ประมวลผลไฟล์ก่อน")
    st.stop()

# --- Sidebar filters ---
st.sidebar.header("ตัวกรอง")
stores = sorted(df['store_id'].dropna().unique().tolist())
selected_stores = st.sidebar.multiselect("สาขา (Store)", stores, default=stores)
show_anomaly_only = st.sidebar.checkbox("แสดงเฉพาะ Anomaly", value=False)

filtered = df[df['store_id'].isin(selected_stores)] if selected_stores else df
if show_anomaly_only:
    filtered = filtered[filtered['is_anomaly']]

# --- KPI row ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("ยอดขายรวม (บาท)", f"{filtered['total_amount'].sum():,.2f}")
col2.metric("จำนวนธุรกรรม", f"{len(filtered):,}")
anomaly_count = int(filtered['is_anomaly'].sum())
col3.metric("Anomaly ที่พบ", f"{anomaly_count:,}", delta=None)
avg_ticket = filtered['total_amount'].mean() if len(filtered) else 0
col4.metric("มูลค่าเฉลี่ย/ธุรกรรม", f"{avg_ticket:,.2f}")

st.divider()

# --- Moving average chart with anomaly highlight ---
st.subheader(f"ยอดขายรายธุรกรรม vs Moving Average (window={MOVING_AVG_WINDOW})")
fig = go.Figure()
fig.add_trace(go.Scatter(
    x=filtered['event_timestamp'], y=filtered['total_amount'],
    mode='lines', name='Total Amount', line=dict(color='#4C9AFF', width=1),
))
fig.add_trace(go.Scatter(
    x=filtered['event_timestamp'], y=filtered['moving_avg'],
    mode='lines', name=f'Moving Avg ({MOVING_AVG_WINDOW})', line=dict(color='#FF8B00', width=2),
))
anomalies = filtered[filtered['is_anomaly']]
if not anomalies.empty:
    fig.add_trace(go.Scatter(
        x=anomalies['event_timestamp'], y=anomalies['total_amount'],
        mode='markers', name='Anomaly', marker=dict(color='#DE350B', size=10, symbol='x'),
    ))
fig.update_layout(height=420, xaxis_title="Event Time", yaxis_title="Total Amount (THB)", legend=dict(orientation="h"))
st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- Anomaly table ---
st.subheader("🚨 ธุรกรรมที่ตรวจพบว่าเป็น Anomaly")
if anomalies.empty:
    st.info("ไม่พบ Anomaly ในข้อมูลชุดปัจจุบัน")
else:
    st.dataframe(
        anomalies[['transaction_id', 'store_id', 'event_timestamp', 'total_amount', 'anomaly_reason', 'source_file']]
        .sort_values('event_timestamp', ascending=False),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("รายการธุรกรรมล่าสุด")


def _highlight_anomaly(row):
    return ['background-color: #FDECEA' if row['is_anomaly'] else '' for _ in row]


recent = filtered.sort_values('event_timestamp', ascending=False).head(50)
st.dataframe(
    recent[['transaction_id', 'store_id', 'event_timestamp', 'product_name', 'quantity', 'total_amount', 'is_anomaly']]
    .style.apply(_highlight_anomaly, axis=1),
    use_container_width=True,
    hide_index=True,
)
