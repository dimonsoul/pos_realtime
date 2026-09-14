"""
Task 3: Live Analytics Dashboard & Secure Deployment
Streamlit dashboard สำหรับ pos_transactions:
 - แสดงยอดขายรวม / จำนวนธุรกรรม / จำนวน anomaly
 - คำนวณ Moving Average (ยอดขายเฉลี่ยเคลื่อนที่) เป็นกราฟเทียบกับยอดขายจริง
 - ไฮไลต์ธุรกรรมที่เป็น Anomaly (negative amount / duplicate id ที่ระบบตรวจพบ)

จัดทำโดย นายมาหะมะ กาลาแต (6910120019)
"""
import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

AUTHOR_NAME = "นายมาหะมะ กาลาแต"
AUTHOR_ID = "6910120019"

# --- Validated data-viz palette (see dataviz skill: references/palette.md) ---
C_SURFACE = "#fcfcfb"
C_PAGE = "#f9f9f7"
C_INK = "#0b0b0b"
C_INK_SECONDARY = "#52514e"
C_MUTED = "#898781"
C_GRID = "#e1e0d9"
C_BORDER = "rgba(11,11,11,0.10)"

C_BLUE = "#2a78d6"      # categorical slot 1 — primary series / neutral KPI
C_ORANGE = "#eb6834"    # categorical slot 2 — moving average / secondary series
C_AQUA = "#1baf7a"      # categorical slot 3 — tertiary series (store breakdown)
C_CRITICAL = "#d03b3b"  # status: critical — anomalies
C_GOOD = "#0ca30c"      # status: good

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


# ----------------------------------------------------------------------------
# Styling — production-style KPI cards, header, and footer
# ----------------------------------------------------------------------------
st.markdown(f"""
<style>
    .block-container {{ padding-top: 1.6rem; padding-bottom: 2rem; max-width: 1280px; }}
    #MainMenu, footer {{ visibility: hidden; }}

    .app-header {{
        display: flex; justify-content: space-between; align-items: flex-start;
        margin-bottom: 0.25rem; gap: 1rem; flex-wrap: wrap;
    }}
    .app-title {{ font-size: 1.7rem; font-weight: 700; color: {C_INK}; margin: 0; line-height: 1.25; }}
    .app-subtitle {{ font-size: 0.92rem; color: {C_INK_SECONDARY}; margin-top: 0.2rem; }}
    .author-badge {{
        background: {C_SURFACE}; border: 1px solid {C_BORDER}; border-radius: 999px;
        padding: 0.4rem 0.9rem; font-size: 0.8rem; color: {C_INK_SECONDARY};
        white-space: nowrap; box-shadow: 0 1px 2px rgba(11,11,11,0.04);
    }}
    .author-badge b {{ color: {C_INK}; }}

    .kpi-card {{
        background: {C_SURFACE}; border: 1px solid {C_BORDER}; border-radius: 14px;
        padding: 1.1rem 1.25rem; box-shadow: 0 1px 3px rgba(11,11,11,0.06);
        height: 100%;
    }}
    .kpi-top {{ display: flex; align-items: center; gap: 0.55rem; margin-bottom: 0.5rem; }}
    .kpi-icon {{
        width: 34px; height: 34px; border-radius: 10px; display: flex;
        align-items: center; justify-content: center; font-size: 1.05rem; flex-shrink: 0;
    }}
    .kpi-label {{ font-size: 0.82rem; color: {C_MUTED}; font-weight: 600; }}
    .kpi-value {{ font-size: 1.65rem; font-weight: 700; color: {C_INK}; font-variant-numeric: tabular-nums; }}
    .kpi-sub {{ font-size: 0.78rem; color: {C_MUTED}; margin-top: 0.15rem; }}

    .section-caption {{ color: {C_MUTED}; font-size: 0.85rem; margin-top: -0.4rem; margin-bottom: 0.6rem; }}

    .app-footer {{
        margin-top: 2rem; padding-top: 1rem; border-top: 1px solid {C_GRID};
        font-size: 0.78rem; color: {C_MUTED}; display: flex; justify-content: space-between;
        flex-wrap: wrap; gap: 0.5rem;
    }}
</style>
""", unsafe_allow_html=True)


def kpi_card(icon: str, icon_bg: str, icon_fg: str, label: str, value: str, sub: str = ""):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-top">
            <div class="kpi-icon" style="background:{icon_bg}; color:{icon_fg};">{icon}</div>
            <div class="kpi-label">{label}</div>
        </div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>
    """, unsafe_allow_html=True)


# ----------------------------------------------------------------------------
# Header
# ----------------------------------------------------------------------------
st.markdown(f"""
<div class="app-header">
    <div>
        <p class="app-title">🛒 Real-Time POS Anomaly Detection</p>
        <p class="app-subtitle">Event-driven ingestion (Apache Airflow) → PostgreSQL → live monitoring — รีเฟรชอัตโนมัติทุก 15 วินาที</p>
    </div>
    <div class="author-badge">จัดทำโดย <b>{AUTHOR_NAME}</b> · รหัสนักศึกษา {AUTHOR_ID}</div>
</div>
""", unsafe_allow_html=True)

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
st.sidebar.divider()
st.sidebar.caption(f"อัปเดตล่าสุด: {datetime.now().strftime('%H:%M:%S')}")
st.sidebar.caption(f"{AUTHOR_NAME}\nรหัสนักศึกษา {AUTHOR_ID}")

filtered = df[df['store_id'].isin(selected_stores)] if selected_stores else df
if show_anomaly_only:
    filtered = filtered[filtered['is_anomaly']]

anomalies = filtered[filtered['is_anomaly']]
anomaly_count = int(filtered['is_anomaly'].sum())
avg_ticket = filtered['total_amount'].mean() if len(filtered) else 0
anomaly_rate = (anomaly_count / len(filtered) * 100) if len(filtered) else 0

# --- KPI row ---
k1, k2, k3, k4 = st.columns(4)
with k1:
    kpi_card("💰", "#eaf2fc", C_BLUE, "ยอดขายรวม (บาท)", f"{filtered['total_amount'].sum():,.2f}")
with k2:
    kpi_card("🧾", "#eaf2fc", C_BLUE, "จำนวนธุรกรรม", f"{len(filtered):,}")
with k3:
    kpi_card("🚨", "#fbeaea", C_CRITICAL, "Anomaly ที่พบ", f"{anomaly_count:,}", f"{anomaly_rate:.1f}% ของธุรกรรมทั้งหมด")
with k4:
    kpi_card("💳", "#eaf2fc", C_BLUE, "มูลค่าเฉลี่ย/ธุรกรรม", f"{avg_ticket:,.2f}")

st.write("")

# ----------------------------------------------------------------------------
# Tabs — Overview / Anomalies / Transactions
# ----------------------------------------------------------------------------
tab_overview, tab_anomaly, tab_tx = st.tabs(["📊 ภาพรวม", "🚨 Anomaly", "📋 ธุรกรรมทั้งหมด"])

with tab_overview:
    st.markdown(f"##### ยอดขายรายธุรกรรม เทียบกับ Moving Average (window={MOVING_AVG_WINDOW})")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=filtered['event_timestamp'], y=filtered['total_amount'],
        mode='lines', name='ยอดขายจริง', line=dict(color=C_BLUE, width=2),
    ))
    fig.add_trace(go.Scatter(
        x=filtered['event_timestamp'], y=filtered['moving_avg'],
        mode='lines', name=f'Moving Avg ({MOVING_AVG_WINDOW})', line=dict(color=C_ORANGE, width=2),
    ))
    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies['event_timestamp'], y=anomalies['total_amount'],
            mode='markers', name='Anomaly', marker=dict(color=C_CRITICAL, size=10, symbol='x', line=dict(width=2)),
        ))
    fig.update_layout(
        height=420, hovermode="x unified",
        plot_bgcolor=C_SURFACE, paper_bgcolor=C_SURFACE,
        xaxis=dict(title="Event Time", gridcolor=C_GRID, linecolor=C_GRID),
        yaxis=dict(title="Total Amount (THB)", gridcolor=C_GRID, linecolor=C_GRID),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(t=10, l=10, r=10, b=10),
        font=dict(color=C_INK_SECONDARY),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("##### ยอดขายแยกตามสาขา")
    store_summary = filtered.groupby('store_id').agg(
        total_amount=('total_amount', 'sum'),
        transactions=('transaction_id', 'count'),
    ).reset_index().sort_values('total_amount', ascending=True)
    store_colors = [C_BLUE, C_ORANGE, C_AQUA]
    fig_store = go.Figure(go.Bar(
        x=store_summary['total_amount'], y=store_summary['store_id'], orientation='h',
        marker=dict(color=[store_colors[i % len(store_colors)] for i in range(len(store_summary))]),
        text=store_summary['total_amount'].map(lambda v: f"{v:,.0f}"), textposition='outside',
        hovertemplate="สาขา %{y}<br>ยอดขาย %{x:,.2f} บาท<extra></extra>",
    ))
    fig_store.update_layout(
        height=220, plot_bgcolor=C_SURFACE, paper_bgcolor=C_SURFACE,
        xaxis=dict(title="ยอดขายรวม (บาท)", gridcolor=C_GRID, linecolor=C_GRID),
        yaxis=dict(title=None, gridcolor=C_GRID, linecolor=C_GRID),
        margin=dict(t=10, l=10, r=40, b=10), showlegend=False,
        font=dict(color=C_INK_SECONDARY),
    )
    st.plotly_chart(fig_store, use_container_width=True)

with tab_anomaly:
    if anomalies.empty:
        st.info("ไม่พบ Anomaly ในข้อมูลชุดปัจจุบัน")
    else:
        reason_counts = anomalies['anomaly_reason'].value_counts()
        rc1, rc2 = st.columns([1, 2])
        with rc1:
            for reason, count in reason_counts.items():
                st.markdown(f"""
                <div class="kpi-card" style="margin-bottom:0.6rem;">
                    <div class="kpi-label">{reason or 'unspecified'}</div>
                    <div class="kpi-value" style="color:{C_CRITICAL};">{count:,}</div>
                </div>
                """, unsafe_allow_html=True)
        with rc2:
            st.markdown("###### รายละเอียดธุรกรรม Anomaly")
            st.dataframe(
                anomalies[['transaction_id', 'store_id', 'event_timestamp', 'total_amount', 'anomaly_reason', 'source_file']]
                .sort_values('event_timestamp', ascending=False),
                use_container_width=True, hide_index=True, height=360,
            )

with tab_tx:
    def _highlight_anomaly(row):
        return [f'background-color: #fbeaea' if row['is_anomaly'] else '' for _ in row]

    recent = filtered.sort_values('event_timestamp', ascending=False).head(100)
    st.markdown(f'<p class="section-caption">แสดง {len(recent):,} รายการล่าสุด จากทั้งหมด {len(filtered):,} รายการ — แถวสีชมพูคือ Anomaly</p>', unsafe_allow_html=True)
    st.dataframe(
        recent[['transaction_id', 'store_id', 'event_timestamp', 'product_name', 'quantity', 'total_amount', 'is_anomaly']]
        .style.apply(_highlight_anomaly, axis=1),
        use_container_width=True, hide_index=True, height=460,
    )

# ----------------------------------------------------------------------------
# Footer
# ----------------------------------------------------------------------------
st.markdown(f"""
<div class="app-footer">
    <div>Powered by Apache Airflow · PostgreSQL · Streamlit — 240-513 Data Engineering Principles and Applications</div>
    <div>จัดทำโดย {AUTHOR_NAME} (รหัสนักศึกษา {AUTHOR_ID})</div>
</div>
""", unsafe_allow_html=True)
