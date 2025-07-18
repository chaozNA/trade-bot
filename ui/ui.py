import streamlit as st
import pandas as pd
import json
from datetime import date, timedelta, datetime
from streamlit_autorefresh import st_autorefresh
from commons.db.db_client import db_client
import atexit
import threading

# --- Data Fetching Function ---
@st.cache_data(ttl=30)
def get_data(query, params=()):
    data = db_client.fetchall(query, params)
    return pd.DataFrame(data) if data else pd.DataFrame()

def format_datetime(dt_str):
    if not dt_str:
        return "N/A"
    try:
        return datetime.fromisoformat(dt_str).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return dt_str

# --- Shutdown Handler ---
def shutdown():
    st.write("Shutting down...")
    if 'db_client' in globals() and hasattr(db_client, 'close'):
        db_client.close()
    st.stop()

# --- UI ---
st.title("📊 Auto Trade Dashboard")

# Sidebar for global settings and shutdown
with st.sidebar:
    st.header("Settings")
    refresh_interval = st.slider("Auto-refresh (seconds)", 10, 300, 60, 10)
    if st.button("Shutdown", help="Stop the application"):
        shutdown()
    st_autorefresh(interval=refresh_interval * 1000, key="datarefresh")

# Register shutdown handler
atexit.register(shutdown)

# Overview metrics
st.header("Overview")
metrics_query = """
    SELECT 
        COUNT(CASE WHEN status = 'open' THEN 1 END) as open_trades,
        COUNT(CASE WHEN status = 'closed' THEN 1 END) as closed_trades,
        SUM(CASE WHEN status = 'open' THEN quantity * filled_entry_price ELSE 0 END) as open_value,
        SUM(CASE WHEN status = 'closed' THEN quantity * (COALESCE(filled_exit_price, 0) - COALESCE(filled_entry_price, 0)) ELSE 0 END) as total_pnl
    FROM trades
"""
metrics = get_data(metrics_query)
if not metrics.empty:
    m = metrics.iloc[0]
    cols = st.columns(4)
    cols[0].metric("Open Trades", int(m['open_trades']) if pd.notna(m['open_trades']) else 0)
    cols[1].metric("Closed Trades", int(m['closed_trades']) if pd.notna(m['closed_trades']) else 0)
    cols[2].metric("Open Value", f"${m['open_value']:.2f}" if pd.notna(m['open_value']) else "$0.00")
    cols[3].metric("Total P&L", f"${m['total_pnl']:.2f}" if pd.notna(m['total_pnl']) else "$0.00")

# Tabs
tab_messages, tab_trades, tab_portfolio = st.tabs(["📨 Messages", "📈 Trades", "📊 Portfolio"])

with tab_messages:
    st.subheader("Messages & Analysis")

    # Message filters in sidebar
    with st.sidebar:
        st.subheader("Message Filters")
        msg_start = st.date_input("From", date.today() - timedelta(days=3))
        msg_end = st.date_input("To", date.today())
        msg_status = st.selectbox("Status", ["All", "Analyzed", "Not Analyzed"])
        msg_search = st.text_input("Search Content")

    if msg_start <= msg_end:
        query_parts = ["""
            SELECT m.message_id, m.timestamp, a.name as author_name, c.name as channel_name, m.content,
                   ma.classification, ma.confidence_score, ma.reason, ma.related_trade_id, m.processed
            FROM messages m
            LEFT JOIN authors a ON m.author_id = a.author_id
            LEFT JOIN channels c ON m.channel_id = c.channel_id
            LEFT JOIN message_analyses ma ON m.message_id = ma.message_id
            WHERE date(m.timestamp) BETWEEN ? AND ?
        """]
        params = [msg_start.isoformat(), msg_end.isoformat()]

        if msg_status == "Analyzed":
            query_parts.append("AND ma.analysis_id IS NOT NULL")
        elif msg_status == "Not Analyzed":
            query_parts.append("AND ma.analysis_id IS NULL")

        if msg_search:
            query_parts.append("AND m.content LIKE ?")
            params.append(f"%{msg_search}%")

        query_parts.append("ORDER BY m.timestamp DESC LIMIT 200")

        messages = get_data("\n".join(query_parts), params)

        if not messages.empty:
            messages['timestamp'] = messages['timestamp'].apply(format_datetime)
            messages['content_preview'] = messages['content'].apply(lambda x: x[:100] + '...' if len(x) > 100 else x)
            # Convert to float and fill NA with 0, then infer objects
            messages['confidence_score'] = pd.to_numeric(messages['confidence_score'], errors='coerce').fillna(0).infer_objects(copy=False)
            messages['related_trade_id'] = messages['related_trade_id'].apply(lambda x: f"#{x}" if pd.notna(x) else "-")
            messages['classification'] = messages['classification'].fillna("-")
            messages['reason'] = messages['reason'].fillna("-")

            display_df = messages[["timestamp", "channel_name", "author_name", "content_preview", "classification", "confidence_score", "reason", "related_trade_id"]].rename(columns={
                "timestamp": "Time",
                "channel_name": "Channel",
                "author_name": "Author",
                "content_preview": "Content",
                "classification": "Type",
                "confidence_score": "Conf.",
                "reason": "Reason",
                "related_trade_id": "Trade"
            })

            # Interactive table with row selection
            event = st.dataframe(
                display_df,
                column_config={
                    "Time": st.column_config.TextColumn(width="small"),
                    "Channel": st.column_config.TextColumn(width="medium"),
                    "Author": st.column_config.TextColumn(width="medium"),
                    "Content": st.column_config.TextColumn(width="large"),
                    "Type": st.column_config.TextColumn(width="medium"),
                    "Conf.": st.column_config.ProgressColumn("Conf.", min_value=0, max_value=10, format="%.1f"),
                    "Reason": st.column_config.TextColumn(width="large"),
                    "Trade": st.column_config.TextColumn(width="small")
                },
                use_container_width=True,
                height=400,
                hide_index=True,
                on_select="rerun",
                selection_mode="single-row"
            )

            selected_rows = event.selection["rows"]
            if selected_rows:
                selected_idx = selected_rows[0]
                msg = messages.iloc[selected_idx]
                st.subheader("Selected Message Details")
                with st.container(border=True):
                    st.markdown(f"**ID:** {msg['message_id']} | **Time:** {msg['timestamp']}")
                    st.markdown(f"**Channel:** {msg['channel_name']} | **Author:** {msg['author_name']}")
                    st.markdown("**Content:**")
                    st.code(msg['content'])
                    st.markdown(f"**Processed:** {'Yes' if msg['processed'] else 'No'}")
                    if pd.notna(msg['classification']):
                        cols = st.columns(3)
                        cols[0].metric("Type", msg['classification'])
                        cols[1].metric("Confidence", f"{msg['confidence_score']:.1f}/10")
                        cols[2].metric("Trade", msg['related_trade_id'])
                        st.markdown("**Reason:**")
                        st.write(msg['reason'])
            else:
                st.info("Select a row to view details.")
        else:
            st.info("No messages found.")

with tab_trades:
    st.subheader("Trades")

    # Trade filters in sidebar
    with st.sidebar:
        st.subheader("Trade Filters")
        trade_status = st.selectbox("Status", ["All", "Open", "Closed"])

    query = "SELECT * FROM trades"
    if trade_status != "All":
        query += f" WHERE status = '{trade_status.lower()}'"
    query += " ORDER BY created_at DESC LIMIT 200"

    trades = get_data(query)

    if not trades.empty:
        trades['created_at'] = trades['created_at'].apply(format_datetime)
        trades['opened_at'] = trades['opened_at'].apply(format_datetime)
        trades['closed_at'] = trades['closed_at'].apply(format_datetime)
        # Handle None values in P&L calculation
        trades['pnl'] = trades.apply(
            lambda r: r['quantity'] * (0 if pd.isna(r['filled_exit_price']) else r['filled_exit_price'] - 
                                      (0 if pd.isna(r['filled_entry_price']) else r['filled_entry_price'])) 
            if r['status'] == 'closed' else 0, axis=1)

        display_df = trades[["trade_id", "symbol", "option_type", "strike", "expiration", "status", "quantity", "filled_entry_price", "filled_exit_price", "pnl", "created_at"]].rename(columns={
            "trade_id": "ID",
            "option_type": "Type",
            "filled_entry_price": "Entry",
            "filled_exit_price": "Exit",
            "pnl": "P&L",
            "created_at": "Created"
        })

        # Interactive table with row selection
        event = st.dataframe(
            display_df,
            column_config={
                "ID": st.column_config.NumberColumn(width="small"),
                "symbol": st.column_config.TextColumn(width="medium"),
                "Type": st.column_config.TextColumn(width="small"),
                "strike": st.column_config.NumberColumn(format="%.2f", width="small"),
                "expiration": st.column_config.DateColumn(width="medium"),
                "status": st.column_config.TextColumn(width="small"),
                "quantity": st.column_config.NumberColumn(format="%.0f", width="small"),
                "Entry": st.column_config.NumberColumn(format="%.2f", width="small"),
                "Exit": st.column_config.NumberColumn(format="%.2f", width="small"),
                "P&L": st.column_config.NumberColumn(format="$%.2f", width="small"),
                "Created": st.column_config.TextColumn(width="medium")
            },
            use_container_width=True,
            height=400,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row"
        )

        selected_rows = event.selection["rows"]
        if selected_rows:
            selected_idx = selected_rows[0]
            trade = trades.iloc[selected_idx]
            st.subheader("Selected Trade Details")
            with st.container(border=True):
                cols = st.columns(2)
                with cols[0]:
                    st.markdown(f"**Symbol:** {trade['symbol']}")
                    st.markdown(f"**Type:** {trade['option_type']}")
                    st.markdown(f"**Strike:** ${trade['strike']:.2f}")
                    st.markdown(f"**Expiration:** {trade['expiration']}")
                    st.markdown(f"**Status:** {trade['status']}")
                with cols[1]:
                    st.markdown(f"**Quantity:** {trade['quantity']:.0f}")
                    st.markdown(f"**Entry Price:** ${trade['filled_entry_price']:.2f}")
                    if pd.notna(trade['filled_exit_price']):
                        st.markdown(f"**Exit Price:** ${trade['filled_exit_price']:.2f}")
                    if trade['status'] == 'closed':
                        st.markdown(f"**P&L:** ${trade['pnl']:.2f}")
                    st.markdown(f"**Created:** {trade['created_at']}")
                if pd.notna(trade['opened_at']):
                    st.markdown(f"**Opened:** {trade['opened_at']}")
                if pd.notna(trade['closed_at']):
                    st.markdown(f"**Closed:** {trade['closed_at']}")

                st.markdown("**History:**")
                history = get_data("SELECT * FROM trade_history WHERE trade_id = ? ORDER BY timestamp", (trade['trade_id'],))
                if not history.empty:
                    history['timestamp'] = history['timestamp'].apply(format_datetime)
                    for _, h in history.iterrows():
                        details = json.loads(h['details']) if h['details'] else {}
                        st.json({
                            "Event": h['event_type'],
                            "Time": h['timestamp'],
                            "Details": details
                        })
                else:
                    st.info("No history entries.")
        else:
            st.info("Select a row to view details.")
    else:
        st.info("No trades found.")

with tab_portfolio:
    st.subheader("Portfolio Summary")

    # Enhanced metrics
    st.subheader("Metrics")
    cols = st.columns(3)
    cols[0].metric("Total Trades", len(get_data("SELECT * FROM trades")))
    # Add more if needed

    # P&L Chart
    st.subheader("Cumulative P&L")
    closed_trades = get_data("SELECT closed_at, quantity * (COALESCE(filled_exit_price, 0) - COALESCE(filled_entry_price, 0)) as pnl FROM trades WHERE status = 'closed' ORDER BY closed_at")
    if not closed_trades.empty:
        closed_trades['closed_at'] = pd.to_datetime(closed_trades['closed_at']).dt.date
        pnl_by_date = closed_trades.groupby('closed_at')['pnl'].sum().cumsum().reset_index(name='Cumulative P&L')
        st.line_chart(pnl_by_date.set_index('closed_at'))

    # Recent Trades
    st.subheader("Recent Trades")
    recent_trades = get_data("SELECT * FROM trades ORDER BY created_at DESC LIMIT 50")
    if not recent_trades.empty:
        st.dataframe(recent_trades, use_container_width=True, hide_index=True)
    else:
        st.info("No trades.")