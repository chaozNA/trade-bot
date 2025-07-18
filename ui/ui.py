import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from streamlit_autorefresh import st_autorefresh
from commons.mongodb.mongodb_client import MongoDBClient
from utils.logger import get_logger
from dotenv import load_dotenv
import os
import json

# Set wide layout
st.set_page_config(layout="wide", page_title="Trade Bot Messages")

load_dotenv()
logger = get_logger("ui")

MESSAGES_COLLECTION = os.getenv("MESSAGES_COLLECTION", "raw_messages")
ANALYZED_ACTIONS_COLLECTION = os.getenv("ANALYZED_ACTIONS_COLLECTION", "analyzed_actions")

# Initialize MongoDB client
mongo_client = MongoDBClient()

# --- Data Fetching Function ---
@st.cache_data(ttl=30)
def get_messages(start_date, end_date, status, search):
    try:
        raw_messages = mongo_client.get_collection(MESSAGES_COLLECTION)
        analyzed_actions = mongo_client.get_collection(ANALYZED_ACTIONS_COLLECTION)

        query = {
            "timestamp": {
                "$gte": start_date.isoformat(),
                "$lte": end_date.isoformat() + "T23:59:59"
            }
        }
        if search:
            query["content"] = {"$regex": search, "$options": "i"}

        messages = list(raw_messages.find(query).sort("timestamp", -1).limit(200))
        message_ids = [msg["message_id"] for msg in messages]
        analyses = list(analyzed_actions.find({"message_id": {"$in": message_ids}}))
        analyses_dict = {a["message_id"]: a for a in analyses}

        data = []
        for msg in messages:
            msg_id = msg["message_id"]
            analysis = analyses_dict.get(msg_id, {})
            data.append({
                "message_id": str(msg_id),
                "timestamp": msg["timestamp"],
                "channel_name": msg["channel_name"],
                "author_name": msg["author_name"],
                "content": msg["content"],
                "processed": msg.get("processed", False),
                "action_type": analysis.get("action_type", "-"),
                "confidence_score": float(analysis.get("confidence_score", 0.0)),
                "reason": analysis.get("reason", "-"),
                "related_trade_id": f"#{analysis['related_trade_id']}" if analysis.get("related_trade_id") else "-",
                "analysis_id": str(analysis.get("analysis_id", "-")),
                "analysis_payload": json.dumps(analysis.get("analysis_payload", {}), indent=2),
                "analysis_timestamp": analysis.get("analysis_timestamp", "-"),
                "has_analysis": msg_id in analyses_dict
            })

        df = pd.DataFrame(data)
        if not df.empty:
            if status == "Analyzed":
                df = df[df["has_analysis"] == True].reset_index(drop=True)
            elif status == "Not Analyzed":
                df = df[df["has_analysis"] == False].reset_index(drop=True)
        return df
    except Exception as e:
        logger.error(f"Error fetching messages: {e}")
        return pd.DataFrame()

# --- Fetch Message Chain ---
@st.cache_data(ttl=30)
def get_message_chain(message_id):
    try:
        message_id = str(message_id)
        message_id_int = int(message_id)
        raw_messages = mongo_client.get_collection(MESSAGES_COLLECTION)

        # Find the root by traversing upwards
        current_id = message_id_int
        parent_chain_ids = []
        visited = set()
        while current_id and current_id not in visited:
            visited.add(current_id)
            parent_chain_ids.append(current_id)
            msg = raw_messages.find_one({"message_id": current_id})
            if not msg:
                break
            current_id = msg.get("parent_id")

        if not parent_chain_ids:
            return pd.DataFrame()

        root_id = parent_chain_ids[-1]  # Last ID in the chain is the root

        # Now traverse the entire thread from the root downwards
        chain = []
        visited = set()

        def add_message(msg_id, parent_id=None, level=0):
            if msg_id in visited:
                return
            visited.add(msg_id)
            msg = raw_messages.find_one({"message_id": msg_id})
            if not msg:
                return
            is_selected = (msg_id == message_id_int)
            t = "Root" if level == 0 else f"Reply (Level {level})"
            if is_selected:
                t += " (Selected)"
            chain.append({
                "message_id": str(msg_id),
                "timestamp": msg["timestamp"],
                "author_name": msg["author_name"],
                "channel_name": msg["channel_name"],
                "content": msg["content"],
                "parent_id": str(parent_id) if parent_id is not None else "-",
                "type": t,
                "level": level
            })
            # Fetch and add replies, sorted by timestamp
            replies = list(raw_messages.find({"parent_id": msg_id}).sort("timestamp", 1))
            for reply in replies:
                add_message(reply["message_id"], msg_id, level + 1)

        # Start from root
        add_message(root_id)

        df = pd.DataFrame(chain)
        if not df.empty:
            df = df.sort_values("timestamp")  # Sort chronologically
            df["indent_content"] = df.apply(
                lambda row: "\u00a0" * (row["level"] * 4) + (row["content"][:80] + "..." if len(row["content"]) > 80 else row["content"]),
                axis=1
            )
        return df
    except Exception as e:
        logger.error(f"Error fetching message chain: {e}")
        return pd.DataFrame()

def format_datetime(dt_str):
    if not dt_str or dt_str == "-":
        return "N/A"
    try:
        return datetime.fromisoformat(dt_str).strftime('%Y-%m-%d %H:%M')
    except ValueError:
        return dt_str

# --- Sidebar for Filters ---
with st.sidebar:
    st.subheader("Filters")
    msg_start = st.date_input("From", date.today() - timedelta(days=3))
    msg_end = st.date_input("To", date.today())
    msg_status = st.selectbox("Status", ["All", "Analyzed", "Not Analyzed"])
    msg_search = st.text_input("Search Content", placeholder="Enter text...")
    refresh_interval = st.slider("Refresh Interval (s)", 10, 300, 60, 10)

# Auto refresh
st_autorefresh(interval=refresh_interval * 1000, key="datarefresh")

# --- Main UI ---
st.title("📨 Trade Bot Messages")

st.subheader("Messages & Analyses")
if msg_start <= msg_end:
    messages = get_messages(msg_start, msg_end, msg_status, msg_search)
    if not messages.empty:
        messages["timestamp"] = messages["timestamp"].apply(format_datetime)
        messages["analysis_timestamp"] = messages["analysis_timestamp"].apply(format_datetime)
        messages["content_preview"] = messages["content"].apply(lambda x: x[:80] + "..." if len(x) > 80 else x)

        st.caption(f"Showing {len(messages)} messages")

        selected_idx = st.dataframe(
            messages[[
                "timestamp", "channel_name", "author_name", "content_preview",
                "action_type", "confidence_score", "reason", "related_trade_id"
            ]].rename(columns={
                "timestamp": "Time", "channel_name": "Channel", "author_name": "Author",
                "content_preview": "Content", "action_type": "Type",
                "confidence_score": "Conf.", "reason": "Reason", "related_trade_id": "Trade"
            }),
            column_config={
                "Conf.": st.column_config.ProgressColumn(
                    "Conf.", min_value=0, max_value=10, format="%.1f", width=80
                ),
                "Content": st.column_config.TextColumn(width="large"),
                "Reason": st.column_config.TextColumn(width="medium"),
                "Type": st.column_config.TextColumn(width="small"),
                "Trade": st.column_config.TextColumn(width="small")
            },
            use_container_width=True,
            height=400,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun"
        ).selection["rows"]

        if selected_idx:
            msg = messages.iloc[selected_idx[0]]
            st.subheader("Message Details")
            tabs = st.tabs(["Message & Analysis", "Reason & Payload", "Conversation Thread"])

            with tabs[0]:
                cols = st.columns([2, 1])
                with cols[0]:
                    st.markdown(f"**Message ID:** {msg['message_id']}")
                    st.markdown(f"**Time:** {msg['timestamp']}")
                    st.markdown(f"**Channel:** {msg['channel_name']}")
                    st.markdown(f"**Author:** {msg['author_name']}")
                    st.markdown("**Content:**")
                    st.code(msg['content'], language="text", line_numbers=True)
                with cols[1]:
                    st.markdown(f"**Processed:** {'Yes' if msg['processed'] else 'No'}")
                    if msg['has_analysis']:
                        st.markdown("**Analysis:**")
                        st.metric("Type", msg['action_type'])
                        st.metric("Confidence", f"{msg['confidence_score']:.1f}/10")
                        st.metric("Trade", msg['related_trade_id'])
                        st.markdown(f"**Analysis ID:** {msg['analysis_id']}")
                        st.markdown(f"**Analysis Time:** {msg['analysis_timestamp']}")
                    else:
                        st.info("No analysis available for this message.")

            if msg['has_analysis']:
                with tabs[1]:
                    st.markdown("**Reason:**")
                    st.write(msg['reason'])
                    st.markdown("**Analysis Payload:**")
                    st.code(msg['analysis_payload'], language="json", line_numbers=True)
            else:
                with tabs[1]:
                    st.info("No analysis available.")

            with tabs[2]:
                chain = get_message_chain(msg["message_id"])
                if not chain.empty:
                    chain["timestamp"] = chain["timestamp"].apply(format_datetime)
                    for _, row in chain.iterrows():
                        with st.chat_message("user" if "Selected" in row["type"] else "assistant"):
                            st.markdown(f"**{row['author_name']}** @ {row['timestamp']} (ID: {row['message_id']}, Parent: {row['parent_id']}, Type: {row['type']})")
                            st.write(row['content'])
                else:
                    st.info("No parent or reply messages found.")
        else:
            st.info("Select a message to view details.")
    else:
        st.info("No messages found for the selected filters.")
else:
    st.error("Invalid date range: 'From' date must be before or equal to 'To' date.")