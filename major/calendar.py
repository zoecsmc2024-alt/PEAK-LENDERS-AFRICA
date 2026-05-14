import streamlit as st
import pandas as pd
from datetime import datetime
from core.database import get_cached_data
from streamlit_calendar import calendar

def show_calendar():
    # 🔄 Refresh data on each load
    st.cache_data.clear()
    
    st.title("📅 Collection & Activity Manager")

    # 1. DATA LOADING
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("No loan records found.")
        return

    # 2. DATA CLEANING (Fixed the 'Series' error here)
    # Convert to datetime safely
    loans_df["end_date"] = pd.to_datetime(loans_df["end_date"], errors="coerce")
    
    # Clean up status strings correctly using .str accessor
    loans_df["status"] = loans_df["status"].astype(str).str.upper().str.strip()
    
    # Map borrower names
    if borrowers_df is not None and not borrowers_df.empty:
        bor_map = dict(zip(borrowers_df["id"].astype(str), borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].astype(str).map(bor_map).fillna("Unknown")
    else:
        loans_df["borrower"] = "Unknown"

    today = pd.Timestamp.today().normalize()

    # 3. DEFINE LOGIC BUCKETS
    # Pending: Applied but not yet running
    pending_df = loans_df[loans_df["status"] == "PENDING"].copy()
    
    # Active: Currently running and not yet past due date
    active_df = loans_df[
        (loans_df["status"] == "ACTIVE") & (loans_df["end_date"] >= today)
    ].copy()
    
    # Overdue: Marked as overdue OR Active but past the end date
    overdue_df = loans_df[
        (loans_df["status"] == "OVERDUE") | 
        ((loans_df["status"] == "ACTIVE") & (loans_df["end_date"] < today))
    ].copy()

    # 4. TOP LEVEL METRICS
    m1, m2, m3 = st.columns(3)
    m1.metric("🟠 Pending", len(pending_df))
    m2.metric("🔵 Active", len(active_df))
    m3.metric("🔴 Overdue", len(overdue_df))

    # 5. THE CALENDAR
    st.subheader("Schedule Overview")
    calendar_events = []
    
    # Combine all relevant categories for the calendar
    display_df = pd.concat([pending_df, active_df, overdue_df])
    
    for _, r in display_df.iterrows():
        if pd.isna(r["end_date"]): continue
        
        # Assign colors based on status logic
        if r["status"] == "PENDING":
            color = "#FFA500" # Orange
        elif r["status"] == "OVERDUE" or r["end_date"] < today:
            color = "#FF4B4B" # Red
        else:
            color = "#4A90E2" # Blue

        calendar_events.append({
            "title": f"{r['borrower']} ({r['status']})",
            "start": r["end_date"].strftime("%Y-%m-%d"),
            "color": color,
            "allDay": True,
        })

    calendar(events=calendar_events, options={"initialView": "dayGridMonth"}, key="main_cal")

    # 6. ACTION TABS (Clears up the confusion)
    st.divider()
    tab_overdue, tab_pending, tab_active = st.tabs(["🚨 Overdue Actions", "⏳ Pending Approval", "📑 Active Book"])

    with tab_overdue:
        if not overdue_df.empty:
            st.warning(f"Total Overdue: {len(overdue_df)}")
            st.dataframe(overdue_df[["borrower", "total_repayable", "end_date", "status"]], use_container_width=True)
        else:
            st.success("All collections are up to date!")

    with tab_pending:
        if not pending_df.empty:
            st.info("These loans are awaiting status update to 'Active'.")
            st.dataframe(pending_df[["borrower", "total_repayable", "end_date"]], use_container_width=True)
        else:
            st.write("No pending applications.")

    with tab_active:
        st.dataframe(active_df[["borrower", "total_repayable", "end_date"]], use_container_width=True)
