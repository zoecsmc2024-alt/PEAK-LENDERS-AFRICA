import streamlit as st
import pandas as pd
from datetime import datetime
from core.database import get_cached_data
from streamlit_calendar import calendar

def show_calendar():
    st.cache_data.clear()
    st.title("📅 Loan Activity & Collections")

    # 1. FETCH & PREP DATA
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("📅 No loan data available.")
        return

    # Map Borrower Names
    if borrowers_df is not None and not borrowers_df.empty:
        bor_map = dict(zip(borrowers_df["id"].astype(str), borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].astype(str).map(bor_map).fillna("Unknown")
    else:
        loans_df["borrower"] = "Unknown"

    # Standardize Dates and Status
    loans_df["end_date"] = pd.to_datetime(loans_df["end_date"], errors="coerce")
    loans_df["status"] = loans_df["status"].astype(str).str.upper().strip()
    today = pd.Timestamp.today().normalize()

    # 2. CATEGORIZATION LOGIC
    # We split these now so the rest of the page is easy to build
    pending_mask = loans_df["status"] == "PENDING"
    active_mask = loans_df["status"] == "ACTIVE"
    overdue_mask = (loans_df["status"] == "OVERDUE") | ((loans_df["end_date"] < today) & (loans_df["status"] == "ACTIVE"))

    # 3. CALENDAR SECTION
    calendar_events = []
    for _, r in loans_df.iterrows():
        if pd.isna(r["end_date"]): continue
        
        # Color Coding logic
        if r["status"] == "PENDING":
            color = "#FFA500" # Orange
        elif r["status"] == "OVERDUE" or r["end_date"] < today:
            color = "#FF4B4B" # Red
        else:
            color = "#4A90E2" # Blue

        calendar_events.append({
            "title": f"{r['status']}: {r['borrower']}",
            "start": r["end_date"].strftime("%Y-%m-%d"),
            "color": color,
            "allDay": True,
        })

    calendar(events=calendar_events, options={"initialView": "dayGridMonth"}, key="loan_cal")

    # 4. METRICS ROW
    st.divider()
    m1, m2, m3 = st.columns(3)
    m1.metric("🟠 Pending Approval", len(loans_df[pending_mask]))
    m2.metric("🔵 Active Loans", len(loans_df[active_mask]))
    m3.metric("🔴 Overdue/Action Required", len(loans_df[overdue_mask]))

    # 5. TABBED WORKSPACE (The "Full" part of the page)
    # This organizes the confusion into clear "buckets"
    tab1, tab2, tab3 = st.tabs(["⚠️ Overdue & Today", "⏳ Pending Approval", "📑 All Active"])

    with tab1:
        st.subheader("Immediate Actions")
        # Filter for anything due today or in the past that isn't closed
        urgent_df = loans_df[overdue_mask].copy()
        if not urgent_df.empty:
            urgent_df["Days Late"] = (today - urgent_df["end_date"]).dt.days
            st.dataframe(
                urgent_df[["borrower", "total_repayable", "end_date", "Days Late"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("No overdue accounts!")

    with tab2:
        st.subheader("Loans Awaiting Verification")
        p_df = loans_df[pending_mask]
        if not p_df.empty:
            st.write("These loans need to be approved or moved to Active status.")
            st.dataframe(p_df[["borrower", "total_repayable", "end_date"]], use_container_width=True)
        else:
            st.info("No pending applications.")

    with tab3:
        st.subheader("Current Loan Book")
        st.dataframe(loans_df[active_mask][["borrower", "total_repayable", "end_date"]], use_container_width=True)
