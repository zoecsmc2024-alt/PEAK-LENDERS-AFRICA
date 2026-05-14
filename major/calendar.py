import streamlit as st
import pandas as pd
from streamlit_calendar import calendar
from core.database import get_cached_data

def show_calendar():
    # 1. SETUP - Use the full width of the browser
    # Note: If st.set_page_config is called elsewhere, this may not be needed
    st.title("📅 Activity & Collection Dashboard")
    
    # 🔄 FORCE FRESH DATA
    st.cache_data.clear()

    # 2. FETCH DATA
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("📅 No active data to display.")
        return

    # 3. STANDARDIZATION
    loans_df["end_date"] = pd.to_datetime(loans_df["end_date"], errors="coerce")
    loans_df["status"] = loans_df["status"].astype(str).str.upper().str.strip()
    
    if borrowers_df is not None:
        bor_map = dict(zip(borrowers_df["id"].astype(str), borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].astype(str).map(bor_map).fillna("Unknown")

    # 4. ROLLOVER LOGIC (Based on image_94de15.png)
    # Sort so the highest installment number is first, then drop old ones.
    # This ensures LN-0069 shows 'PENDING' (Inst 2) and hides 'BCF' (Inst 1).
    loans_df = loans_df.sort_values(by=["loan_id_label", "installment_no"], ascending=[True, False])
    current_book = loans_df.drop_duplicates(subset=["loan_id_label"], keep="first").copy()

    today = pd.Timestamp.today().normalize()

    # 5. LAYOUT: Top Metrics (Full Width)
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("🟠 Pending", len(current_book[current_book["status"] == "PENDING"]))
    with m2:
        st.metric("🔵 Active", len(current_book[current_book["status"] == "ACTIVE"]))
    with m3:
        st.metric("🔴 Overdue", len(current_book[current_book["end_date"] < today]))
    with m4:
        st.metric("✅ Total Live", len(current_book))

    st.divider()

    # 6. TWO-COLUMN DASHBOARD (Calendar on Left, Action List on Right)
    left_col, right_col = st.columns([2, 1])

    with left_col:
        st.subheader("Collection Schedule")
        calendar_events = []
        for _, r in current_book.iterrows():
            if pd.isna(r["end_date"]): continue
            
            # Color coding
            if r["status"] == "PENDING": color = "#FFA500"
            elif r["end_date"] < today: color = "#FF4B4B"
            else: color = "#4A90E2"

            calendar_events.append({
                "title": f"{r['loan_id_label']} - {r['borrower']}",
                "start": r["end_date"].strftime("%Y-%m-%d"),
                "color": color,
                "allDay": True,
            })

        calendar(events=calendar_events, options={"initialView": "dayGridMonth"}, key="dash_cal")

    with right_col:
        st.subheader("📌 Focus Areas")
        
        with st.expander("🚨 Overdue Follow-up", expanded=True):
            overdue = current_book[current_book["end_date"] < today]
            st.dataframe(overdue[["loan_id_label", "borrower", "end_date"]], hide_index=True, use_container_width=True)
            
        with st.expander("⏳ Pending Approval", expanded=True):
            pending = current_book[current_book["status"] == "PENDING"]
            st.dataframe(pending[["loan_id_label", "borrower"]], hide_index=True, use_container_width=True)

    # 7. BOTTOM SECTION: THE COMPLETE DATA TABLE (Full Width)
    st.divider()
    st.subheader("📑 Current Status of All Loans")
    # This replicates the broad view from your image but only shows the 'Live' installment
    st.dataframe(
        current_book[[
            "loan_id_label", "installment_no", "borrower", 
            "total_repayable", "end_date", "status"
        ]], 
        use_container_width=True, 
        hide_index=True
    )
