import streamlit as st
import pandas as pd
from core.database import get_cached_data

def show_calendar():
    st.title("📅 Activity Calendar")

    # 1. DATA LOAD
    loans_df = get_cached_data("loans")

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    # Use a copy to ensure we aren't modifying the cached original
    df = loans_df.copy()
    df.columns = df.columns.str.strip().str.replace(" ", "_")

    # 2. TYPE CLEANING & COLUMN REPAIR
    # We use pd.Series() to wrap the columns. This prevents the 'str' attribute error.
    df["Status"] = pd.Series(df.get("Status", "UNKNOWN")).astype(str).str.upper()
    df["Borrower"] = pd.Series(df.get("Borrower", "Unknown")).astype(str)
    df["End_Date"] = pd.to_datetime(df.get("End_Date"), errors="coerce")
    
    # Numeric Safety
    for col in ["Total_Repayable", "Principal", "Interest", "sn", "cycle_no"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 3. CONSTRUCT STATUS VIEW (SAFE VERSION)
    # This was the specific line causing your error.
    df["status_view"] = df["Status"] + " • " + df["Borrower"]

    # 4. FILTER: LATEST CYCLE ONLY
    # Sort and drop duplicates so we only see the current state of a loan SN
    df = df.sort_values(["sn", "cycle_no"], ascending=True).drop_duplicates(subset=["sn"], keep="last")

    # 5. KPI METRICS WITH COLORS
    today = pd.Timestamp.today().normalize()
    active = df[~df["Status"].isin(["CLOSED", "CLEARED", "BCF"])]
    
    due_today = active[active["End_Date"].dt.date == today.date()]
    upcoming = active[(active["End_Date"] > today) & (active_loans["End_Date"] <= today + pd.Timedelta(days=7))]
    overdue = active[active["End_Date"] < today]

    c1, c2, c3 = st.columns(3)
    c1.metric("Due Today", len(due_today), delta_color="normal")
    c2.metric("Upcoming (7 Days)", len(upcoming))
    c3.metric("Overdue", len(overdue), delta=f"-{len(overdue)}", delta_color="inverse")

    st.divider()

    # 6. DATAFRAME CONFIGURATION (COLORS & FORMATS)
    # We use column_config instead of HTML to add "Status" colors
    ui_config = {
        "Status": st.column_config.SelectboxColumn(
            "Status",
            options=["ACTIVE", "PENDING", "OVERDUE", "CLEARED"],
            required=True,
        ),
        "Total_Repayable": st.column_config.NumberColumn("Amount Due", format="UGX %d"),
        "End_Date": st.column_config.DateColumn("Deadline", format="DD/MM/YYYY"),
    }

    # 7. DISPLAY SECTIONS
    st.subheader("🚨 Overdue Items")
    if not overdue.empty:
        st.dataframe(
            overdue[["Loan_ID", "Borrower", "Total_Repayable", "End_Date", "Status"]],
            column_config=ui_config,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.success("No overdue loans. Great job!")

    st.subheader("📅 Upcoming Deadlines")
    st.dataframe(
        active[["Loan_ID", "Borrower", "Total_Repayable", "End_Date", "Status"]].sort_values("End_Date"),
        column_config=ui_config,
        use_container_width=True,
        hide_index=True
    )
