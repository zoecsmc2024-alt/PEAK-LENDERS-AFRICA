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
    # Wrapping in pd.Series prevents the 'str' object has no attribute 'astype' error
    df["Status"] = pd.Series(df.get("Status", "UNKNOWN")).astype(str).str.upper()
    df["Borrower"] = pd.Series(df.get("Borrower", "Unknown")).astype(str)
    df["End_Date"] = pd.to_datetime(df.get("End_Date"), errors="coerce")
    
    # Numeric Safety for your financial tracking
    for col in ["Total_Repayable", "Principal", "Interest", "sn", "cycle_no"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 3. FILTER: LATEST CYCLE ONLY
    # Based on your cycle-aware logic: only show the most recent record per SN
    df = df.sort_values(["sn", "cycle_no"], ascending=True).drop_duplicates(subset=["sn"], keep="last")

    # 4. DEFINE ACTIVE DATASET
    # Filtering out historical and closed records
    active_loans = df[~df["Status"].isin(["CLOSED", "CLEARED", "BCF"])].copy()

    # 5. KPI METRICS (NATIVE COLORS)
    today = pd.Timestamp.today().normalize()
    
    due_today = active_loans[active_loans["End_Date"].dt.date == today.date()]
    upcoming = active_loans[(active_loans["End_Date"] > today) & (active_loans["End_Date"] <= today + pd.Timedelta(days=7))]
    overdue = active_loans[active_loans["End_Date"] < today]

    c1, c2, c3 = st.columns(3)
    # Using 'delta' to add status colors (Red/Green/Gray)
    c1.metric("Due Today", len(due_today), delta="Immediate", delta_color="inverse")
    c2.metric("Upcoming (7 Days)", len(upcoming), delta="Scheduled", delta_color="off")
    c3.metric("Overdue", len(overdue), delta=f"{len(overdue)} Required", delta_color="normal")

    st.divider()

    # 6. DATAFRAME CONFIGURATION
    # Using column_config for clean, colorful UI interaction
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

    st.subheader("📅 All Active Deadlines")
    st.dataframe(
        active_loans[["Loan_ID", "Borrower", "Total_Repayable", "End_Date", "Status"]].sort_values("End_Date"),
        column_config=ui_config,
        use_container_width=True,
        hide_index=True
    )

    # 8. REVENUE FORECAST (FOOTER)
    this_month = active_loans[active_loans["End_Date"].dt.month == today.month]
    st.info(f"**Monthly Collection Forecast:** UGX {this_month['Total_Repayable'].sum():,.0f} across {len(this_month)} scheduled payments.")
