import streamlit as st
import pandas as pd
from core.database import get_cached_data

def show_calendar():
    st.title("📅 Activity Calendar")

    # ==============================
    # ⚡ DATA LOADING & CLEANING
    # ==============================
    loans_df = get_cached_data("loans")

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    loans_df = loans_df.copy()
    loans_df.columns = loans_df.columns.str.strip().str.replace(" ", "_")

    # Normalize Column Types
    loans_df["End_Date"] = pd.to_datetime(loans_df.get("End_Date"), errors="coerce")
    loans_df["Borrower"] = loans_df.get("Borrower", "Unknown").astype(str)
    loans_df["Status"] = loans_df.get("Status", "UNKNOWN").astype(str).str.upper()
    
    # Numeric Safety
    for col in ["Total_Repayable", "Principal", "Interest", "balance", "cycle_no"]:
        if col in loans_df.columns:
            loans_df[col] = pd.to_numeric(loans_df[col], errors="coerce").fillna(0)

    today = pd.Timestamp.today().normalize()

    # ==========================================
    # 🛠️ REINFORCED: STATUS VIEW (FIXED)
    # ==========================================
    # We cast to Series explicitly to ensure .astype() is available
    if "Status" in loans_df.columns and "Borrower" in loans_df.columns:
        # Ensure they are treated as strings first to prevent concatenation issues
        s_col = pd.Series(loans_df["Status"]).astype(str).fillna("UNKNOWN")
        b_col = pd.Series(loans_df["Borrower"]).astype(str).fillna("Unknown")
        
        loans_df["status_view"] = s_col + " • " + b_col

    # ==========================================
    # 🛡️ FILTER: SHOW ONLY LATEST SN
    # ==========================================
    # Ensures we don't show historical "BCF" cycles on the calendar
    loans_df = loans_df.sort_values(["sn", "cycle_no"], ascending=True)
    loans_df = loans_df.drop_duplicates(subset=["sn"], keep="last")

    # Active Loans (Pending/Active/Overdue)
    active_loans = loans_df[~loans_df["Status"].isin(["CLOSED", "CLEARED", "BCF"])].copy()

    # ==============================
    # 📊 KPI SECTION
    # ==============================
    due_today = active_loans[active_loans["End_Date"].dt.date == today.date()]
    upcoming = active_loans[(active_loans["End_Date"] > today) & (active_loans["End_Date"] <= today + pd.Timedelta(days=7))]
    overdue = active_loans[active_loans["End_Date"] < today]

    c1, c2, c3 = st.columns(3)
    
    # Using delta colors for visual impact
    c1.metric("Due Today", len(due_today), delta="Immediate", delta_color="inverse")
    c2.metric("Upcoming (7 Days)", len(upcoming), delta="Scheduled", delta_color="off")
    c3.metric("Overdue Loans", len(overdue), delta=f"-{len(overdue)} Action Required", delta_color="normal")

    st.divider()

    # ==============================
    # 🎨 DATAFRAME COLOR CONFIG
    # ==============================
    # Streamlit Column Config allows us to add "colors" via Progress bars or Status tags
    column_style = {
        "Status": st.column_config.SelectboxColumn(
            "Status",
            options=["ACTIVE", "PENDING", "OVERDUE", "CLEARED"],
            required=True,
        ),
        "Total_Repayable": st.column_config.NumberColumn("Total Due (UGX)", format="Shs %d"),
        "End_Date": st.column_config.DateColumn("Deadline", format="DD/MM/YYYY"),
    }

    # ==============================
    # 🗓️ SCHEDULE SECTIONS
    # ==============================
    
    # 1. DUE TODAY
    st.subheader("📌 Due Today")
    if not due_today.empty:
        st.dataframe(
            due_today[["Loan_ID", "Borrower", "Total_Repayable", "End_Date", "Status"]],
            column_config=column_style,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.write("✅ No deadlines for today.")

    # 2. OVERDUE
    st.subheader("🚨 Overdue (Action Required)")
    if not overdue.empty:
        # Add a "Days Late" column for better visibility
        overdue = overdue.copy()
        overdue["Days_Late"] = (today - overdue["End_Date"]).dt.days
        
        st.dataframe(
            overdue[["Loan_ID", "Borrower", "Total_Repayable", "End_Date", "Days_Late", "Status"]].sort_values("Days_Late", ascending=False),
            column_config={
                **column_style,
                "Days_Late": st.column_config.NumberColumn("Days Late", help="Days passed since deadline")
            },
            use_container_width=True,
            hide_index=True
        )
    else:
        st.write("🎉 No overdue loans!")

    # 3. UPCOMING
    st.subheader("⏳ Upcoming (Next 7 Days)")
    if not upcoming.empty:
        st.dataframe(
            upcoming[["Loan_ID", "Borrower", "Total_Repayable", "End_Date", "Status"]].sort_values("End_Date"),
            column_config=column_style,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.write("ℹ️ No loans scheduled for the next week.")

    # ==============================
    # 📈 MONTHLY FORECAST
    # ==============================
    st.divider()
    st.subheader("💰 Monthly Collection Forecast")
    
    this_month_df = active_loans[active_loans["End_Date"].dt.month == today.month]
    total_expected = this_month_df["Total_Repayable"].sum()
    
    col_a, col_b = st.columns(2)
    with col_a:
        st.info(f"**Total Expected this Month:** UGX {total_expected:,.0f}")
    with col_b:
        st.info(f"**Scheduled Loans:** {len(this_month_df)}")

    st.caption("💡 This page automatically filters to the latest cycle for each Serial Number (SN).")
