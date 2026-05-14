import streamlit as st
import pandas as pd
from core.database import get_cached_data

# ==============================
# 📅 ACTIVITY CALENDAR PAGE (CLEAN + SAFE + COLORFUL)
# ==============================
def show_calendar():

    st.markdown(
        "<h2 style='color:#2B3F87;'>📅 Activity Calendar</h2>",
        unsafe_allow_html=True
    )

    # ==============================
    # DATA LOAD
    # ==============================
    loans_df = get_cached_data("loans")

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    # Use a copy to avoid SettingWithCopy warnings
    loans_df = loans_df.copy()
    loans_df.columns = loans_df.columns.str.strip().str.replace(" ", "_")

    # ==============================
    # SAFE COLUMN BOOTSTRAP
    # ==============================
    defaults = {
        "End_Date": pd.NaT,
        "Total_Repayable": 0,
        "Status": "UNKNOWN",
        "Borrower": "Unknown",
        "Loan_ID": "N/A",
        "Principal": 0,
        "Interest": 0,
        "sn": "N/A",
        "cycle_no": 1
    }

    for col, default in defaults.items():
        if col not in loans_df.columns:
            loans_df[col] = default

    # ==============================
    # TYPE CLEANING
    # ==============================
    loans_df["End_Date"] = pd.to_datetime(loans_df["End_Date"], errors="coerce")
    loans_df["Total_Repayable"] = pd.to_numeric(loans_df["Total_Repayable"], errors="coerce").fillna(0)
    loans_df["Principal"] = pd.to_numeric(loans_df["Principal"], errors="coerce").fillna(0)
    loans_df["Interest"] = pd.to_numeric(loans_df["Interest"], errors="coerce").fillna(0)
    loans_df["Status"] = loans_df["Status"].astype(str).str.upper()
    
    # Ensure Borrower is a string to prevent the "Multiple Columns" error
    loans_df["Borrower"] = loans_df["Borrower"].astype(str)

    today = pd.Timestamp.today().normalize()

    # ==========================================
    # ✨ FILTER: SHOW ONLY LATEST SN (NO DUPLICATES)
    # ==========================================
    # This prevents the calendar from showing old cycles that were rolled over (BCF)
    loans_df = loans_df.sort_values(["sn", "cycle_no"], ascending=True)
    loans_df = loans_df.drop_duplicates(subset=["sn"], keep="last")

    # ==============================
    # ACTIVE LOANS FILTER
    # ==============================
    # Filter out closed/cleared, leaving only PENDING and ACTIVE
    active_loans = loans_df[
        ~loans_df["Status"].isin(["CLOSED", "CLEARED", "BCF"])
    ].copy()

    # ==============================
    # CALCULATED DISPLAY AMOUNT
    # ==============================
    active_loans["Display_Amount"] = active_loans["Total_Repayable"]
    mask_zero = active_loans["Display_Amount"] <= 0

    active_loans.loc[mask_zero, "Display_Amount"] = (
        active_loans["Principal"] + active_loans["Interest"]
    )

    # ==============================
    # OVERDUE / DUE FILTERS
    # ==============================
    due_today_df = active_loans[
        active_loans["End_Date"].dt.date == today.date()
    ]

    upcoming_df = active_loans[
        (active_loans["End_Date"] > today) &
        (active_loans["End_Date"] <= today + pd.Timedelta(days=7))
    ]

    overdue_df = active_loans[
        active_loans["End_Date"] < today
    ].copy()

    # ==============================
    # KPI CARDS
    # ==============================
    c1, c2, c3 = st.columns(3)
    c1.metric("📅 Due Today", len(due_today_df))
    c2.metric("⏳ Upcoming (7 Days)", len(upcoming_df))
    c3.metric("🚨 Overdue Loans", len(overdue_df))

    st.markdown("---")

    # ==========================================
    # 🛠️ FIXED: STATUS VIEW (SAFE CONCAT)
    # ==========================================
    # We use .fillna('') and explicit casting to ensure we don't hit a Series mismatch
    loans_df["status_view"] = (
        loans_df["Status"].fillna("UNKNOWN")
        + " • "
        + loans_df["Borrower"].fillna("Unknown")
    )

    # ==============================
    # CLEAN STREAMLIT TABLE
    # ==============================
    st.subheader("📊 Loan Overview (Current Cycles)")

    st.dataframe(
        loans_df[[
            "Loan_ID",
            "Borrower",
            "Status",
            "End_Date",
            "Total_Repayable",
            "Principal",
            "Interest"
        ]].sort_values("End_Date", na_position="last"),
        use_container_width=True,
        hide_index=True
    )

    # ==============================
    # DUE TODAY SECTION
    # ==============================
    st.markdown("### 📌 Due Today")
    if due_today_df.empty:
        st.success("✨ No deadlines today.")
    else:
        st.dataframe(
            due_today_df[[
                "Loan_ID",
                "Borrower",
                "Display_Amount",
                "End_Date",
                "Status"
            ]],
            use_container_width=True,
            hide_index=True
        )

    # ==============================
    # UPCOMING SECTION
    # ==============================
    st.markdown("### ⏳ Upcoming (7 Days)")
    if upcoming_df.empty:
        st.info("No upcoming deadlines.")
    else:
        st.dataframe(
            upcoming_df[[
                "Loan_ID",
                "Borrower",
                "Display_Amount",
                "End_Date",
                "Status"
            ]].sort_values("End_Date"),
            use_container_width=True,
            hide_index=True
        )

    # ==============================
    # OVERDUE SECTION
    # ==============================
    st.markdown("### 🚨 Overdue Loans (Action Required)")
    if overdue_df.empty:
        st.success("Clean sheet 🎉 No overdue loans.")
    else:
        # Calculate days late
        overdue_df["Days_Late"] = (today - overdue_df["End_Date"]).dt.days
        st.dataframe(
            overdue_df[[
                "Loan_ID",
                "Borrower",
                "Display_Amount",
                "End_Date",
                "Days_Late",
                "Status"
            ]].sort_values("Days_Late", ascending=False),
            use_container_width=True,
            hide_index=True
        )

    # ==============================
    # REVENUE FORECAST
    # ==============================
    st.markdown("---")
    st.subheader("📊 Revenue Forecast (This Month)")

    this_month_df = active_loans[
        active_loans["End_Date"].dt.month == today.month
    ]

    total_expected = this_month_df["Display_Amount"].sum()

    col1, col2 = st.columns(2)
    col1.metric("Expected Collections", f"UGX {total_expected:,.0f}")
    col2.metric("Scheduled Loans", len(this_month_df))

    st.caption("💡 Data filtered to show the most recent cycle per SN.")
