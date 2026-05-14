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
        "Interest": 0
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

    today = pd.Timestamp.today().normalize()

    # ==============================
    # ACTIVE LOANS FILTER
    # ==============================
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

    overdue_count = len(overdue_df)

    # ==============================
    # KPI CARDS (COLORFUL BUT SAFE)
    # ==============================
    c1, c2, c3 = st.columns(3)

    c1.metric("📅 Due Today", len(due_today_df))
    c2.metric("⏳ Upcoming (7 Days)", len(upcoming_df))
    c3.metric("🚨 Overdue Loans", overdue_count)

    st.markdown("---")

    # ==============================
    # OPTIONAL STATUS VIEW (FIXED BUG)
    # ==============================
    loans_df["status_view"] = (
        loans_df["Status"].astype(str)
        + " • "
        + loans_df["Borrower"].astype(str)
    )

    # ==============================
    # CLEAN STREAMLIT TABLE (NO HTML)
    # ==============================
    st.subheader("📊 Loan Overview")

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
        use_container_width=True
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
            use_container_width=True
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
            use_container_width=True
        )

    # ==============================
    # OVERDUE SECTION (COLORED VIA STREAMLIT, NOT HTML)
    # ==============================
    st.markdown("### 🚨 Overdue Loans (Action Required)")

    if overdue_df.empty:
        st.success("Clean sheet 🎉 No overdue loans.")
    else:
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
            use_container_width=True
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

    st.caption("💡 Click rows above for borrower drill-down (coming next upgrade)")
