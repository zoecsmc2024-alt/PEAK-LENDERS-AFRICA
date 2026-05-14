import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from core.database import get_cached_data

# IMPORTANT: correct calendar component
from streamlit_calendar import calendar


def show_calendar():

    st.markdown(
        "<h2 style='color: #2B3F87;'>📅 Loan Activity Calendar</h2>",
        unsafe_allow_html=True
    )

    tenant_id = st.session_state.get("tenant_id")
    if not tenant_id:
        st.error("Session expired.")
        return

    # ==============================
    # FAST SAFE DATA LOADING
    # ==============================
    def load(name):
        df = get_cached_data(name)
        if df is None:
            return pd.DataFrame()
        if "tenant_id" in df.columns:
            df = df[df["tenant_id"].astype(str) == str(tenant_id)]
        return df.copy()

    loans_df = load("loans")
    borrowers_df = load("borrowers")

    if loans_df.empty:
        st.info("📅 No loan data available yet.")
        return

    # ==============================
    # SAFE NORMALIZATION
    # ==============================
    for col in ["principal", "interest", "cycle_no", "balance"]:
        if col in loans_df.columns:
            loans_df[col] = pd.to_numeric(loans_df[col], errors="coerce").fillna(0)

    if "status" in loans_df.columns:
        loans_df["status"] = loans_df["status"].astype(str).str.upper()
    else:
        loans_df["status"] = "UNKNOWN"

    if "sn" in loans_df.columns:
        loans_df["sn"] = loans_df["sn"].astype(str).str.upper()

    # ==============================
    # BORROWER MAPPING (SAFE)
    # ==============================
    if not borrowers_df.empty and "id" in borrowers_df.columns:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(borrowers_df["id"], borrowers_df.get("name", "")))
        if "borrower_id" in loans_df.columns:
            loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)
            loans_df["borrower"] = loans_df["borrower_id"].map(bor_map).fillna("Unknown")
    else:
        loans_df["borrower"] = "Unknown"

    # ==============================
    # DATES
    # ==============================
    if "end_date" in loans_df.columns:
        loans_df["end_date"] = pd.to_datetime(loans_df["end_date"], errors="coerce")
    else:
        loans_df["end_date"] = pd.NaT

    today = pd.Timestamp.now().normalize()

    # ==============================
    # ACTIVE LOANS
    # ==============================
    active_loans = loans_df[
        ~loans_df["status"].isin(["CLEARED", "BCF", "CLOSED"])
    ].copy()

    # ==============================
    # OVERDUE LOGIC (SAFE)
    # ==============================
    overdue_mask = (
        active_loans["end_date"].notna()
        & (active_loans["end_date"] < today)
    )

    overdue_count = int(overdue_mask.sum())

    due_today_df = active_loans[
        active_loans["end_date"].dt.date == today.date()
    ]

    upcoming_df = active_loans[
        (active_loans["end_date"] > today) &
        (active_loans["end_date"] <= today + pd.Timedelta(days=7))
    ]

    # ==============================
    # CALENDAR EVENTS (FAST VECTOR STYLE)
    # ==============================
    events = []

    if not active_loans.empty:

        repayable = active_loans.get("total_repayable", active_loans.get("principal", 0))
        repayable = pd.to_numeric(repayable, errors="coerce").fillna(0)

        is_overdue = active_loans["end_date"] < today

        for i in range(len(active_loans)):

            row = active_loans.iloc[i]

            if pd.isna(row["end_date"]):
                continue

            color = "#FF4B4B" if is_overdue.iloc[i] else "#4A90E2"

            amount = float(row.get("total_repayable", row.get("principal", 0)))

            borrower = row.get("borrower", "Unknown")

            events.append({
                "title": f"UGX {amount:,.0f} - {borrower}",
                "start": row["end_date"].strftime("%Y-%m-%d"),
                "end": row["end_date"].strftime("%Y-%m-%d"),
                "color": color,
                "allDay": True,
            })

    # ==============================
    # RENDER CALENDAR
    # ==============================
    calendar_options = {
        "headerToolbar": {
            "left": "prev,next today",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek"
        },
        "initialView": "dayGridMonth",
        "selectable": True,
    }

    calendar(events=events, options=calendar_options, key="loan_calendar")

    st.markdown("---")

    # ==============================
    # KPI METRICS
    # ==============================
    col1, col2, col3 = st.columns(3)

    col1.markdown(
        f"""
        <div style="padding:16px;background:#F0F8FF;border-radius:12px;border-left:4px solid #2B3F87;">
            <div style="font-size:12px;color:#666;">Due Today</div>
            <div style="font-size:20px;font-weight:700;color:#2B3F87;">{len(due_today_df)}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    col2.markdown(
        f"""
        <div style="padding:16px;background:#F5F7FF;border-radius:12px;border-left:4px solid #4A90E2;">
            <div style="font-size:12px;color:#666;">Next 7 Days</div>
            <div style="font-size:20px;font-weight:700;color:#4A90E2;">{len(upcoming_df)}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    col3.markdown(
        f"""
        <div style="padding:16px;background:#FFF5F5;border-radius:12px;border-left:4px solid #D32F2F;">
            <div style="font-size:12px;color:#666;">Overdue</div>
            <div style="font-size:20px;font-weight:700;color:#D32F2F;">{overdue_count}</div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # ==============================
    # MONTH FORECAST
    # ==============================
    st.markdown("---")
    st.markdown("### 📊 Monthly Forecast")

    this_month = active_loans[
        active_loans["end_date"].dt.month == today.month
    ]

    total_expected = pd.to_numeric(
        this_month.get("total_repayable", this_month.get("principal", 0)),
        errors="coerce"
    ).sum()

    m1, m2 = st.columns(2)
    m1.metric("Expected Collections", f"UGX {total_expected:,.0f}")
    m2.metric("Loans Due This Month", len(this_month))

    # ==============================
    # ACTION TABLE
    # ==============================
    st.markdown("### 📌 Today’s Action List")

    if due_today_df.empty:
        st.success("No due payments today.")
        return

    table_rows = ""

    for _, r in due_today_df.iterrows():

        amount = float(r.get("total_repayable", r.get("principal", 0)))

        table_rows += f"""
        <tr>
            <td style="padding:10px;">{str(r.get('id',''))[:8]}</td>
            <td style="padding:10px;">{r.get('borrower','Unknown')}</td>
            <td style="padding:10px;text-align:right;">UGX {amount:,.0f}</td>
            <td style="padding:10px;text-align:center;">
                <span style="background:#2B3F87;color:white;padding:4px 10px;border-radius:8px;font-size:11px;">
                    COLLECT
                </span>
            </td>
        </tr>
        """

    st.markdown(f"""
    <div style="border:1px solid #2B3F87;border-radius:10px;overflow:hidden;">
        <table style="width:100%;border-collapse:collapse;">
            <tr style="background:#2B3F87;color:white;">
                <th style="padding:10px;">Loan ID</th>
                <th style="padding:10px;">Borrower</th>
                <th style="padding:10px;text-align:right;">Amount</th>
                <th style="padding:10px;">Action</th>
            </tr>
            {table_rows}
        </table>
    </div>
    """, unsafe_allow_html=True)
