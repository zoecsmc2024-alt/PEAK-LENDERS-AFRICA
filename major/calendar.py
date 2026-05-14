import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from core.database import get_cached_data
from streamlit_calendar import calendar


def show_calendar():

    # -----------------------------
    # 🔄 FORCE FRESH DATA
    # -----------------------------
    st.cache_data.clear()

    st.title("📅 Activity Calendar")

    # -----------------------------
    # 1. FETCH DATA
    # -----------------------------
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    # -----------------------------
    # 👤 BORROWER MAPPING
    # -----------------------------
    if borrowers_df is not None and not borrowers_df.empty:

        borrowers_df["id"] = borrowers_df["id"].astype(str)
        loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)

        bor_map = dict(
            zip(
                borrowers_df["id"],
                borrowers_df["name"]
            )
        )

        loans_df["borrower"] = loans_df["borrower_id"].map(
            bor_map
        ).fillna("Unknown borrower")

    else:
        loans_df["borrower"] = "Unknown borrower"

    # -----------------------------
    # 🛡️ STANDARDIZATION
    # -----------------------------
    loans_df["end_date"] = pd.to_datetime(
        loans_df["end_date"],
        errors="coerce"
    )

    loans_df["total_repayable"] = pd.to_numeric(
        loans_df["total_repayable"],
        errors="coerce"
    ).fillna(0)

    loans_df["status"] = (
        loans_df["status"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    today = pd.Timestamp.today().normalize()

    # -----------------------------
    # ✅ TRUE ACTIVE LOANS ONLY
    # -----------------------------
    active_loans = loans_df[
        loans_df["status"].isin([
            "ACTIVE",
            "PENDING",
            "OVERDUE"
        ])
    ].copy()

    # -----------------------------
    # 🎨 CALENDAR EVENTS
    # -----------------------------
    calendar_events = []

    for _, r in active_loans.iterrows():

        if pd.notna(r["end_date"]):

            is_overdue = r["end_date"].date() < today.date()

            ev_color = (
                "#FF4B4B"
                if is_overdue
                else "#4A90E2"
            )

            amount_fmt = f"UGX {float(r['total_repayable']):,.0f}"

            calendar_events.append({
                "title": f"{amount_fmt} - {r['borrower']}",
                "start": r["end_date"].strftime("%Y-%m-%d"),
                "end": r["end_date"].strftime("%Y-%m-%d"),
                "color": ev_color,
                "allDay": True,
            })

    calendar_options = {
        "headerToolbar": {
            "left": "prev,next today",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek"
        },
        "initialView": "dayGridMonth",
        "selectable": True,
    }

    calendar(
        events=calendar_events,
        options=calendar_options,
        key="collection_cal"
    )

    st.divider()

    # -----------------------------
    # 2. 📊 DAILY WORKLOAD METRICS
    # -----------------------------
    due_today_df = active_loans[
        active_loans["end_date"].dt.date == today.date()
    ]

    upcoming_df = active_loans[
        (active_loans["end_date"] > today) &
        (
            active_loans["end_date"]
            <= today + pd.Timedelta(days=7)
        )
    ]

    overdue_df_metrics = active_loans[
        active_loans["end_date"] < today
    ]

    overdue_count = overdue_df_metrics.shape[0]

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "📌 Due Today",
            f"{len(due_today_df):,} Accounts"
        )

    with col2:
        st.metric(
            "⏳ Upcoming (7 Days)",
            f"{len(upcoming_df):,} Accounts"
        )

    with col3:
        st.metric(
            "🔴 Total Overdue",
            f"{overdue_count:,} Accounts"
        )

    # -----------------------------
    # 3. 📈 REVENUE FORECAST
    # -----------------------------
    st.divider()

    st.subheader("📊 Revenue Forecast (This Month)")

    this_month_df = active_loans[
        active_loans["end_date"].dt.month == today.month
    ]

    total_expected = this_month_df[
        "total_repayable"
    ].sum()

    f1, f2 = st.columns(2)

    with f1:
        st.metric(
            "Expected Collections",
            f"UGX {total_expected:,.0f}"
        )

    with f2:
        st.metric(
            "Deadlines This Month",
            f"{len(this_month_df):,}"
        )

    # -----------------------------
    # 4. 📌 ACTION ITEMS
    # -----------------------------
    st.divider()

    st.subheader("📌 Action Items for Today")

    if due_today_df.empty:

        st.success(
            "✨ No collection deadlines for today."
        )

    else:

        today_display = due_today_df.copy()

        today_display["Loan ID"] = today_display.apply(
            lambda r: (
                r.get("loan_id_label")
                if pd.notna(r.get("loan_id_label"))
                else str(r["id"])[:8]
            ),
            axis=1
        )

        today_display["Amount"] = today_display[
            "total_repayable"
        ].apply(lambda x: f"UGX {x:,.0f}")

        today_display["Action"] = "💰 COLLECT NOW"

        st.dataframe(
            today_display[
                [
                    "Loan ID",
                    "borrower",
                    "Amount",
                    "Action"
                ]
            ],
            use_container_width=True,
            hide_index=True
        )

    # -----------------------------
    # 5. 🔴 OVERDUE FOLLOW-UP
    # -----------------------------
    st.divider()

    st.subheader("🔴 Overdue Follow-up")

    try:

        overdue_df = active_loans[
            active_loans["end_date"] < today
        ].copy()

        if not overdue_df.empty:

            overdue_df["days_late"] = (
                today - overdue_df["end_date"]
            ).dt.days

            overdue_df = overdue_df.sort_values(
                by="days_late",
                ascending=False
            )

            overdue_df["Loan ID"] = overdue_df.apply(
                lambda r: (
                    r.get("loan_id_label")
                    if pd.notna(r.get("loan_id_label"))
                    else str(r["id"])[:8]
                ),
                axis=1
            )

            overdue_df["Late By"] = overdue_df[
                "days_late"
            ].apply(lambda x: f"{x:,} Days")

            overdue_df["Status"] = "🔴 OVERDUE"

            overdue_display = overdue_df[
                [
                    "Loan ID",
                    "borrower",
                    "Late By",
                    "Status"
                ]
            ].rename(columns={
                "borrower": "Borrower"
            })

            st.dataframe(
                overdue_display,
                use_container_width=True,
                hide_index=True
            )

        else:
            st.success(
                "✨ No overdue loans currently."
            )

    except Exception as e:
        st.error(
            f"Error generating overdue table: {e}"
        )
