import streamlit as st
import pandas as pd

from core.database import get_cached_data
from streamlit_calendar import calendar


def show_calendar():

    st.title("📅 Collections Calendar")

    # -----------------------------
    # LOAD DATA
    # -----------------------------
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")
    payments_df = get_cached_data("payments")

    # -----------------------------
    # SAFETY
    # -----------------------------
    if loans_df is None or loans_df.empty:
        st.info("No loan data found.")
        return

    if borrowers_df is None:
        borrowers_df = pd.DataFrame()

    if payments_df is None:
        payments_df = pd.DataFrame()

    # -----------------------------
    # CLEAN COLUMNS
    # -----------------------------
    loans_df.columns = (
        loans_df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # -----------------------------
    # TYPE CLEANUP
    # -----------------------------
    loans_df["id"] = loans_df["id"].astype(str)

    if "borrower_id" in loans_df.columns:
        loans_df["borrower_id"] = (
            loans_df["borrower_id"]
            .astype(str)
        )

    # -----------------------------
    # NUMERIC CLEANUP
    # -----------------------------
    numeric_cols = [
        "principal",
        "interest",
        "total_repayable",
        "amount_paid",
        "balance"
    ]

    for col in numeric_cols:

        if col not in loans_df.columns:
            loans_df[col] = 0

        loans_df[col] = pd.to_numeric(
            loans_df[col],
            errors="coerce"
        ).fillna(0)

    # -----------------------------
    # DATE CLEANUP
    # -----------------------------
    for col in ["start_date", "end_date"]:

        if col not in loans_df.columns:
            loans_df[col] = None

        loans_df[col] = pd.to_datetime(
            loans_df[col],
            errors="coerce"
        )

    # -----------------------------
    # STATUS CLEANUP
    # -----------------------------
    loans_df["status"] = (
        loans_df["status"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    # -----------------------------
    # borrower MAP
    # -----------------------------
    if not borrowers_df.empty:

        borrowers_df["id"] = (
            borrowers_df["id"]
            .astype(str)
        )

        bor_map = dict(
            zip(
                borrowers_df["id"],
                borrowers_df["name"]
            )
        )

        loans_df["borrower"] = (
            loans_df["borrower_id"]
            .map(bor_map)
            .fillna("Unknown borrower")
        )

    else:
        loans_df["borrower"] = "Unknown borrower"

    # -----------------------------
    # PAYMENT SYNC
    # -----------------------------
    if (
        not payments_df.empty
        and "loan_id" in payments_df.columns
    ):

        payments_df["loan_id"] = (
            payments_df["loan_id"]
            .astype(str)
        )

        payments_df["amount"] = pd.to_numeric(
            payments_df["amount"],
            errors="coerce"
        ).fillna(0)

        pay_sums = (
            payments_df
            .groupby("loan_id")["amount"]
            .sum()
        )

        loans_df["amount_paid"] = (
            loans_df["id"]
            .map(pay_sums)
            .fillna(0)
        )

    # -----------------------------
    # RECALCULATE BALANCE
    # -----------------------------
    loans_df["balance"] = (
        loans_df["total_repayable"]
        - loans_df["amount_paid"]
    ).clip(lower=0)

    # -----------------------------
    # ONLY PENDING LOANS
    # -----------------------------
    active_loans = loans_df[
        loans_df["status"] == "PENDING"
    ].copy()

    today = pd.Timestamp.today().normalize()

    # -----------------------------
    # METRICS
    # -----------------------------
    due_today_df = active_loans[
        active_loans["end_date"].dt.date
        == today.date()
    ]

    upcoming_df = active_loans[
        (
            active_loans["end_date"]
            > today
        )
        &
        (
            active_loans["end_date"]
            <= today + pd.Timedelta(days=7)
        )
    ]

    overdue_df = active_loans[
        active_loans["end_date"] < today
    ]

    total_pending = active_loans["balance"].sum()

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "📌 Due Today",
        len(due_today_df)
    )

    c2.metric(
        "⏳ Upcoming",
        len(upcoming_df)
    )

    c3.metric(
        "🔴 Overdue",
        len(overdue_df)
    )

    c4.metric(
        "💰 Pending Balance",
        f"{total_pending:,.0f}"
    )

    st.markdown("---")

    # -----------------------------
    # CALENDAR EVENTS
    # -----------------------------
    calendar_events = []

    for _, row in active_loans.iterrows():

        if pd.isna(row["end_date"]):
            continue

        is_overdue = (
            row["end_date"].date()
            < today.date()
        )

        event_color = (
            "#ef4444"
            if is_overdue
            else "#2563eb"
        )

        loan_label = str(
            row.get(
                "loan_id_label",
                str(row["id"])[:8]
            )
        )

        amount = f"{row['balance']:,.0f}"

        calendar_events.append({
            "title":
                f"{loan_label} | "
                f"{row['borrower']} | "
                f"{amount}",

            "start":
                row["end_date"].strftime("%Y-%m-%d"),

            "end":
                row["end_date"].strftime("%Y-%m-%d"),

            "color":
                event_color,

            "allDay":
                True,
        })

    calendar_options = {
        "headerToolbar": {
            "left": "prev,next today",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek"
        },
        "initialView": "dayGridMonth",
        "height": 650,
    }

    calendar(
        events=calendar_events,
        options=calendar_options,
        key="loan_calendar"
    )

    st.markdown("---")

    # -----------------------------
    # OVERDUE TABLE
    # -----------------------------
    st.subheader("🔴 Overdue Follow-up")

    if overdue_df.empty:

        st.success(
            "No overdue pending loans."
        )

    else:

        overdue_df = overdue_df.copy()

        overdue_df["days_late"] = (
            today
            - overdue_df["end_date"]
        ).dt.days

        overdue_df = overdue_df.sort_values(
            by="days_late",
            ascending=False
        )

        overdue_df["Late By"] = (
            overdue_df["days_late"]
            .astype(int)
            .astype(str)
            + " Days"
        )

        overdue_df["Balance"] = (
            overdue_df["balance"]
            .apply(
                lambda x:
                f"{x:,.0f} UGX"
            )
        )

        st.dataframe(
            overdue_df[
                [
                    "loan_id_label",
                    "borrower",
                    "Balance",
                    "Late By"
                ]
            ],
            use_container_width=True,
            hide_index=True
        )
