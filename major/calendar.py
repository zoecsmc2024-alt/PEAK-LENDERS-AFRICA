import streamlit as st
import pandas as pd

from streamlit_calendar import calendar
from core.database import get_cached_data


# ==============================
# 📅 COLLECTIONS CALENDAR
# ==============================
def show_calendar():

    st.title("📅 Collections Calendar")

    # ==============================
    # LOAD DATA
    # ==============================
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")
    payments_df = get_cached_data("payments")

    # ==============================
    # SAFETY
    # ==============================
    if loans_df is None or loans_df.empty:
        st.info("No loans available.")
        return

    if borrowers_df is None:
        borrowers_df = pd.DataFrame()

    if payments_df is None:
        payments_df = pd.DataFrame()

    # ==============================
    # CLEAN COLUMN NAMES
    # ==============================
    loans_df.columns = (
        loans_df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # ==============================
    # REQUIRED COLUMNS
    # ==============================
    required_cols = {
        "id": "",
        "loan_id_label": "",
        "borrower_id": "",
        "status": "",
        "cycle_no": 1,
        "principal": 0,
        "interest": 0,
        "total_repayable": 0,
        "amount_paid": 0,
        "balance": 0,
        "end_date": None
    }

    for col, default in required_cols.items():

        if col not in loans_df.columns:
            loans_df[col] = default

    # ==============================
    # TYPE CLEANUP
    # ==============================
    loans_df["id"] = loans_df["id"].astype(str)

    loans_df["borrower_id"] = (
        loans_df["borrower_id"]
        .astype(str)
    )

    loans_df["loan_id_label"] = (
        loans_df["loan_id_label"]
        .astype(str)
    )

    loans_df["status"] = (
        loans_df["status"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    loans_df["cycle_no"] = pd.to_numeric(
        loans_df["cycle_no"],
        errors="coerce"
    ).fillna(1)

    # ==============================
    # NUMERIC CLEANUP
    # ==============================
    numeric_cols = [
        "principal",
        "interest",
        "total_repayable",
        "amount_paid",
        "balance"
    ]

    for col in numeric_cols:

        loans_df[col] = pd.to_numeric(
            loans_df[col],
            errors="coerce"
        ).fillna(0)

    # ==============================
    # DATE CLEANUP
    # ==============================
    loans_df["end_date"] = pd.to_datetime(
        loans_df["end_date"],
        errors="coerce"
    )

    # ==============================
    # borrower MAP
    # ==============================
    if not borrowers_df.empty:

        borrowers_df["id"] = (
            borrowers_df["id"]
            .astype(str)
        )

        borrower_map = dict(
            zip(
                borrowers_df["id"],
                borrowers_df["name"]
            )
        )

        loans_df["borrower"] = (
            loans_df["borrower_id"]
            .map(borrower_map)
            .fillna("Unknown")
        )

    else:
        loans_df["borrower"] = "Unknown"

    # ==============================
    # PAYMENT SYNC
    # ==============================
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

        payment_sums = (
            payments_df
            .groupby("loan_id")["amount"]
            .sum()
        )

        loans_df["amount_paid"] = (
            loans_df["id"]
            .map(payment_sums)
            .fillna(0)
        )

    # ==============================
    # RECALCULATE BALANCE
    # ==============================
    loans_df["balance"] = (
        loans_df["total_repayable"]
        - loans_df["amount_paid"]
    ).clip(lower=0)

    # ==============================
    # KEEP ONLY LATEST CYCLE
    # ==============================
    loans_df = loans_df.sort_values(
        by=["loan_id_label", "cycle_no"],
        ascending=[True, False]
    )

    loans_df = loans_df.drop_duplicates(
        subset=["loan_id_label"],
        keep="first"
    )

    # ==============================
    # LIVE LOANS ONLY
    # ==============================
    active_loans = loans_df[
        loans_df["status"].isin([
            "ACTIVE",
            "PENDING"
        ])
    ].copy()

    # ==============================
    # TODAY
    # ==============================
    today = pd.Timestamp.today().normalize()

    # ==============================
    # DASHBOARD METRICS
    # ==============================
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
        active_loans["end_date"]
        < today
    ]
    
    total_balance = (
        active_loans["balance"]
        .sum()
    )
    
    pending_count = len(
        active_loans[
            active_loans["status"] == "PENDING"
        ]
    )
    
    active_count = len(
        active_loans[
            active_loans["status"] == "ACTIVE"
        ]
    )
    
    overdue_count = len(overdue_df)
    
    # ==============================
    # COLORED METRIC CARDS
    # ==============================
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    
    with c1:
        st.markdown("""
        <div style="
            background:linear-gradient(135deg,#2563eb,#1d4ed8);
            padding:18px;
            border-radius:16px;
            color:white;
            text-align:center;
            box-shadow:0 4px 12px rgba(37,99,235,0.25);
        ">
            <div style="font-size:13px;">📌 Due Today</div>
            <div style="font-size:30px;font-weight:bold;">{}</div>
        </div>
        """.format(f"{len(due_today_df):,}"), unsafe_allow_html=True)
    
    with c2:
        st.markdown("""
        <div style="
            background:linear-gradient(135deg,#f59e0b,#d97706);
            padding:18px;
            border-radius:16px;
            color:white;
            text-align:center;
            box-shadow:0 4px 12px rgba(245,158,11,0.25);
        ">
            <div style="font-size:13px;">⏳ Upcoming</div>
            <div style="font-size:30px;font-weight:bold;">{}</div>
        </div>
        """.format(f"{len(upcoming_df):,}"), unsafe_allow_html=True)
    
    with c3:
        st.markdown("""
        <div style="
            background:linear-gradient(135deg,#ef4444,#b91c1c);
            padding:18px;
            border-radius:16px;
            color:white;
            text-align:center;
            box-shadow:0 4px 12px rgba(239,68,68,0.25);
        ">
            <div style="font-size:13px;">🔴 Overdue</div>
            <div style="font-size:30px;font-weight:bold;">{}</div>
        </div>
        """.format(f"{overdue_count:,}"), unsafe_allow_html=True)
    
    with c4:
        st.markdown("""
        <div style="
            background:linear-gradient(135deg,#f97316,#ea580c);
            padding:18px;
            border-radius:16px;
            color:white;
            text-align:center;
            box-shadow:0 4px 12px rgba(249,115,22,0.25);
        ">
            <div style="font-size:13px;">🟠 Pending</div>
            <div style="font-size:30px;font-weight:bold;">{}</div>
        </div>
        """.format(f"{pending_count:,}"), unsafe_allow_html=True)
    
    with c5:
        st.markdown("""
        <div style="
            background:linear-gradient(135deg,#06b6d4,#0891b2);
            padding:18px;
            border-radius:16px;
            color:white;
            text-align:center;
            box-shadow:0 4px 12px rgba(6,182,212,0.25);
        ">
            <div style="font-size:13px;">🔵 Active</div>
            <div style="font-size:30px;font-weight:bold;">{}</div>
        </div>
        """.format(f"{active_count:,}"), unsafe_allow_html=True)
    
    with c6:
        st.markdown("""
        <div style="
            background:linear-gradient(135deg,#10b981,#047857);
            padding:18px;
            border-radius:16px;
            color:white;
            text-align:center;
            box-shadow:0 4px 12px rgba(16,185,129,0.25);
        ">
            <div style="font-size:13px;">💰 Portfolio</div>
            <div style="font-size:26px;font-weight:bold;">{}</div>
        </div>
        """.format(f"{total_balance:,.0f}"), unsafe_allow_html=True)
    
    st.divider()

    # ==============================
    # CALENDAR EVENTS
    # ==============================
    calendar_events = []

    for _, row in active_loans.iterrows():

        if pd.isna(row["end_date"]):
            continue

        end_date = row["end_date"]

        if end_date.date() < today.date():
            color = "#ef4444"

        elif row["status"] == "PENDING":
            color = "#f59e0b"

        else:
            color = "#2563eb"

        amount = f"{row['balance']:,.0f}"

        calendar_events.append({
            "title":
                f"{row['loan_id_label']} • "
                f"{row['borrower']} • "
                f"{amount}",

            "start":
                end_date.strftime("%Y-%m-%d"),

            "end":
                end_date.strftime("%Y-%m-%d"),

            "color":
                color,

            "allDay":
                True,
        })

    calendar_options = {
        "headerToolbar": {
            "left": "prev,next today",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek"
        },

        "initialView":
            "dayGridMonth",

        "height":
            650,
    }

    calendar(
        events=calendar_events,
        options=calendar_options,
        key="loan_calendar"
    )

    st.divider()

    
    # ==============================
    # UPCOMING COLLECTIONS
    # ==============================
    st.subheader("⏳ Upcoming Collections")
    
    if upcoming_df.empty:
    
        st.info(
            "No upcoming collections."
        )
    
    else:
    
        upcoming_df = upcoming_df.copy()
    
        upcoming_df["Amount"] = (
            upcoming_df["balance"]
            .apply(
                lambda x:
                f"{x:,.0f} UGX"
            )
        )
    
        upcoming_df["Due Date"] = (
            upcoming_df["end_date"]
            .dt.strftime("%d %b %Y")
        )
    
        upcoming_df["Days Left"] = (
            upcoming_df["end_date"]
            - today
        ).dt.days
    
        upcoming_df = upcoming_df.sort_values(
            by="Days Left"
        )
    
        # ==============================
        # COLOR ENGINE
        # ==============================
        def highlight_upcoming(row):
    
            days = row["Days Left"]
    
            # Due very soon
            if days <= 2:
    
                style = (
                    "background-color:#fee2e2;"
                    "color:#991b1b;"
                    "font-weight:bold;"
                )
    
            # Medium urgency
            elif days <= 5:
    
                style = (
                    "background-color:#fef3c7;"
                    "color:#92400e;"
                    "font-weight:bold;"
                )
    
            # Safe
            else:
    
                style = (
                    "background-color:#dcfce7;"
                    "color:#166534;"
                )
    
            return [style] * len(row)
    
        # ==============================
        # INTERACTIVE STYLED TABLE
        # ==============================
        styled_upcoming = (
            upcoming_df[
                [
                    "loan_id_label",
                    "borrower",
                    "Amount",
                    "Due Date",
                    "Days Left",
                    "status"
                ]
            ]
            .style
            .apply(
                highlight_upcoming,
                axis=1
            )
        )
    
        st.dataframe(
            styled_upcoming,
            use_container_width=True,
            hide_index=True
        )
    
        st.divider()
    
        
