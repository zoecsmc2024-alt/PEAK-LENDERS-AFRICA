import streamlit as st
import pandas as pd

from streamlit_calendar import calendar
from core.database import get_cached_data


# ==========================================
# 📅 COLLECTIONS CALENDAR (INDUSTRY STANDARD)
# ==========================================
def show_calendar():

    st.title("📅 Collections Calendar")

    # ==========================================
    # LOAD DATA
    # ==========================================
    schedules_df = get_cached_data("loan_schedules")
    borrowers_df = get_cached_data("borrowers")
    loans_df = get_cached_data("loans")

    # ==========================================
    # SAFETY
    # ==========================================
    if schedules_df is None or schedules_df.empty:

        st.info("No repayment schedules available.")
        return

    if borrowers_df is None:
        borrowers_df = pd.DataFrame()

    if loans_df is None:
        loans_df = pd.DataFrame()

    # ==========================================
    # CLEAN COLUMN NAMES
    # ==========================================
    schedules_df.columns = (
        schedules_df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    borrowers_df.columns = (
        borrowers_df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    loans_df.columns = (
        loans_df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # ==========================================
    # REQUIRED COLUMNS
    # ==========================================
    required_cols = {

        "loan_id": "",
        "due_date": None,
        "amount_due": 0,
        "amount_paid": 0,
        "balance": 0,
        "status": "PENDING",
    }

    for col, default in required_cols.items():

        if col not in schedules_df.columns:
            schedules_df[col] = default

    # ==========================================
    # TYPE CLEANUP
    # ==========================================
    schedules_df["loan_id"] = (
        schedules_df["loan_id"]
        .astype(str)
    )

    schedules_df["status"] = (
        schedules_df["status"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    # ==========================================
    # NUMERIC CLEANUP
    # ==========================================
    numeric_cols = [

        "amount_due",
        "amount_paid",
        "balance"
    ]

    for col in numeric_cols:

        schedules_df[col] = pd.to_numeric(
            schedules_df[col],
            errors="coerce"
        ).fillna(0)

    # ==========================================
    # DATE CLEANUP
    # ==========================================
    schedules_df["due_date"] = pd.to_datetime(
        schedules_df["due_date"],
        errors="coerce"
    )

    # ==========================================
    # LOAN MAP
    # ==========================================
    loan_map = {}

    if not loans_df.empty:

        loans_df["id"] = (
            loans_df["id"]
            .astype(str)
        )

        loans_df["borrower_id"] = (
            loans_df["borrower_id"]
            .astype(str)
        )

        loans_df["loan_id_label"] = (
            loans_df["loan_id_label"]
            .astype(str)
        )

        loan_map = loans_df.set_index("id").to_dict("index")

    # ==========================================
    # BORROWER MAP
    # ==========================================
    borrower_map = {}

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

    # ==========================================
    # ENRICH SCHEDULES
    # ==========================================
    def get_loan_label(loan_id):

        loan = loan_map.get(loan_id, {})

        return loan.get(
            "loan_id_label",
            "Unknown Loan"
        )

    def get_borrower_name(loan_id):

        loan = loan_map.get(loan_id, {})

        borrower_id = str(
            loan.get("borrower_id", "")
        )

        return borrower_map.get(
            borrower_id,
            "Unknown Borrower"
        )

    schedules_df["loan_label"] = (
        schedules_df["loan_id"]
        .apply(get_loan_label)
    )

    schedules_df["borrower"] = (
        schedules_df["loan_id"]
        .apply(get_borrower_name)
    )

    # ==========================================
    # TODAY
    # ==========================================
    today = pd.Timestamp.today().normalize()

    # ==========================================
    # COLLECTION METRICS
    # ==========================================
    due_today_df = schedules_df[

        (
            schedules_df["due_date"]
            .dt.date
            == today.date()
        )
        &
        (
            schedules_df["balance"] > 0
        )
    ]

    overdue_df = schedules_df[

        (
            schedules_df["due_date"]
            < today
        )
        &
        (
            schedules_df["balance"] > 0
        )
    ]

    upcoming_df = schedules_df[

        (
            schedules_df["due_date"]
            > today
        )
        &
        (
            schedules_df["due_date"]
            <= today + pd.Timedelta(days=7)
        )
        &
        (
            schedules_df["balance"] > 0
        )
    ]

    total_due_today = (
        due_today_df["balance"]
        .sum()
    )

    total_overdue = (
        overdue_df["balance"]
        .sum()
    )

    total_upcoming = (
        upcoming_df["balance"]
        .sum()
    )

    # ==========================================
    # METRIC CARDS
    # ==========================================
    c1, c2, c3 = st.columns(3)

    with c1:

        st.markdown(f"""
        <div style="
            background:linear-gradient(135deg,#2563eb,#1d4ed8);
            padding:20px;
            border-radius:18px;
            color:white;
            text-align:center;
            box-shadow:0 4px 14px rgba(37,99,235,0.25);
        ">
            <div style="font-size:14px;">
                📌 Due Today
            </div>

            <div style="
                font-size:30px;
                font-weight:bold;
                margin-top:8px;
            ">
                {total_due_today:,.0f}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with c2:

        st.markdown(f"""
        <div style="
            background:linear-gradient(135deg,#ef4444,#b91c1c);
            padding:20px;
            border-radius:18px;
            color:white;
            text-align:center;
            box-shadow:0 4px 14px rgba(239,68,68,0.25);
        ">
            <div style="font-size:14px;">
                🔴 Overdue
            </div>

            <div style="
                font-size:30px;
                font-weight:bold;
                margin-top:8px;
            ">
                {total_overdue:,.0f}
            </div>
        </div>
        """, unsafe_allow_html=True)

    with c3:

        st.markdown(f"""
        <div style="
            background:linear-gradient(135deg,#10b981,#047857);
            padding:20px;
            border-radius:18px;
            color:white;
            text-align:center;
            box-shadow:0 4px 14px rgba(16,185,129,0.25);
        ">
            <div style="font-size:14px;">
                ⏳ Upcoming (7 Days)
            </div>

            <div style="
                font-size:30px;
                font-weight:bold;
                margin-top:8px;
            ">
                {total_upcoming:,.0f}
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ==========================================
    # CALENDAR EVENTS
    # ==========================================
    calendar_events = []

    for _, row in schedules_df.iterrows():

        if pd.isna(row["due_date"]):
            continue

        if row["balance"] <= 0:
            continue

        due_date = row["due_date"]

        # ==========================================
        # EVENT COLORS
        # ==========================================
        if due_date.date() < today.date():

            color = "#ef4444"

        elif due_date.date() == today.date():

            color = "#f59e0b"

        else:

            color = "#2563eb"

        calendar_events.append({

            "title":
                f"{row['loan_label']} • "
                f"{row['borrower']} • "
                f"{row['balance']:,.0f}",

            "start":
                due_date.strftime("%Y-%m-%d"),

            "end":
                due_date.strftime("%Y-%m-%d"),

            "color":
                color,

            "allDay":
                True,
        })

    # ==========================================
    # CALENDAR OPTIONS
    # ==========================================
    calendar_options = {

        "headerToolbar": {

            "left": "prev,next today",

            "center": "title",

            "right": "dayGridMonth,timeGridWeek"
        },

        "initialView":
            "dayGridMonth",

        "height":
            700,
    }

    calendar(
        events=calendar_events,
        options=calendar_options,
        key="collections_calendar"
    )

    st.divider()

    # ==========================================
    # UPCOMING COLLECTIONS TABLE
    # ==========================================
    st.subheader("⏳ Upcoming Collections")

    if upcoming_df.empty:

        st.info(
            "No upcoming collections."
        )

    else:

        upcoming_df = upcoming_df.copy()

        upcoming_df["Due Date"] = (
            upcoming_df["due_date"]
            .dt.strftime("%d %b %Y")
        )

        upcoming_df["Amount Due"] = (
            upcoming_df["balance"]
            .apply(
                lambda x:
                f"{x:,.0f} UGX"
            )
        )

        upcoming_df["Days Left"] = (

            upcoming_df["due_date"]
            - today

        ).dt.days

        upcoming_df = upcoming_df.sort_values(
            by="Days Left"
        )

        # ==========================================
        # TABLE COLORS
        # ==========================================
        def highlight_rows(row):

            days = row["Days Left"]

            if days <= 2:

                style = (
                    "background-color:#fee2e2;"
                    "color:#991b1b;"
                    "font-weight:bold;"
                )

            elif days <= 5:

                style = (
                    "background-color:#fef3c7;"
                    "color:#92400e;"
                    "font-weight:bold;"
                )

            else:

                style = (
                    "background-color:#dcfce7;"
                    "color:#166534;"
                )

            return [style] * len(row)

        styled_df = (

            upcoming_df[
                [
                    "loan_label",
                    "borrower",
                    "Amount Due",
                    "Due Date",
                    "Days Left",
                    "status"
                ]
            ]

            .style

            .apply(
                highlight_rows,
                axis=1
            )
        )

        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True
        )
