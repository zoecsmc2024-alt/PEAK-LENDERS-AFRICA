import streamlit as st
import pandas as pd
from streamlit_calendar import calendar
from core.database import get_cached_data
from datetime import datetime, timedelta
import uuid
from core.database import get_cached_data
from streamlit_calendar import calendar


def show_calendar():
    # 1. SETUP - Use the full width of the browser
    # Note: If st.set_page_config is called elsewhere, this may not be needed
    st.title("📅 Activitimport streamlit as st


# ==============================
# 📅 LOAN COLLECTION CALENDAR
# ==============================
def show_calendar():

    st.title("📅 Collections & Follow-up Calendar")

    # --------------------------------
    # 🔄 CLEAR CACHE FOR LIVE DATA
    # --------------------------------
    st.cache_data.clear()

    # --------------------------------
    # LOAD DATA
    # --------------------------------
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")
    payments_df = get_cached_data("payments")

    # --------------------------------
    # SAFETY FALLBACKS
    # --------------------------------
    if loans_df is None or loans_df.empty:
        st.info("No loan data available.")
        return

    if borrowers_df is None:
        borrowers_df = pd.DataFrame()

    if payments_df is None:
        payments_df = pd.DataFrame()

    # --------------------------------
    # CLEANUP
    # --------------------------------
    loans_df.columns = (
        loans_df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    loans_df["id"] = loans_df["id"].astype(str)
    loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)

    # --------------------------------
    # NUMERIC CLEANUP
    # --------------------------------
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

    # --------------------------------
    # DATE CLEANUP
    # --------------------------------
    for col in ["start_date", "end_date"]:

        if col not in loans_df.columns:
            loans_df[col] = None

        loans_df[col] = pd.to_datetime(
            loans_df[col],
            errors="coerce"
        )

    # --------------------------------
    # STATUS CLEANUP
    # --------------------------------
    loans_df["status"] = (
        loans_df["status"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    # --------------------------------
    # borrower MAP
    # --------------------------------
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

    # --------------------------------
    # PAYMENT SYNC
    # --------------------------------
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

    # --------------------------------
    # RECALCULATE BALANCE
    # --------------------------------
    loans_df["balance"] = (
        loans_df["total_repayable"]
        - loans_df["amount_paid"]
    ).clip(lower=0)

    # --------------------------------
    # ACTIVE CALENDAR LOANS
    # ONLY PENDING LOANS
    # --------------------------------
    active_loans = loans_df[
        loans_df["status"] == "PENDING"
    ].copy()

    today = pd.Timestamp.today().normalize()

    # =================================
    # 📊 DASHBOARD METRICS
    # =================================
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

    total_pending_amount = (
        active_loans["balance"].sum()
    )

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "📌 Due Today",
        f"{len(due_today_df):,}"
    )

    c2.metric(
        "⏳ Upcoming",
        f"{len(upcoming_df):,}"
    )

    c3.metric(
        "🔴 Overdue",
        f"{len(overdue_df):,}"
    )

    c4.metric(
        "💰 Pending Balance",
        f"{total_pending_amount:,.0f}"
    )

    st.divider()

    # =================================
    # 📅 CALENDAR EVENTS
    # =================================
    calendar_events = []

    for _, row in active_loans.iterrows():

        if pd.isna(row["end_date"]):
            continue

        end_date = row["end_date"]

        is_overdue = (
            end_date.date()
            < today.date()
        )

        if is_overdue:
            event_color = "#ef4444"
        else:
            event_color = "#2563eb"

        loan_label = (
            row["loan_id_label"]
            if pd.notna(
                row.get("loan_id_label")
            )
            else str(row["id"])[:8]
        )

        amount = (
            f"{row['balance']:,.0f}"
        )

        calendar_events.append({
            "title":
                f"{loan_label} • "
                f"{row['borrower']} • "
                f"{amount}",

            "start":
                end_date.strftime("%Y-%m-%d"),

            "end":
                end_date.strftime("%Y-%m-%d"),

            "color":
                event_color,

            "allDay":
                True,
        })

    calendar_options = {
        "headerToolbar": {
            "left": "prev,next today",
            "center": "title",
            "right":
                "dayGridMonth,timeGridWeek"
        },

        "initialView":
            "dayGridMonth",

        "height":
            650,

        "selectable":
            True,
    }

    calendar(
        events=calendar_events,
        options=calendar_options,
        key="loan_calendar"
    )

    st.divider()

    # =================================
    # 📌 DUE TODAY TABLE
    # =================================
    st.subheader("📌 Due Today")

    if due_today_df.empty:

        st.success(
            "No collections due today."
        )

    else:

        due_display = due_today_df.copy()

        due_display["Loan ID"] = (
            due_display["loan_id_label"]
        )

        due_display["Amount"] = (
            due_display["balance"]
            .apply(
                lambda x:
                f"{x:,.0f} UGX"
            )
        )

        due_display["Due Date"] = (
            due_display["end_date"]
            .dt.strftime("%d %b %Y")
        )

        due_display["Action"] = (
            "💰 Collect"
        )

        st.dataframe(
            due_display[
                [
                    "Loan ID",
                    "borrower",
                    "Amount",
                    "Due Date",
                    "Action"
                ]
            ],
            use_container_width=True,
            hide_index=True
        )

    # =================================
    # 🔴 OVERDUE FOLLOW-UP
    # =================================
    st.divider()

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

        overdue_df["Loan ID"] = (
            overdue_df["loan_id_label"]
        )

        overdue_df["Late By"] = (
            overdue_df["days_late"]
            .apply(
                lambda x:
                f"{x:,} Days"
            )
        )

        overdue_df["Balance"] = (
            overdue_df["balance"]
            .apply(
                lambda x:
                f"{x:,.0f} UGX"
            )
        )

        overdue_df["Status"] = (
            "🔴 OVERDUE"
        )

        overdue_display = overdue_df[
            [
                "Loan ID",
                "borrower",
                "Balance",
                "Late By",
                "Status"
            ]
        ].rename(columns={
            "borrower":
                "borrower"
        })

        def highlight_overdue(row):

            return [
                "background-color:#fee2e2;"
                "color:#991b1b;"
                "font-weight:bold;"
            ] * len(row)

        styled_overdue = (
            overdue_display.style
            .apply(
                highlight_overdue,
                axis=1
            )
        )

        st.dataframe(
            styled_overdue,
            use_container_width=True,
            hide_index=True
        )

    # =================================
    # ⏳ UPCOMING COLLECTIONS
    # =================================
    st.divider()

    st.subheader("⏳ Upcoming Collections")

    if upcoming_df.empty:

        st.info(
            "No upcoming collections."
        )

    else:

        upcoming_display = (
            upcoming_df.copy()
        )

        upcoming_display["Loan ID"] = (
            upcoming_display[
                "loan_id_label"
            ]
        )

        upcoming_display["Amount"] = (
            upcoming_display["balance"]
            .apply(
                lambda x:
                f"{x:,.0f} UGX"
            )
        )

        upcoming_display["Due Date"] = (
            upcoming_display["end_date"]
            .dt.strftime("%d %b %Y")
        )

        upcoming_display["Days Left"] = (
            upcoming_display["end_date"]
            - today
        ).dt.days

        upcoming_display = (
            upcoming_display.sort_values(
                by="Days Left"
            )
        )

        st.dataframe(
            upcoming_display[
                [
                    "Loan ID",
                    "borrower",
                    "Amount",
                    "Due Date",
                    "Days Left"
                ]
            ].rename(columns={
                "borrower":
                    "borrower"
            }),

            use_container_width=True,
            hide_index=True
        )y & Collection Dashboard")
    
    # 🔄 FORCE FRESH DATA
    st.cache_data.clear()

    # 2. FETCH DATA
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("📅 No active data to display.")
        return

    # 3. STANDARDIZATION
    loans_df["end_date"] = pd.to_datetime(loans_df["end_date"], errors="coerce")
    loans_df["status"] = loans_df["status"].astype(str).str.upper().str.strip()
    
    if borrowers_df is not None:
        bor_map = dict(zip(borrowers_df["id"].astype(str), borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].astype(str).map(bor_map).fillna("Unknown")

    # 4. ROLLOVER LOGIC (Based on image_94de15.png)
    # Sort so the highest installment number is first, then drop old ones.
    # This ensures LN-0069 shows 'PENDING' (Inst 2) and hides 'BCF' (Inst 1).
    loans_df = loans_df.sort_values(by=["loan_id_label", "cycle_no"], ascending=[True, False])
    current_book = loans_df.drop_duplicates(subset=["loan_id_label"], keep="first").copy()

    today = pd.Timestamp.today().normalize()

    # 5. LAYOUT: Top Metrics (Full Width)
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("🟠 Pending", len(current_book[current_book["status"] == "PENDING"]))
    with m2:
        st.metric("🔵 Active", len(current_book[current_book["status"] == "ACTIVE"]))
    with m3:
        st.metric("🔴 Overdue", len(current_book[current_book["end_date"] < today]))
    with m4:
        st.metric("✅ Total Live", len(current_book))

    st.divider()

    # 6. TWO-COLUMN DASHBOARD (Calendar on Left, Action List on Right)
    left_col, right_col = st.columns([2, 1])

    with left_col:
        st.subheader("Collection Schedule")
        calendar_events = []
        for _, r in current_book.iterrows():
            if pd.isna(r["end_date"]): continue
            
            # Color coding
            if r["status"] == "PENDING": color = "#FFA500"
            elif r["end_date"] < today: color = "#FF4B4B"
            else: color = "#4A90E2"

            calendar_events.append({
                "title": f"{r['loan_id_label']} - {r['borrower']}",
                "start": r["end_date"].strftime("%Y-%m-%d"),
                "color": color,
                "allDay": True,
            })

        calendar(events=calendar_events, options={"initialView": "dayGridMonth"}, key="dash_cal")

    with right_col:
        st.subheader("📌 Focus Areas")
        
        with st.expander("🚨 Overdue Follow-up", expanded=True):
            overdue = current_book[current_book["end_date"] < today]
            st.dataframe(overdue[["loan_id_label", "borrower", "end_date"]], hide_index=True, use_container_width=True)
            
        with st.expander("⏳ Pending Approval", expanded=True):
            pending = current_book[current_book["status"] == "PENDING"]
            st.dataframe(pending[["loan_id_label", "borrower"]], hide_index=True, use_container_width=True)

    # 7. BOTTOM SECTION: THE COMPLETE DATA TABLE (Full Width)
    st.divider()
    st.subheader("📑 Current Status of All Loans")
    # This replicates the broad view from your image but only shows the 'Live' installment
    st.dataframe(
        current_book[[
            "loan_id_label", "cycle_no", "borrower", 
            "total_repayable", "end_date", "status"
        ]], 
        use_container_width=True, 
        hide_index=True
    )
