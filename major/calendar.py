import streamlit as st
import pandas as pd
from datetime import datetime
from streamlit_calendar import calendar
from core.database import get_cached_data

# ==========================================
# 📅 COLLECTIONS CALENDAR (INDUSTRY STANDARD)
# ==========================================
def show_calendar():
    st.markdown("""
    <style>
    .calendar-metric-container {
        padding: 20px;
        border-radius: 18px;
        color: white;
        text-align: center;
    }
    </style>
    """, unsafe_allow_html=True)

    st.title("📅 Collections Calendar")

    # ==========================================
    # LOAD DATA FROM CACHE LAYER
    # ==========================================
    schedules_raw = get_cached_data("loan_schedules")
    borrowers_raw = get_cached_data("borrowers")
    loans_raw = get_cached_data("loans")

    # ==========================================
    # SAFETY EMPTY CHECK DEFENSE
    # ==========================================
    if schedules_raw is None or (isinstance(schedules_raw, pd.DataFrame) and schedules_raw.empty) or len(schedules_raw) == 0:
        st.info("ℹ️ No repayment schedules available.")
        return

    # Normalize inputs to DataFrames safely
    schedules_df = pd.DataFrame(schedules_raw).copy()
    borrowers_df = pd.DataFrame(borrowers_raw).copy() if borrowers_raw is not None else pd.DataFrame()
    loans_df = pd.DataFrame(loans_raw).copy() if loans_raw is not None else pd.DataFrame()

    # ==========================================
    # CLEAN COLUMN NAMES TO SNAKE_CASE
    # ==========================================
    for df in [schedules_df, borrowers_df, loans_df]:
        if not df.empty:
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_")
            )

    # ==========================================
    # ENFORCE REQUIRED SCHEMA COLUMNS
    # ==========================================
    required_cols = {
        "loan_id": "",
        "due_date": None,
        "amount_due": 0.0,
        "amount_paid": 0.0,
        "balance": 0.0,
        "status": "PENDING",
    }

    for col, default in required_cols.items():
        if col not in schedules_df.columns:
            schedules_df[col] = default

    # ==========================================
    # TYPE NORMALIZATION & CLEANUP
    # ==========================================
    schedules_df = schedules_df.dropna(subset=["loan_id"])
    schedules_df["loan_id"] = schedules_df["loan_id"].astype(str).str.strip()
    schedules_df["status"] = schedules_df["status"].astype(str).str.upper().str.strip()

    numeric_cols = ["amount_due", "amount_paid", "balance"]
    for col in numeric_cols:
        schedules_df[col] = pd.to_numeric(schedules_df[col], errors="coerce").fillna(0.0)

    schedules_df["due_date"] = pd.to_datetime(schedules_df["due_date"], errors="coerce")

    # ==========================================
    # ENRICH SCRIPT FROM LOAN/BORROWER SCHEMAS
    # ==========================================
    loan_label_map = {}
    borrower_id_map = {}
    borrower_name_map = {}

    if not borrowers_df.empty and "id" in borrowers_df.columns:
        borrowers_df["id"] = borrowers_df["id"].astype(str).str.strip()
        name_col = next((c for c in ["name", "full_name", "borrower_name"] if c in borrowers_df.columns), None)
        if name_col:
            borrower_name_map = dict(zip(borrowers_df["id"], borrowers_df[name_col]))

    if not loans_df.empty and "id" in loans_df.columns:
        loans_df["id"] = loans_df["id"].astype(str).str.strip()
        
        label_col = next((c for c in ["loan_id_label", "loan_no", "sn"] if c in loans_df.columns), None)
        if label_col:
            loan_label_map = dict(zip(loans_df["id"], loans_df[label_col].astype(str)))
            
        if "borrower_id" in loans_df.columns:
            loans_df["borrower_id"] = loans_df["borrower_id"].astype(str).str.strip()
            borrower_id_map = dict(zip(loans_df["id"], loans_df["borrower_id"]))

    # Vectorized mapping for maximum database throughput performance
    schedules_df["loan_label"] = schedules_df["loan_id"].map(loan_label_map).fillna("Unknown Loan")
    schedules_df["borrower_id"] = schedules_df["loan_id"].map(borrower_id_map).fillna("")
    schedules_df["borrower"] = schedules_df["borrower_id"].map(borrower_name_map).fillna("Unknown Borrower")

    # ==========================================
    # CORE TIME COMPARATOR TARGET
    # ==========================================
    today = pd.Timestamp.today().normalize()

    # ==========================================
    # PORTFOLIO SEGMENTATION FILTER MATRIX
    # ==========================================
    due_today_mask = (schedules_df["due_date"].dt.date == today.date()) & (schedules_df["balance"] > 0)
    overdue_mask = (schedules_df["due_date"] < today) & (schedules_df["balance"] > 0)
    upcoming_mask = (schedules_df["due_date"] > today) & (schedules_df["due_date"] <= today + pd.Timedelta(days=7)) & (schedules_df["balance"] > 0)

    total_due_today = schedules_df.loc[due_today_mask, "balance"].sum()
    total_overdue = schedules_df.loc[overdue_mask, "balance"].sum()
    total_upcoming = schedules_df.loc[upcoming_mask, "balance"].sum()

    # ==========================================
    # FINANCIAL METRICS CARDS GENERATOR
    # ==========================================
    c1, c2, c3 = st.columns(3)

    with c1:
        st.markdown(f"""
        <div class="calendar-metric-container" style="background: linear-gradient(135deg, #2563EB, #1D4ED8); box-shadow: 0 4px 14px rgba(37,99,235,0.25);">
            <div style="font-size:14px; font-weight:500;">📌 Due Today</div>
            <div style="font-size:28px; font-weight:700; margin-top:8px;">UGX {total_due_today:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        st.markdown(f"""
        <div class="calendar-metric-container" style="background: linear-gradient(135deg, #EF4444, #B91C1C); box-shadow: 0 4px 14px rgba(239,68,68,0.25);">
            <div style="font-size:14px; font-weight:500;">🔴 Overdue Portfolio</div>
            <div style="font-size:28px; font-weight:700; margin-top:8px;">UGX {total_overdue:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        st.markdown(f"""
        <div class="calendar-metric-container" style="background: linear-gradient(135deg, #10B981, #047857); box-shadow: 0 4px 14px rgba(16,185,129,0.25);">
            <div style="font-size:14px; font-weight:500;">⏳ Upcoming (7 Days)</div>
            <div style="font-size:28px; font-weight:700; margin-top:8px;">UGX {total_upcoming:,.0f}</div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # ==========================================
    # FULLCALENDAR COMPLIANT JSON OBJECT GENERATOR
    # ==========================================
    calendar_events = []
    active_schedules = schedules_df[schedules_df["balance"] > 0].dropna(subset=["due_date"])

    for _, row in active_schedules.iterrows():
        due_date = row["due_date"]
        
        # Color coding matrix assignment
        if due_date.date() < today.date():
            color = "#ef4444"  # Urgent Overdue Red
        elif due_date.date() == today.date():
            color = "#f59e0b"  # Due Today Amber
        else:
            color = "#2563eb"  # Standard Forward Blue

        calendar_events.append({
            "title": f"🧾 {row['loan_label']} | {row['borrower']} | UGX {row['balance']:,.0f}",
            "start": due_date.strftime("%Y-%m-%d"),
            "end": due_date.strftime("%Y-%m-%d"),
            "color": color,
            "allDay": True,
            "extendedProps": {
                "balance": float(row["balance"]),
                "status": row["status"]
            }
        })

    calendar_options = {
        "headerToolbar": {
            "left": "prev,next today",
            "center": "title",
            "right": "dayGridMonth,timeGridWeek"
        },
        "initialView": "dayGridMonth",
        "height": 680,
        "editable": False,
        "selectable": True
    }

    calendar(
        events=calendar_events,
        options=calendar_options,
        key="collections_calendar_v2"
    )

    st.divider()

    # ==========================================
    # UPCOMING 7-DAY LEDGER DATAFRAME
    # ==========================================
    st.subheader("⏳ Upcoming Collections Ledger")

    upcoming_df = schedules_df[upcoming_mask].copy()

    if upcoming_df.empty:
        st.info("No incoming schedule collections matching the 7-day parameters found.")
    else:
        upcoming_df["Days Left"] = (upcoming_df["due_date"] - today).dt.days
        upcoming_df = upcoming_df.sort_values(by="Days Left")
        
        # Human-readable presentation modifications
        upcoming_df["Due Date"] = upcoming_df["due_date"].dt.strftime("%d %b %Y")
        upcoming_df["Amount Due"] = upcoming_df["balance"].apply(lambda x: f"UGX {x:,.0f}")

        # Row styling handler callback
        def highlight_rows(row):
            days = row["Days Left"]
            if days <= 2:
                return ["background-color: #fee2e2; color: #991b1b; font-weight: 600;"] * len(row)
            elif days <= 5:
                return ["background-color: #fef3c7; color: #92400e; font-weight: 600;"] * len(row)
            else:
                return ["background-color: #dcfce7; color: #166534;"] * len(row)

        styled_output_df = (
            upcoming_df[["loan_label", "borrower", "Amount Due", "Due Date", "Days Left", "status"]]
            .style
            .apply(highlight_rows, axis=1)
        )

        st.dataframe(
            styled_output_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "loan_label": "Loan Ref",
                "borrower": "Borrower Name",
                "status": "Schedule Status"
            }
        )
