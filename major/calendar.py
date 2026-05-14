import streamlit as st
import pandas as pd

from core.database import get_cached_data


def show_calendar():

    st.markdown("## 📅 Activity Calendar")

    # ==============================
    # DATA LOAD
    # ==============================
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is empty — no active loans.")
        return

    loans = loans_df.copy()

    # ==============================
    # CLEAN CORE FIELDS
    # ==============================
    loans["sn"] = loans.get("sn", "").astype(str).str.strip().str.upper()
    loans["status"] = loans.get("status", "").astype(str).str.upper()

    loans["balance"] = pd.to_numeric(loans.get("balance", 0), errors="coerce").fillna(0)
    loans["total_repayable"] = pd.to_numeric(loans.get("total_repayable", 0), errors="coerce").fillna(0)

    loans["cycle_no"] = pd.to_numeric(loans.get("cycle_no", 1), errors="coerce").fillna(1)

    loans["end_date"] = pd.to_datetime(loans.get("end_date"), errors="coerce")
    loans["start_date"] = pd.to_datetime(loans.get("start_date"), errors="coerce")

    # ==============================
    # SMART STATUS FIX
    # ==============================
    loans = loans.sort_values(["sn", "cycle_no", "start_date"])

    for sn, grp in loans.groupby("sn"):
        idx = grp.index.tolist()

        if len(idx) > 1:
            loans.loc[idx[:-1], "status"] = "BCF"

        last = idx[-1]
        if abs(loans.at[last, "balance"]) < 1:
            loans.at[last, "status"] = "CLEARED"

    loans.loc[
        (loans["balance"] <= 0) & (loans["status"] != "BCF"),
        "status"
    ] = "CLEARED"

    # ==============================
    # BORROWER MAPPING
    # ==============================
    if borrowers_df is not None and not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans["borrower"] = loans.get("borrower_id", "").astype(str).map(bor_map)
        loans["borrower"] = loans["borrower"].fillna("Unknown borrower")
    else:
        loans["borrower"] = "Unknown borrower"

    # ==============================
    # ACTIVE LOANS ONLY
    # ==============================
    active_loans = loans[
        ~loans["status"].isin(["CLEARED", "BCF", "CLOSED"])
    ].copy()

    today = pd.Timestamp.today().normalize()

    # ==============================
    # CALENDAR DATA
    # ==============================
    calendar_df = active_loans[active_loans["end_date"].notna()].copy()

    calendar_df["overdue"] = calendar_df["end_date"] < today

    # ==============================
    # METRICS (NO HTML)
    # ==============================
    due_today = calendar_df[calendar_df["end_date"].dt.date == today.date()]
    upcoming = calendar_df[
        (calendar_df["end_date"] > today) &
        (calendar_df["end_date"] <= today + pd.Timedelta(days=7))
    ]
    overdue = calendar_df[calendar_df["overdue"]]

    c1, c2, c3 = st.columns(3)

    c1.metric("Due Today", len(due_today))
    c2.metric("Upcoming (7 days)", len(upcoming))
    c3.metric("Overdue", len(overdue))

    st.divider()

    # ==============================
    # CALENDAR TABLE (SAFE STREAMLIT UI)
    # ==============================
    st.subheader("📆 Upcoming Deadlines")

    if calendar_df.empty:
        st.info("No scheduled repayments found.")
    else:
        display = calendar_df.sort_values("end_date")[[
            "borrower",
            "total_repayable",
            "balance",
            "end_date",
            "status"
        ]].copy()

        display["end_date"] = display["end_date"].dt.strftime("%Y-%m-%d")

        display = display.rename(columns={
            "borrower": "Borrower",
            "total_repayable": "Total Due",
            "balance": "Balance",
            "end_date": "Due Date",
            "status": "Status"
        })

        st.dataframe(
            display,
            use_container_width=True,
            hide_index=True
        )

    # ==============================
    # TODAY ACTION LIST (NO HTML TABLE)
    # ==============================
    st.subheader("📌 Action Items (Today)")

    if due_today.empty:
        st.success("No collections due today.")
    else:
        for _, r in due_today.iterrows():
            col_a, col_b, col_c, col_d = st.columns([2, 3, 2, 2])

            col_a.write(f"**{str(r.get('id', 'N/A'))[:8]}**")
            col_b.write(r["borrower"])
            col_c.write(f"UGX {r['total_repayable']:,.0f}")
            col_d.button("Collect", key=f"collect_{r.name}")
