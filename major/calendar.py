import streamlit as st
import pandas as pd

from core.database import get_cached_data


def show_calendar():

    st.markdown(
        "## 📅 Activity Calendar",
        help="Loan due dates, collections, and risk overview"
    )

    # ==============================
    # LOAD DATA SAFELY
    # ==============================
    loans_df = get_cached_data("loans")

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    loans = loans_df.copy()

    # ==============================
    # CLEAN COLUMNS
    # ==============================
    loans.columns = loans.columns.str.strip().str.replace(" ", "_")

    required = ["End_Date", "Total_Repayable", "Status", "Borrower", "Loan_ID", "Principal", "Interest"]

    for col in required:
        if col not in loans.columns:
            loans[col] = 0 if col in ["Total_Repayable", "Principal", "Interest"] else "Unknown"

    loans["End_Date"] = pd.to_datetime(loans["End_Date"], errors="coerce")
    loans["Total_Repayable"] = pd.to_numeric(loans["Total_Repayable"], errors="coerce").fillna(0)
    loans["Principal"] = pd.to_numeric(loans["Principal"], errors="coerce").fillna(0)
    loans["Interest"] = pd.to_numeric(loans["Interest"], errors="coerce").fillna(0)

    today = pd.Timestamp.today().normalize()

    # ==============================
    # ACTIVE FILTER
    # ==============================
    active_loans = loans[
        loans["Status"].astype(str).str.lower() != "closed"
    ].copy()

    active_loans = active_loans[active_loans["End_Date"].notna()]

    # ==============================
    # SAFE AMOUNT FUNCTION
    # ==============================
    def get_amount(r):
        val = r.get("Total_Repayable", 0)
        if val and val > 0:
            return val
        return float(r.get("Principal", 0)) + float(r.get("Interest", 0))

    # ==============================
    # COLOR LOGIC (NO HTML, PURE UI)
    # ==============================
    def status_icon(row):
        if row["End_Date"] < today:
            return "🔴 OVERDUE"
        if row["End_Date"] <= today + pd.Timedelta(days=7):
            return "🟠 DUE SOON"
        return "🟢 ACTIVE"

    active_loans["status_view"] = active_loans.apply(status_icon, axis=1)

    # ==============================
    # KPI CARDS
    # ==============================
    due_today = active_loans[active_loans["End_Date"].dt.date == today.date()]
    upcoming = active_loans[
        (active_loans["End_Date"] > today) &
        (active_loans["End_Date"] <= today + pd.Timedelta(days=7))
    ]
    overdue = active_loans[active_loans["End_Date"] < today]

    c1, c2, c3 = st.columns(3)

    c1.metric("📅 Due Today", len(due_today))
    c2.metric("⏳ Upcoming (7 Days)", len(upcoming))
    c3.metric("🔴 Overdue", len(overdue))

    st.divider()

    # ==============================
    # CALENDAR LIST VIEW (COLORFUL BUT SAFE)
    # ==============================
    st.subheader("📆 Loan Schedule")

    display = active_loans.sort_values("End_Date").copy()

    display["Amount"] = display.apply(get_amount, axis=1)
    display["Due Date"] = display["End_Date"].dt.strftime("%Y-%m-%d")

    display = display[[
        "Loan_ID",
        "Borrower",
        "Amount",
        "Due Date",
        "status_view"
    ]].rename(columns={
        "Loan_ID": "Loan ID",
        "Borrower": "Borrower",
        "status_view": "Status"
    })

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Status": st.column_config.TextColumn(
                "Status",
                help="Loan urgency level"
            )
        }
    )

    # ==============================
    # TODAY ACTIONS (NO HTML)
    # ==============================
    st.subheader("📌 Action Items for Today")

    if due_today.empty:
        st.success("✨ No deadlines for today. Clean operations!")
    else:
        for _, r in due_today.iterrows():

            col1, col2, col3 = st.columns([3, 3, 2])

            col1.write(f"**{r['Loan_ID']}**")
            col2.write(r["Borrower"])
            col3.write(f"UGX {get_amount(r):,.0f}")

            st.caption(f"Status: 🔴 URGENT COLLECTION")
            st.button("Collect", key=f"collect_{r.name}")

    # ==============================
    # UPCOMING (SAFE + CLEAN)
    # ==============================
    st.subheader("⏳ Upcoming Deadlines (7 Days)")

    if upcoming.empty:
        st.info("No upcoming payments in the next 7 days.")
    else:
        for _, r in upcoming.sort_values("End_Date").iterrows():

            st.write(
                f"📅 **{r['End_Date'].strftime('%d %b %Y')}** — "
                f"{r['Borrower']} — "
                f"UGX {get_amount(r):,.0f}"
            )

    # ==============================
    # OVERDUE SECTION (COLORFUL BUT SAFE)
    # ==============================
    st.subheader("🔴 Overdue Loans")

    if overdue.empty:
        st.success("No overdue loans 🎉")
    else:

        overdue = overdue.copy()
        overdue["Days Late"] = (today - overdue["End_Date"]).dt.days
        overdue = overdue.sort_values("Days Late", ascending=False)

        for _, r in overdue.iterrows():

            severity = "🔴 CRITICAL" if r["Days Late"] > 7 else "🟠 WARNING"

            st.write(
                f"{severity} **{r['Loan_ID']}** — "
                f"{r['Borrower']} — "
                f"{r['Days Late']} days late — "
                f"UGX {get_amount(r):,.0f}"
            )
