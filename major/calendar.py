import streamlit as st
import pandas as pd

from core.database import get_cached_data


def show_calendar():

    st.markdown(
        "## 📅 Activity Calendar",
        help="Loan repayment tracking dashboard"
    )

    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("📅 No active loans in the system.")
        return

    loans = loans_df.copy()

    # ==============================
    # CLEAN DATA
    # ==============================
    loans["status"] = loans.get("status", "").astype(str).str.upper()
    loans["balance"] = pd.to_numeric(loans.get("balance", 0), errors="coerce").fillna(0)
    loans["total_repayable"] = pd.to_numeric(loans.get("total_repayable", 0), errors="coerce").fillna(0)

    loans["end_date"] = pd.to_datetime(loans.get("end_date"), errors="coerce")

    today = pd.Timestamp.today().normalize()

    # ==============================
    # STATUS COLOR ENGINE 🎨
    # ==============================
    def status_style(row):
        if pd.isna(row["end_date"]):
            return "⚪ Unknown"

        if row["status"] in ["CLEARED", "BCF"]:
            return "🟢 Completed"

        if row["end_date"] < today:
            return "🔴 Overdue"

        if row["end_date"] <= today + pd.Timedelta(days=7):
            return "🟠 Due Soon"

        return "🟢 Active"

    loans["calendar_status"] = loans.apply(status_style, axis=1)

    # ==============================
    # METRICS (COLORED FEEL)
    # ==============================
    overdue = loans[loans["calendar_status"] == "🔴 Overdue"]
    due_soon = loans[loans["calendar_status"] == "🟠 Due Soon"]
    active = loans[loans["calendar_status"] == "🟢 Active"]

    c1, c2, c3, c4 = st.columns(4)

    c1.metric("🔴 Overdue", len(overdue))
    c2.metric("🟠 Due Soon", len(due_soon))
    c3.metric("🟢 Active", len(active))
    c4.metric("📊 Total Loans", len(loans))

    st.divider()

    # ==============================
    # COLORFUL CALENDAR TABLE (NO HTML)
    # ==============================
    st.subheader("📆 Loan Schedule Overview")

    view = loans[loans["end_date"].notna()].copy()

    view = view.sort_values("end_date")

    view["Due Date"] = view["end_date"].dt.strftime("%Y-%m-%d")
    view["Borrower"] = view.get("borrower", "Unknown")
    view["Amount"] = view["total_repayable"].apply(lambda x: f"UGX {x:,.0f}")
    view["Status"] = view["calendar_status"]

    view = view[["Borrower", "Amount", "Due Date", "Status"]]

    # Streamlit native styling (SAFE)
    def color_status(val):
        if "Overdue" in val:
            return "background-color:#FEE2E2; color:#991B1B; font-weight:bold;"
        if "Due Soon" in val:
            return "background-color:#FFF7ED; color:#9A3412; font-weight:bold;"
        if "Active" in val:
            return "background-color:#ECFDF5; color:#065F46; font-weight:bold;"
        if "Completed" in val:
            return "background-color:#EFF6FF; color:#1E3A8A; font-weight:bold;"
        return ""

    styled = (
        view.style
        .applymap(color_status, subset=["Status"])
        .format({"Amount": "{}"})
    )

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True
    )

    # ==============================
    # TODAY PANEL (COLORED BUT CLEAN)
    # ==============================
    st.subheader("📌 Today's Collection Focus")

    today_loans = loans[loans["end_date"].dt.date == today.date()]

    if today_loans.empty:
        st.success("✨ No collections due today — clean slate!")
    else:
        for _, r in today_loans.iterrows():

            status_icon = "🔴" if r["end_date"] < today else "🟠"

            col1, col2, col3 = st.columns([3, 3, 2])

            col1.write(f"{status_icon} **{r.get('borrower','Unknown')}**")
            col2.write(f"UGX {r['total_repayable']:,.0f}")
            col3.button("Collect", key=f"collect_{r.name}")
