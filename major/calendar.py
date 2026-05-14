import streamlit as st
import pandas as pd

from core.database import get_cached_data


def show_calendar():

    st.markdown("## 📅 Loan Operations Center")

    # ==============================
    # LOAD DATA
    # ==============================
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("No active loan data available.")
        return

    loans = loans_df.copy()

    # ==============================
    # CLEAN CORE FIELDS
    # ==============================
    loans["status"] = loans.get("status", "").astype(str).str.upper()
    loans["balance"] = pd.to_numeric(loans.get("balance", 0), errors="coerce").fillna(0)
    loans["total_repayable"] = pd.to_numeric(loans.get("total_repayable", 0), errors="coerce").fillna(0)

    loans["end_date"] = pd.to_datetime(loans.get("end_date"), errors="coerce")

    today = pd.Timestamp.today().normalize()

    # ==============================
    # BORROWER MAPPING
    # ==============================
    if borrowers_df is not None and not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans["borrower"] = loans.get("borrower_id", "").astype(str).map(bor_map)
        loans["borrower"] = loans["borrower"].fillna("Unknown")
    else:
        loans["borrower"] = "Unknown"

    # ==============================
    # FILTER ACTIVE LOANS
    # ==============================
    active = loans[
        ~loans["status"].isin(["CLEARED", "BCF", "CLOSED"])
    ].copy()

    # ==============================
    # 🔥 RISK ENGINE (CORE UPGRADE)
    # ==============================
    def risk_score(row):
        if pd.isna(row["end_date"]):
            return 0

        days = (row["end_date"] - today).days

        if row["status"] in ["DEFAULT"]:
            return 100

        if days < 0:
            return 90

        if days <= 3:
            return 75

        if days <= 7:
            return 50

        return 10

    active["risk_score"] = active.apply(risk_score, axis=1)

    # ==============================
    # COLOR CLASSIFICATION
    # ==============================
    def priority_label(score):
        if score >= 90:
            return "🔴 CRITICAL"
        if score >= 70:
            return "🟠 HIGH"
        if score >= 40:
            return "🟡 MEDIUM"
        return "🟢 LOW"

    active["priority"] = active["risk_score"].apply(priority_label)

    # ==============================
    # KPI STRIP (FAST INSIGHT)
    # ==============================
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("🔴 Critical Loans", (active["risk_score"] >= 90).sum())
    col2.metric("🟠 High Risk", (active["risk_score"].between(70, 89)).sum())
    col3.metric("🟡 Medium Risk", (active["risk_score"].between(40, 69)).sum())
    col4.metric("💰 Total Exposure", f"UGX {active['total_repayable'].sum():,.0f}")

    st.divider()

    # ==============================
    # SORTED OPERATIONS TABLE
    # ==============================
    st.subheader("📊 Loan Priority Queue (Ops View)")

    view = active.sort_values("risk_score", ascending=False)[
        ["borrower", "total_repayable", "balance", "end_date", "risk_score", "priority"]
    ].copy()

    view["end_date"] = view["end_date"].dt.strftime("%Y-%m-%d")

    view = view.rename(columns={
        "borrower": "Borrower",
        "total_repayable": "Total Due",
        "balance": "Outstanding",
        "end_date": "Due Date",
        "risk_score": "Risk Score",
        "priority": "Priority"
    })

    st.dataframe(
        view,
        use_container_width=True,
        hide_index=True
    )

    # ==============================
    # BORROWER DRILLDOWN
    # ==============================
    st.subheader("👤 Borrower Exposure Analysis")

    borrower_summary = active.groupby("borrower").agg(
        loans=("borrower", "count"),
        exposure=("total_repayable", "sum"),
        outstanding=("balance", "sum"),
        avg_risk=("risk_score", "mean")
    ).reset_index()

    borrower_summary = borrower_summary.sort_values("exposure", ascending=False)

    st.dataframe(
        borrower_summary,
        use_container_width=True,
        hide_index=True
    )

    # ==============================
    # TODAY OPS PANEL
    # ==============================
    st.subheader("📌 Today's Priority Actions")

    today_ops = active[active["risk_score"] >= 70].copy()

    if today_ops.empty:
        st.success("All systems stable. No urgent collections today.")
    else:
        for _, r in today_ops.iterrows():

            left, mid, right = st.columns([3, 3, 2])

            left.write(f"{r['priority']} **{r['borrower']}**")
            mid.write(f"UGX {r['total_repayable']:,.0f}")
            right.button("Collect", key=f"ops_{r.name}")

    # ==============================
    # HEALTH SUMMARY FOOTER
    # ==============================
    st.divider()

    st.markdown(
        f"""
        ### 🧠 Portfolio Health Summary

        - Total Active Loans: **{len(active)}**
        - High Risk Exposure: **UGX {active[active['risk_score'] >= 70]['total_repayable'].sum():,.0f}**
        - Portfolio Risk Weighted Score: **{active['risk_score'].mean():.1f}**
        """
    )
