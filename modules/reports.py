# ==========================================
# 🚀 BALLISTIC FINTECH REPORTS ENGINE (PRODUCTION READY)
# ==========================================
import plotly.express as px
import pandas as pd
import streamlit as st
from datetime import datetime

def show_reports():
    """
    Advanced financial reporting with multi-tenant isolation 
    and investor-grade intelligence metrics.
    """

    # ==============================
    # 🎨 HEADER (EXECUTIVE UI)
    # ==============================
    st.markdown("""
    <div style='background: linear-gradient(90deg,#1E3A8A,#2B3F87); padding:20px; border-radius:15px; margin-bottom:25px;'>
        <h2 style='margin:0; color:white; font-size:24px;'>📊 Financial Intelligence Dashboard</h2>
        <p style='margin:0; color:#DBEAFE; font-size:13px;'>Real-time P&L, Balance Sheet, and Portfolio Yield Analysis</p>
    </div>
    """, unsafe_allow_html=True)

    tenant = st.session_state.get("tenant_id")
    if not tenant:
        st.error("Session Expired.")
        return

    # ==============================
    # 🛡️ DATA FETCH & TENANT SAFETY
    # ==============================
    def safe_tenant_filter(df_name):
        try:
            df = get_cached_data(df_name)
            if df is None or df.empty:
                return pd.DataFrame()
            if "tenant_id" in df.columns:
                return df[df["tenant_id"].astype(str) == str(tenant)].copy()
            return df
        except Exception:
            return pd.DataFrame()

    loans = safe_tenant_filter("loans")
    payments = safe_tenant_filter("payments")
    expenses = safe_tenant_filter("expenses")
    payroll = safe_tenant_filter("payroll")
    petty = safe_tenant_filter("petty_cash")
    borrowers = safe_tenant_filter("borrowers")

    if loans.empty:
        st.info("💡 No loan data found. Financial reports will populate once loans are issued.")
        return

    # ==============================
    # 🧰 REUSABLE HELPERS
    # ==============================
    def col_sum(df, col):
        if df.empty or col not in df.columns: return 0.0
        return pd.to_numeric(df[col], errors="coerce").fillna(0).sum()

    def to_numeric_series(df, col):
        if df.empty or col not in df.columns: return pd.Series(dtype=float)
        return pd.to_numeric(df[col], errors="coerce").fillna(0)

    def attach_borrower_names(loans_df, borrowers_df):
        if borrowers_df.empty or "id" not in borrowers_df.columns or "name" not in borrowers_df.columns:
            loans_df["borrower"] = loans_df.get("borrower", "Unknown")
            return loans_df
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        mapped = loans_df["borrower_id"].map(bor_map)
        loans_df["borrower"] = mapped.fillna(loans_df.get("borrower")).fillna("Unknown")
        return loans_df

    loans = attach_borrower_names(loans, borrowers)

    # ==============================
    # 🔢 CORE FINANCIAL ACCOUNTING
    # ==============================
    total_capital_out = col_sum(loans, "principal")
    projected_interest = col_sum(loans, "interest")
    actual_collected = col_sum(payments, "amount")

    direct_expenses = col_sum(expenses, "amount")
    nssf_tax = col_sum(payroll, "nssf_5") + col_sum(payroll, "nssf_10")
    paye_tax = col_sum(payroll, "paye")
    salary_net = col_sum(payroll, "net_pay")
    petty_out = col_sum(petty[petty.get("type") == "Out"], "amount") if not petty.empty else 0

    total_opex = direct_expenses + petty_out + nssf_tax + paye_tax + salary_net

    cash_profit = actual_collected - total_opex

    # ==============================
    # 💎 KPI TILES
    # ==============================
    def render_kpi(title, value, color, icon="💰"):
        st.markdown(f"""
            <div style="padding:16px; border-radius:12px; background:white; border:1px solid #E5E7EB; box-shadow:0 2px 4px rgba(0,0,0,0.02); margin-bottom:10px;">
                <p style="font-size:11px; color:#6B7280; margin:0; font-weight:600;">{icon} {title}</p>
                <h3 style="margin:0; color:{color}; font-size:20px;">UGX {value:,.0f}</h3>
            </div>
        """, unsafe_allow_html=True)

    k1, k2, k3, k4 = st.columns(4)
    with k1: render_kpi("ACTIVE CAPITAL", total_capital_out, "#1E3A8A")
    with k2: render_kpi("INT. REVENUE", projected_interest, "#059669")
    with k3: render_kpi("COLLECTIONS", actual_collected, "#7C3AED")
    with k4: render_kpi("NET CASHFLOW", cash_profit, "#059669" if cash_profit >= 0 else "#DC2626")

    # ==============================
    # 📈 TREND ANALYSIS
    # ==============================
    st.markdown("### 📈 Monthly Profit & Loss Trend", unsafe_allow_html=True)

    payments["date"] = pd.to_datetime(payments.get("date"), errors="coerce")
    expenses["date"] = pd.to_datetime(expenses.get("date"), errors="coerce")

    if not payments.empty:
        inc_m = payments.set_index("date").resample("ME")["amount"].sum()
        inc_m.name = "Income"
    else:
        inc_m = pd.Series(dtype=float, name="Income")

    if not expenses.empty:
        exp_m = expenses.set_index("date").resample("ME")["amount"].sum()
        exp_m.name = "Expenses"
    else:
        exp_m = pd.Series(dtype=float, name="Expenses")
    pl_combined = pd.concat([inc_m, exp_m], axis=1).fillna(0)
    pl_combined["Net"] = pl_combined["Income"] - pl_combined["Expenses"]

    if not pl_combined.empty:
        fig_trend = px.area(
            pl_combined,
            color_discrete_map={"Income": "#059669", "Expenses": "#EF4444", "Net": "#1E3A8A"},
            line_shape="spline",
            labels={"index":"Month", "value":"UGX"}
        )
        fig_trend.update_layout(
            hovermode="x unified",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("📉 No trend data available yet.")

    # ==============================
    # 🧠 INVESTOR INTELLIGENCE METRICS
    # ==============================
    overdue_loans = loans[loans["status"].astype(str).str.upper().str.contains("OVERDUE", na=False)]
    par_value = col_sum(overdue_loans, "balance")
    par_ratio = (par_value / total_capital_out * 100) if total_capital_out > 0 else 0
    yield_pct = (projected_interest / total_capital_out * 100) if total_capital_out > 0 else 0
    coll_eff = (actual_collected / (total_capital_out + projected_interest) * 100) if (total_capital_out + projected_interest) > 0 else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Portfolio Yield", f"{yield_pct:.1f}%")
    m2.metric("Collection Eff.", f"{coll_eff:.1f}%")
    m3.metric("PAR Ratio", f"{par_ratio:.1f}%", delta=f"{par_ratio:.1f}%", delta_color="inverse")
    m4.metric("OpEx Ratio", f"{(total_opex/actual_collected*100 if actual_collected > 0 else 0):.1f}%")

    # ==============================
    # 🧾 STATEMENTS
    # ==============================
    s1, s2 = st.columns(2)

    with s1:
        st.markdown("#### 💰 Income Statement (OpEx)")
        st.markdown(f"""
        <div style="background:#F9FAFB; padding:20px; border-radius:12px; border:1px solid #E5E7EB">
            <small>REVENUE (Projected interest)</small><br><b>UGX {projected_interest:,.0f}</b><hr>
            <small>OPERATIONAL COSTS</small><br><b>UGX {total_opex:,.0f}</b><br>
            <p style="font-size:12px; color:#666;">Includes Salaries, Taxes, Petty Cash & Admin Expenses</p>
            <h4 style="color:#1E3A8A; margin-top:10px;">TRUE NET: UGX {(projected_interest - total_opex):,.0f}</h4>
        </div>
        """, unsafe_allow_html=True)

    with s2:
        st.markdown("#### 🧾 Balance Sheet Position")
        loan_book_value = col_sum(loans, "balance")
        total_assets = cash_profit + loan_book_value
        st.markdown(f"""
        <div style="background:#F9FAFB; padding:20px; border-radius:12px; border:1px solid #E5E7EB">
            <small>CASH AT HAND</small><br><b>UGX {cash_profit:,.0f}</b><hr>
            <small>LOAN BOOK (Active Receivables)</small><br><b>UGX {loan_book_value:,.0f}</b><br>
            <p style="font-size:12px; color:#666;">Current value of all outstanding principal + interest</p>
            <h4 style="color:#059669; margin-top:10px;">TOTAL ASSETS: UGX {total_assets:,.0f}</h4>
        </div>
        """, unsafe_allow_html=True)

    # ==============================
    # 📤 DATA EXPORT
    # ==============================
    with st.expander("📥 Export Financial Data for Auditors"):
        report_data = {
            "Metric": ["Capital Out", "Interest Revenue", "Total OpEx", "Cash Profit", "Portfolio Yield %", "PAR %"],
            "Value": [total_capital_out, projected_interest, total_opex, cash_profit, f"{yield_pct:.2f}%", f"{par_ratio:.2f}%"]
        }
        export_df = pd.DataFrame(report_data)
        st.table(export_df)

        csv = export_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Download Full Executive Report",
            data=csv,
            file_name=f"FinReport_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
