# ==========================================
# 🚀 BALLISTIC FINTECH REPORTS ENGINE (PRODUCTION READY)
# ==========================================
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
# Core DB utilities
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas
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
    
    # Ensure numeric safety
    for col in ["principal", "interest", "cycle_no"]:
        if col in loans.columns:
            loans[col] = pd.to_numeric(loans[col], errors="coerce").fillna(0)
    
    # Normalize status
    loans["status"] = loans["status"].astype(str).str.upper()
    
    # ==============================
    # 🧠 ACTIVE CAPITAL (Cycle 1 ONLY, Active/Pending Loans)
    # ==============================
    active_capital_loans = loans[
        (loans["cycle_no"] == 1) &
        (loans["status"].isin(["ACTIVE", "PENDING"]))
    ]
    
    total_capital_out = active_capital_loans["principal"].sum()
    
    # ==============================
    # 💰 INT. REVENUE (ONLY CLEARED LOANS, ALL CYCLES INCLUDED)
    # ==============================
    cleared_loans = loans[loans["status"] == "CLEARED"]
    
    projected_interest = cleared_loans["interest"].sum()
    
    # ==============================
    # 💵 COLLECTIONS (UNCHANGED)
    # ==============================
    actual_collected = col_sum(payments, "amount")
    
    # ==============================
    # 💸 OPEX (UNCHANGED)
    # ==============================
    direct_expenses = col_sum(expenses, "amount")
    nssf_tax = col_sum(payroll, "nssf_5") + col_sum(payroll, "nssf_10")
    paye_tax = col_sum(payroll, "paye")
    salary_net = col_sum(payroll, "net_pay")
    
    petty_out = 0
    if not petty.empty and "type" in petty.columns:
        petty_out = col_sum(petty[petty["type"] == "Out"], "amount")
    
    total_opex = direct_expenses + petty_out + nssf_tax + paye_tax + salary_net
    
    # ==============================
    # 📊 NET CASHFLOW (UNCHANGED)
    # ==============================
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
    # 🧾 FINANCIAL STATEMENTS (CYCLE-AWARE)
    # ==============================
    s1, s2 = st.columns(2)
    
    # ------------------------------
    # Compute fiscal year: July–June
    # ------------------------------
    def fiscal_year(dt):
        if pd.isna(dt):
            return "Unknown"
        return f"{dt.year}-{dt.year+1}" if dt.month >= 7 else f"{dt.year-1}-{dt.year}"
    
    loans["start_date"] = pd.to_datetime(loans.get("start_date"), errors="coerce")
    payments["date"] = pd.to_datetime(payments.get("date"), errors="coerce")
    expenses["date"] = pd.to_datetime(expenses.get("date"), errors="coerce")
    
    loans["fiscal_year"] = loans["start_date"].apply(fiscal_year)
    payments["fiscal_year"] = payments["date"].apply(fiscal_year)
    expenses["fiscal_year"] = expenses["date"].apply(fiscal_year)
    
    fiscal_years = sorted(loans["fiscal_year"].dropna().unique())
    
    # ==============================
    # 💰 INCOME STATEMENT & BALANCE SHEET (FY-AWARE)
    # ==============================
    
    # ------------------------------
    # Fiscal Year Selector
    # ------------------------------
    fiscal_years = sorted(loans["fiscal_year"].dropna().unique())
    selected_fy = st.selectbox("Select Financial Year", fiscal_years)
    
    fy_loans = loans[loans["fiscal_year"] == selected_fy]
    fy_expenses = expenses[expenses["fiscal_year"] == selected_fy]
    fy_payments = payments[payments["fiscal_year"] == selected_fy]
    
    # ------------------------------
    # 💰 INCOME STATEMENT
    # ------------------------------
    with s1:
        st.subheader(f"💰 Income Statement (Profit & Loss) — FY {selected_fy}")
    
        # Active Capital → Cycle 1 PENDING/ACTIVE only
        active_capital = fy_loans[
            (fy_loans["cycle_no"] == 1) &
            (fy_loans["status"].str.upper().isin(["ACTIVE", "PENDING"]))
        ]["principal"].sum()
    
        # Interest Revenue → CLEARED loans (all cycles)
        int_revenue = fy_loans[
            fy_loans["status"].str.upper() == "CLEARED"
        ]["interest"].sum()
    
        # OPEX → Direct expenses only (salaries/taxes already included)
        total_opex = col_sum(fy_expenses, "amount")
    
        # Net Profit
        net_profit = int_revenue - total_opex
    
        st.dataframe(pd.DataFrame({
            "Description": [
                "Active Capital (Cycle 1 ACTIVE/PENDING)",
                "Interest Revenue (CLEARED Loans)",
                "Total Operating Expenses (OPEX)",
                "Net Profit"
            ],
            "amount (UGX)": [
                f"{active_capital:,.0f}",
                f"{int_revenue:,.0f}",
                f"{total_opex:,.0f}",
                f"{net_profit:,.0f}"
            ]
        }), use_container_width=True)
    
    # ------------------------------
    # 🏦 BALANCE SHEET SNAPSHOT
    # ------------------------------
    with s2:
        st.subheader(f"🏦 Balance Sheet — FY {selected_fy}")
    
        # Loan Book → all cycles outstanding balances
        loan_book_value = fy_loans["balance"].sum()
    
        # Cash Position → payments minus expenses
        cash_position = col_sum(fy_payments, "amount") - col_sum(fy_expenses, "amount")
    
        # Total Assets = Active Capital + Loan Book + Cash Position
        total_assets = active_capital + loan_book_value + cash_position
    
        st.dataframe(pd.DataFrame({
            "Description": [
                "Active Capital (Cycle 1 ACTIVE/PENDING)",
                "Loan Book (All Outstanding Cycles)",
                "Cash Position",
                "Total Assets"
            ],
            "amount (UGX)": [
                f"{active_capital:,.0f}",
                f"{loan_book_value:,.0f}",
                f"{cash_position:,.0f}",
                f"{total_assets:,.0f}"
            ]
        }), use_container_width=True)
    
    # ------------------------------
    # 📤 EXPORT
    # ------------------------------
    with st.expander(f"📥 Export Executive Report — FY {selected_fy}"):
    
        export_rows = [{
            "Fiscal Year": selected_fy,
            "Active Capital": active_capital,
            "Interest Revenue": int_revenue,
            "Total OPEX": total_opex,
            "Net Profit": net_profit,
            "Cash Position": cash_position,
            "Loan Book Value": loan_book_value,
            "Total Assets": total_assets
        }]
    
        export_df = pd.DataFrame(export_rows)
        st.dataframe(export_df)
    
        csv = export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download Full Executive Report (CSV)",
            data=csv,
            file_name=f"FinReport_{selected_fy}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
