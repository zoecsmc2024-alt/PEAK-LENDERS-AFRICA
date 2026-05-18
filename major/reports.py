# ==========================================
# 🚀 BALLISTIC FINTECH REPORTS ENGINE (FIXED)
# ==========================================
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

from core.database import get_cached_data


# -----------------------------
# HELPER: REALIZED INTEREST CALC (Hoisted to Top to prevent NameError)
# -----------------------------
def compute_realized_interest(loans_df, payments_df):
    if loans_df.empty:
        return 0.0
    loans_df = loans_df.copy()
    payments_df = payments_df.copy()

    for col in ["principal", "interest"]:
        if col not in loans_df.columns:
            loans_df[col] = 0.0
            
    if payments_df.empty or "amount" not in payments_df.columns:
        payments_df["amount"] = 0.0

    loan_pay = payments_df.groupby("loan_id")["amount"].sum().to_dict()
    loans_df["loan_total"] = loans_df["principal"] + loans_df["interest"]

    def calc(row):
        paid = loan_pay.get(row.get("id"), 0.0)
        if row["loan_total"] <= 0:
            return 0.0
        # Determine how much of the payment is interest yield vs capital recovery
        ratio = min(paid / row["loan_total"], 1.0)
        return row["interest"] * ratio

    return float(loans_df.apply(calc, axis=1).sum())


# -----------------------------
# MAIN MODULE VIEW
# -----------------------------
def show_reports():

    st.markdown("""
    <div style='background: linear-gradient(90deg,#1E3A8A,#2B3F87); padding:20px; border-radius:15px; margin-bottom:25px;'>
        <h2 style='margin:0; color:white; font-size:24px;'>📊 Financial Intelligence Dashboard</h2>
        <p style='margin:0; color:#DBEAFE; font-size:13px;'>Real-time P&L, Balance Sheet, Portfolio Analytics</p>
    </div>
    """, unsafe_allow_html=True)

    tenant = st.session_state.get("tenant_id")
    if not tenant:
        st.error("Session Expired.")
        return

    # ==============================
    # ⚡ FAST TENANT CACHE FILTER
    # ==============================
    @st.cache_data(ttl=120, show_spinner=False)
    def safe_tenant_filter(df_name):
        try:
            df = get_cached_data(df_name)
            if df is None or df.empty:
                return pd.DataFrame()
            if "tenant_id" in df.columns:
                return df[df["tenant_id"].astype(str) == str(tenant)].copy()
            return df
        except:
            return pd.DataFrame()

    loans = safe_tenant_filter("loans")
    payments = safe_tenant_filter("payments")
    expenses = safe_tenant_filter("expenses")
    payroll = safe_tenant_filter("payroll")

    if loans.empty:
        st.info("No loan portfolio data available yet to build telemetry.")
        return

    # ==============================
    # 🧼 CLEAN & UNIQUE DATA ENGINE
    # ==============================
    loans = loans.copy()
    for c in ["principal", "interest", "cycle_no", "balance"]:
        if c in loans.columns:
            loans[c] = pd.to_numeric(loans[c], errors="coerce").fillna(0.0)
    
    loans["status"] = loans["status"].astype(str).str.upper()

    # Create a "Current State" dataframe to avoid double-counting active cycles
    latest_loans = (
        loans.sort_values(["sn", "cycle_no"], ascending=True)
        .drop_duplicates(subset=["sn"], keep="last")
    )

    def sum_col(df, col):
        if df.empty or col not in df.columns:
            return 0.0
        return float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())

    # ==============================
    # 💰 GLOBAL AGGREGATIONS
    # ==============================
    # Only pull principal balances from current active book state
    active_capital = latest_loans[
        latest_loans["status"].isin(["ACTIVE", "PENDING", "OVERDUE"])
    ]["principal"].sum()

    # Calculate realized historical earnings
    projected_interest = compute_realized_interest(loans, payments)

    # Operational Expense Calculations
    actual_collected = sum_col(payments, "amount")
    direct_expenses = sum_col(expenses, "amount")
    
    nssf = sum_col(payroll, "nssf_5") + sum_col(payroll, "nssf_10")
    paye = sum_col(payroll, "paye")
    salary = sum_col(payroll, "net_pay")
    total_opex = direct_expenses + nssf + paye + salary
    
    net_profit_global = projected_interest - total_opex
    cash_profit = actual_collected - total_opex

    # ==============================
    # 💳 KPI METRIC DISPLAY
    # ==============================
    def kpi(title, value, color):
        st.markdown(f"""
        <div style="padding:16px;border-radius:12px;background:#fff;border:1px solid #eee;box-shadow: 0px 2px 4px rgba(0,0,0,0.02)">
            <div style="font-size:11px;color:#666;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;">{title}</div>
            <div style="font-size:18px;color:{color};font-weight:700;margin-top:4px;">
                UGX {value:,.0f}
            </div>
        </div>
        """, unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1: kpi("Active Capital", active_capital, "#1E3A8A")
    with c2: kpi("Realised Interest", projected_interest, "#059669")
    with c3: kpi("Accrual Net Profit", net_profit_global, "#059669" if net_profit_global >= 0 else "#DC2626")
    with c4: kpi("Net Cashflow", cash_profit, "#7C3AED")

    # ==============================
    # 📈 TREND TIME RE-SAMPLING ENGINE
    # ==============================
    st.write("")
    payments_t = payments.copy()
    expenses_t = expenses.copy()
    
    payments_t["date"] = pd.to_datetime(payments_t.get("date"), errors="coerce")
    expenses_t["date"] = pd.to_datetime(expenses_t.get("date"), errors="coerce")

    inc = pd.Series(dtype=float)
    exp = pd.Series(dtype=float)

    if not payments_t.empty:
        inc = payments_t.dropna(subset=["date"]).set_index("date").resample("ME")["amount"].sum()

    if not expenses_t.empty:
        exp = expenses_t.dropna(subset=["date"]).set_index("date").resample("ME")["amount"].sum()

    df_trend = pd.concat([inc, exp], axis=1).fillna(0.0)
    df_trend.columns = ["Income", "Expenses"]
    df_trend["Net Cashflow"] = df_trend["Income"] - df_trend["Expenses"]

    if not df_trend.empty:
        fig = px.area(df_trend, x=df_trend.index, y=["Income", "Expenses", "Net Cashflow"], 
                      color_discrete_map={"Income": "#10B981", "Expenses": "#EF4444", "Net Cashflow": "#3B82F6"},
                      title="Monthly Liquidity Inflow vs Outflow Trend")
        fig.update_layout(height=280, margin=dict(l=0, r=0, t=35, b=0), legend=dict(orientation="h", y=1.1, x=0))
        st.plotly_chart(fig, use_container_width=True)

    # ==============================
    # 🛑 RISK METRICS (PAR / YIELD)
    # ==============================
    overdue = latest_loans[latest_loans["status"].str.contains("OVERDUE", na=False)]
    par = sum_col(overdue, "balance")
    
    par_ratio = (par / active_capital * 100) if active_capital else 0.0
    yield_pct = (projected_interest / active_capital * 100) if active_capital else 0.0
    eff = (actual_collected / (active_capital + projected_interest) * 100) if (active_capital + projected_interest) else 0.0
    opex_ratio = (total_opex / actual_collected * 100) if actual_collected else 0.0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Portfolio Yield", f"{yield_pct:.1f}%")
    m2.metric("Collection Efficiency", f"{eff:.1f}%")
    m3.metric("PAR (Portfolio at Risk)", f"{par_ratio:.1f}%")
    m4.metric("Opex Efficiency Ratio", f"{opex_ratio:.1f}%")

    # ==============================
    # 📅 FINANCIAL STATEMENT LEDGER (JULY - JUNE CYCLES)
    # ==============================
    st.write("---")
    
    def calculate_ugandan_fy(dt):
        if pd.isna(dt): 
            return "Unknown"
        # If month is July (7) or later, it marks the start of the next FY block
        return f"FY {dt.year}/{str(dt.year+1)[2:]}" if dt.month >= 7 else f"FY {dt.year-1}/{str(dt.year)[2:]}"

    # Standardize structural timestamps for grouping
    loans["start_date"] = pd.to_datetime(loans.get("start_date"), errors="coerce")
    payments["date"] = pd.to_datetime(payments.get("date"), errors="coerce")
    expenses["date"] = pd.to_datetime(expenses.get("date"), errors="coerce")
    payroll["created_at"] = pd.to_datetime(payroll.get("created_at"), errors="coerce")

    loans["fiscal_year"] = loans["start_date"].apply(calculate_ugandan_fy)
    payments["fiscal_year"] = payments["date"].apply(calculate_ugandan_fy)
    expenses["fiscal_year"] = expenses["date"].apply(calculate_ugandan_fy)
    payroll["fiscal_year"] = payroll["created_at"].apply(calculate_ugandan_fy)

    all_years = sorted(list(set(loans["fiscal_year"].dropna().unique()) | set(payments["fiscal_year"].dropna().unique())))
    if "Unknown" in all_years: 
        all_years.remove("Unknown")
        
    if not all_years:
        st.info("Insufficient chronological date metrics available to construct accounting periods.")
        return

    selected = st.selectbox("📂 Audit Financial Year Window", all_years, index=len(all_years)-1)

    # Isolated Fiscal Datasets
    fy_loans = loans[loans["fiscal_year"] == selected]
    fy_pay = payments[payments["fiscal_year"] == selected]
    fy_exp = expenses[expenses["fiscal_year"] == selected]
    fy_payr = payroll[payroll["fiscal_year"] == selected]

    fy_latest = fy_loans.sort_values(["sn", "cycle_no"]).drop_duplicates(subset=["sn"], keep="last")

    s1, s2 = st.columns(2)
    
    with s1:
        st.subheader(f"Income Statement — {selected}")
        
        fy_active = fy_latest[fy_latest["status"].isin(["ACTIVE", "PENDING", "OVERDUE"])]["principal"].sum()
        fy_interest = compute_realized_interest(fy_loans, fy_pay)
        
        # FIXED: Comprehensive OPEX calculations matching global profile variables
        fy_direct_exp = sum_col(fy_exp, "amount")
        fy_nssf = sum_col(fy_payr, "nssf_5") + sum_col(fy_payr, "nssf_10")
        fy_paye = sum_col(fy_payr, "paye")
        fy_salary = sum_col(fy_payr, "net_pay")
        fy_total_opex = fy_direct_exp + fy_nssf + fy_paye + fy_salary
        
        st.dataframe(pd.DataFrame({
            "Itemized Line": ["Allocated Active Capital", "Realized Interest Income", "Total Dynamic OPEX", "Net Accounting Profit"],
            "Amount (UGX)": [
                f"{fy_active:,.0f}",
                f"{fy_interest:,.0f}",
                f"{fy_total_opex:,.0f}",
                f"{(fy_interest - fy_total_opex):,.0f}"
            ]
        }), use_container_width=True, hide_index=True)

    with s2:
        st.subheader(f"Balance Sheet — {selected}")
        
        # Statement of position metrics
        loan_book = fy_latest["balance"].sum()
        cash = sum_col(fy_pay, "amount") - sum_col(fy_exp, "amount") - fy_nssf - fy_paye - fy_salary
        total_assets = loan_book + cash
        
        st.dataframe(pd.DataFrame({
            "Asset Classification": ["Outstanding Loan Book (Receivables)", "Net Cash Equivalents", "Total Book Assets"],
            "Amount (UGX)": [
                f"{loan_book:,.0f}",
                f"{cash:,.0f}",
                f"{total_assets:,.0f}"
            ]
        }), use_container_width=True, hide_index=True)
