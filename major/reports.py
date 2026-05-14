# ==========================================
# 🚀 BALLISTIC FINTECH REPORTS ENGINE (FAST + PRODUCTION SAFE)
# ==========================================
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas

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
    borrowers = safe_tenant_filter("borrowers")

    if loans.empty:
        st.info("No loan data available.")
        return

    # ==============================
    # ⚡ FAST NUMERIC ENGINE (VECTORISED)
    # ==============================
    def num(df, cols):
        for c in cols:
            if c in df.columns:
                return pd.to_numeric(df[c], errors="coerce").fillna(0)
        return pd.Series([0]*len(df), index=df.index)

    def sum_col(df, col):
        return float(pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0).sum())

    # ==============================
    # CLEAN LOANS
    # ==============================
    loans = loans.copy()
    for c in ["principal", "interest", "cycle_no"]:
        if c in loans.columns:
            loans[c] = pd.to_numeric(loans[c], errors="coerce").fillna(0)
    loans["status"] = loans["status"].astype(str).str.upper()

    # ==============================
    # ACTIVE CAPITAL
    # ==============================
    active_capital = loans[
        (loans["cycle_no"] == 1) &
        (loans["status"].isin(["ACTIVE", "PENDING"]))
    ]["principal"].sum()

    # ==============================
    # ⚡ FAST INTEREST ENGINE (PROPORTIONAL TO PAYMENTS)
    # ==============================
    payments_clean = payments.copy()
    if not payments_clean.empty and "amount" in payments_clean.columns:
        payments_clean["amount"] = pd.to_numeric(payments_clean["amount"], errors="coerce").fillna(0)
    else:
        payments_clean["amount"] = 0

    loan_pay = payments_clean.groupby("loan_id")["amount"].sum().to_dict()
    loans["loan_total"] = loans["principal"] + loans["interest"]

    def calc_interest(row):
        paid = loan_pay.get(row.get("id"), 0)
        if row["loan_total"] <= 0:
            return 0
        ratio = min(paid / row["loan_total"], 1.0)
        return row["interest"] * ratio

    projected_interest = loans.apply(calc_interest, axis=1).sum()

    # ==============================
    # COLLECTIONS
    # ==============================
    actual_collected = sum_col(payments, "amount")

    # ==============================
    # OPEX (NO PETTY CASH)
    # ==============================
    direct_expenses = sum_col(expenses, "amount")
    nssf = sum_col(payroll, "nssf_5") + sum_col(payroll, "nssf_10")
    paye = sum_col(payroll, "paye")
    salary = sum_col(payroll, "net_pay")
    total_opex = direct_expenses + nssf + paye + salary
    cash_profit = actual_collected - total_opex

    # ==============================
    # KPI CARDS
    # ==============================
    def kpi(title, value, color):
        st.markdown(f"""
        <div style="padding:16px;border-radius:12px;background:#fff;border:1px solid #eee">
            <div style="font-size:11px;color:#666">{title}</div>
            <div style="font-size:20px;color:{color};font-weight:700">
                UGX {value:,.0f}
            </div>
        </div>
        """, unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1: kpi("ACTIVE CAPITAL", active_capital, "#1E3A8A")
    with c2: kpi("INTEREST (REALISED)", projected_interest, "#059669")
    with c3: kpi("COLLECTIONS", actual_collected, "#7C3AED")
    with c4: kpi("NET CASHFLOW", cash_profit, "#059669" if cash_profit >= 0 else "#DC2626")

    # ==============================
    # TREND ENGINE
    # ==============================
    payments["date"] = pd.to_datetime(payments.get("date"), errors="coerce")
    expenses["date"] = pd.to_datetime(expenses.get("date"), errors="coerce")

    inc = pd.Series(dtype=float)
    exp = pd.Series(dtype=float)

    if not payments.empty:
        inc = payments.dropna(subset=["date"]).set_index("date").resample("ME")["amount"].sum()

    if not expenses.empty:
        exp = expenses.dropna(subset=["date"]).set_index("date").resample("ME")["amount"].sum()

    df_trend = pd.concat([inc, exp], axis=1).fillna(0)
    df_trend.columns = ["Income", "Expenses"]
    df_trend["Net"] = df_trend["Income"] - df_trend["Expenses"]

    if not df_trend.empty:
        fig = px.area(df_trend, x=df_trend.index, y=["Income","Expenses","Net"])
        st.plotly_chart(fig, use_container_width=True)

    # ==============================
    # RISK METRICS
    # ==============================
    overdue = loans[loans["status"].str.contains("OVERDUE", na=False)]
    par = sum_col(overdue, "balance")
    par_ratio = (par / active_capital * 100) if active_capital else 0
    yield_pct = (projected_interest / active_capital * 100) if active_capital else 0
    eff = (actual_collected / (active_capital + projected_interest) * 100) if (active_capital + projected_interest) else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Yield", f"{yield_pct:.1f}%")
    m2.metric("Collection Eff.", f"{eff:.1f}%")
    m3.metric("PAR", f"{par_ratio:.1f}%")
    m4.metric("Opex Ratio", f"{(total_opex/actual_collected*100 if actual_collected else 0):.1f}%")

    # ==============================
    # FINANCIAL STATEMENTS
    # ==============================
    s1, s2 = st.columns(2)

    def fy(dt):
        if pd.isna(dt):
            return "Unknown"
        return f"{dt.year}-{dt.year+1}" if dt.month >= 7 else f"{dt.year-1}-{dt.year}"

    loans["start_date"] = pd.to_datetime(loans.get("start_date"), errors="coerce")
    payments["date"] = pd.to_datetime(payments.get("date"), errors="coerce")
    expenses["date"] = pd.to_datetime(expenses.get("date"), errors="coerce")

    loans["fiscal_year"] = loans["start_date"].apply(fy)
    payments["fiscal_year"] = payments["date"].apply(fy)
    expenses["fiscal_year"] = expenses["date"].apply(fy)

    years = sorted(loans["fiscal_year"].dropna().unique())
    selected = st.selectbox("Select Financial Year", years)

    fy_loans = loans[loans["fiscal_year"] == selected]
    fy_exp = expenses[expenses["fiscal_year"] == selected]
    fy_pay = payments[payments["fiscal_year"] == selected]

    with s1:
        st.subheader(f"Income Statement — {selected}")
    
        fy_active = fy_loans[
            (fy_loans["cycle_no"] == 1) &
            (fy_loans["status"].isin(["ACTIVE","PENDING"]))
        ]["principal"].sum()
    
        fy_interest = compute_realized_interest(fy_loans, fy_pay) if not fy_loans.empty else 0
        fy_opex = sum_col(fy_exp, "amount")
    
        st.dataframe(pd.DataFrame({
            "Item": ["Active Capital","Interest","OPEX","Net Profit"],
            "UGX": [
                f"{fy_active:,.0f}",
                f"{fy_interest:,.0f}",
                f"{fy_opex:,.0f}",
                f"{fy_interest - fy_opex:,.0f}"
            ]
        }), use_container_width=True)

    with s2:
        st.subheader(f"Balance Sheet — {selected}")
    
        loan_book = fy_loans["balance"].sum()
        cash = sum_col(fy_pay, "amount") - sum_col(fy_exp, "amount")
        total_assets = fy_active + loan_book + cash
    
        st.dataframe(pd.DataFrame({
            "Item": ["Active Capital","Loan Book","Cash","Total Assets"],
            "UGX": [
                f"{fy_active:,.0f}",
                f"{loan_book:,.0f}",
                f"{cash:,.0f}",
                f"{total_assets:,.0f}"
            ]
        }), use_container_width=True)

# -----------------------------
# HELPER: REALIZED INTEREST CALC
# -----------------------------
def compute_realized_interest(loans_df, payments_df):
    if loans_df.empty:
        return 0
    loans_df = loans_df.copy()
    payments_df = payments_df.copy()

    for col in ["principal", "interest"]:
        if col not in loans_df.columns:
            loans_df[col] = 0
    if payments_df.empty or "amount" not in payments_df.columns:
        payments_df["amount"] = 0

    loan_pay = payments_df.groupby("loan_id")["amount"].sum().to_dict()
    loans_df["loan_total"] = loans_df["principal"] + loans_df["interest"]

    def calc(row):
        paid = loan_pay.get(row.get("id"), 0)
        if row["loan_total"] <= 0:
            return 0
        ratio = min(paid / row["loan_total"], 1.0)
        return row["interest"] * ratio

    return loans_df.apply(calc, axis=1).sum()
