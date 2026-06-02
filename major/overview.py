import streamlit as st
import pandas as pd
import plotly.express as px

# Core DB utilities
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas

# ==========================================
# 1. CORE PAGE FUNCTIONS (BRANDING & WIDE LAYOUT)
# ==========================================

st.set_page_config(layout="wide")

if "auto_refresh_tick" not in st.session_state:
    st.session_state.auto_refresh_tick = 0


def soft_refresh():
    st.session_state.auto_refresh_tick += 1


def get_Active_color():
    return st.session_state.get("theme_color", "#1E3A8A")


@st.cache_data(ttl=60, show_spinner=False)
def load_cached(name, tenant_id):
    try:
        raw_data = get_cached_data(name, tenant_id)
        if raw_data is None:
            return pd.DataFrame()
        return raw_data if isinstance(raw_data, pd.DataFrame) else pd.DataFrame(raw_data)
    except Exception as e:
        st.sidebar.error(f"⚠️ Cache Load Fail ({name}): {str(e)}")
        return pd.DataFrame()


# =========================================================
# HELPERS
# =========================================================

def normalize(df):
    try:
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df.columns = (
            df.columns
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )
        return df
    except:
        return pd.DataFrame()


def safe_numeric(df, cols):
    try:
        if df is None or df.empty:
            return pd.Series(0.0, index=df.index if df is not None else [])

        for c in cols:
            if c in df.columns:
                return pd.to_numeric(df[c], errors="coerce").fillna(0.0)

        return pd.Series(0.0, index=df.index)
    except:
        return pd.Series(0.0)


def safe_date(df, cols):
    try:
        if df is None or df.empty:
            return pd.Series(pd.NaT, index=df.index if df is not None else [])

        for c in cols:
            if c in df.columns:
                return pd.to_datetime(df[c], errors="coerce")

        return pd.Series(pd.NaT, index=df.index)
    except:
        return pd.Series(pd.NaT)


def first_existing(df, cols):
    try:
        for c in cols:
            if c in df.columns:
                return c
        return None
    except:
        return None


# ------------------------------
# MAIN DASHBOARD
# ------------------------------

def show_overview():
    st.markdown("""
    <style>
    .metric-box {
        padding: 16px;
        border-radius: 16px;
        color: #F9FAFB !important;
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 4px 14px rgba(0,0,0,0.06);
        margin-bottom: 10px;
        transition: all 0.22s ease;
        overflow: hidden;
    }
    .metric-box * { color: #F9FAFB !important; }
    .blue-card { background: linear-gradient(135deg, #5B6B8C, #3E4C68) !important; }
    .green-card { background: linear-gradient(135deg, #5E7C6B, #3E5A4C) !important; }
    .red-card { background: linear-gradient(135deg, #8B5E5E, #5E3E3E) !important; }
    .orange-card { background: linear-gradient(135deg, #9A7B5F, #6B523E) !important; }
    div[data-testid="stMarkdownContainer"] { background: transparent !important; }
    </style>
    """, unsafe_allow_html=True)
    
    brand_color = get_Active_color()

    # 1. Verify Active Tenant Session Token
    tenant_id = st.session_state.get("tenant_id")
    if not tenant_id:
        st.error("🔐 Access Denied: No active session tenant token located. Please sign in.")
        return

    try:
        # --- UI HEADER ---
        st.markdown(
            f"""
            <div style='background:{brand_color}; padding:25px; border-radius:15px; margin-bottom:25px; color:white;'>
                <h1 style='margin:0; font-size:28px;'>🏛️ Financial Control Center</h1>
                <p style='margin:0; opacity:0.8;'>Real-time insights across loans & expenses</p>
            </div>
            """,
            unsafe_allow_html=True
        )

        # --- DATA INGESTION ---
        loans_df = normalize(load_cached("loans", tenant_id))
        payments_df = normalize(load_cached("payments", tenant_id))
        expenses_df = normalize(load_cached("expenses", tenant_id))

        # =========================================================
        # CRITICAL FIX: EXPLICIT TENANT ISOLATION FILTERING
        # =========================================================
        # This guarantees that calculations only run on data belonging to the selected client
        str_tenant = str(tenant_id).strip().lower()

        if not loans_df.empty and "tenant_id" in loans_df.columns:
            loans_df["tenant_id"] = loans_df["tenant_id"].astype(str).str.strip().str.lower()
            loans_df = loans_df[loans_df["tenant_id"] == str_tenant]

        if not payments_df.empty and "tenant_id" in payments_df.columns:
            payments_df["tenant_id"] = payments_df["tenant_id"].astype(str).str.strip().str.lower()
            payments_df = payments_df[payments_df["tenant_id"] == str_tenant]

        if not expenses_df.empty and "tenant_id" in expenses_df.columns:
            expenses_df["tenant_id"] = expenses_df["tenant_id"].astype(str).str.strip().str.lower()
            expenses_df = expenses_df[expenses_df["tenant_id"] == str_tenant]
        # =========================================================

        # --- SMART STATUS LOGIC ---
        if not loans_df.empty and "sn" in loans_df.columns:
            loans_df["sn"] = loans_df["sn"].astype(str).str.strip().str.upper()
            bal_col = first_existing(loans_df, ["balance", "total_repayable"])
            
            if bal_col:
                loans_df["tmp_bal"] = pd.to_numeric(loans_df[bal_col], errors="coerce").fillna(0.0)
                loans_df = loans_df.sort_values(by=["sn", "cycle_no", "start_date"] if "cycle_no" in loans_df.columns and "start_date" in loans_df.columns else ["sn"])

                for _, grp in loans_df.groupby("sn"):
                    indices = grp.index.tolist()
                    if len(indices) > 1:
                        loans_df.loc[indices[:-1], "status"] = "BCF"

                    latest_idx = indices[-1]
                    if abs(loans_df.at[latest_idx, "tmp_bal"]) < 1.0:
                        loans_df.at[latest_idx, "status"] = "CLEARED"

                loans_df.loc[((loans_df["tmp_bal"] <= 0) & (loans_df["status"] != "BCF")), "status"] = "CLEARED"
                loans_df.drop(columns=["tmp_bal"], inplace=True)

        # --- DATA ALIGNMENT & METRIC PREP ---
        if not loans_df.empty:
            loans_df["principal_n"] = safe_numeric(loans_df, ["principal", "amount"])
            loans_df["interest_n"] = safe_numeric(loans_df, ["interest", "interest_amount"])
            
            total_repayable = safe_numeric(loans_df, ["total_repayable", "balance"])
            amount_paid = safe_numeric(loans_df, ["amount_paid", "paid"])
            loans_df["balance_n"] = (total_repayable - amount_paid).clip(lower=0)
            
            loans_df["cycle_no"] = pd.to_numeric(loans_df.get("cycle_no", 1), errors="coerce").fillna(1)
            original_loans = loans_df[loans_df["cycle_no"] == 1]
            total_principal = float(original_loans["principal_n"].sum())
        else:
            total_principal = 0.0

        if not expenses_df.empty:
            expenses_df["amount"] = safe_numeric(expenses_df, ["amount"])
            total_expenses = float(expenses_df["amount"].sum())
        else:
            total_expenses = 0.0

        overdue_count = 0
        if not loans_df.empty:
            today = pd.Timestamp.now().normalize()
            loans_df["due_date_dt"] = safe_date(loans_df, ["end_date", "due_date"])
            
            if "status" not in loans_df.columns:
                loans_df["status"] = "ACTIVE"
            else:
                loans_df["status"] = loans_df["status"].astype(str).str.upper()

            overdue_mask = (
                loans_df["due_date_dt"].notna()
                & (loans_df["due_date_dt"] < today)
                & (~loans_df["status"].isin(["CLEARED", "BCF", "CLOSED", "PAID"]))
            )
            overdue_count = int(overdue_mask.sum())

        def compute_interest_earned(l_df, p_df):
            if l_df.empty or p_df.empty or "id" not in l_df.columns:
                return 0.0
            
            total_interest_earned = 0.0
            p_amt_col = first_existing(p_df, ["amount", "paid", "payment"])
            p_loan_col = first_existing(p_df, ["loan_id", "loan"])
            
            if not p_amt_col or not p_loan_col:
                return 0.0

            p_df_copy = p_df.copy()
            p_df_copy["clean_amt"] = pd.to_numeric(p_df_copy[p_amt_col], errors="coerce").fillna(0.0)

            for _, loan in l_df.iterrows():
                loan_id = loan["id"]
                principal = float(loan.get("principal_n", 0.0))
                interest = float(loan.get("interest_n", 0.0))
                target_denominator = principal + interest

                if target_denominator <= 0:
                    continue

                loan_payments = p_df_copy[p_df_copy[p_loan_col] == loan_id]
                paid = loan_payments["clean_amt"].sum()
                ratio = min(paid / target_denominator, 1.0)
                total_interest_earned += interest * ratio

            return total_interest_earned

        total_interest = compute_interest_earned(loans_df, payments_df)

        if overdue_count >= 5:
            st.warning(f"⚠️ {overdue_count} overdue loans need urgent administrative attention.")

        # --- RENDERING KPI CARDS ---
        def render_metric_card(container, title, value, subtitle, css_class):
            with container:
                st.markdown(
                    f"""
                    <div class="metric-box {css_class}">
                        <div style="font-size: 13px; opacity: 0.85; font-weight: 500;">{title}</div>
                        <div style="font-size: 24px; font-weight: 700; margin: 4px 0;">{value}</div>
                        <div style="font-size: 11px; opacity: 0.7;">{subtitle}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        m1, m2, m3, m4 = st.columns(4)
        render_metric_card(m1, "Active Principal", f"UGX {total_principal:,.0f}", "Portfolio Asset Value", "blue-card")
        render_metric_card(m2, "Interest Income (REALIZED)", f"UGX {total_interest:,.0f}", "Based on cash collections", "green-card")
        render_metric_card(m3, "Operational Costs", f"UGX {total_expenses:,.0f}", "Total Outflows", "red-card")
        render_metric_card(m4, "Critical Alerts", str(overdue_count), "Overdue Loan Contracts", "orange-card")

        st.write("")

        # --- DATA VISUALIZATION SECTION ---
        col_l, col_r = st.columns([2, 1])

        with col_l:
            st.markdown("#### 📈 Revenue Trend vs Expenses")
            try:
                pay_date_col = first_existing(payments_df, ["date", "payment_date", "created_at"])
                pay_amt_col = first_existing(payments_df, ["amount", "paid", "payment"])

                if not payments_df.empty and pay_date_col and pay_amt_col:
                    rev_df = payments_df.copy()
                    rev_df["date_dt"] = pd.to_datetime(rev_df[pay_date_col], errors="coerce")
                    rev_df["amount_n"] = pd.to_numeric(rev_df[pay_amt_col], errors="coerce").fillna(0.0)
                    rev_df = rev_df.dropna(subset=["date_dt"])
                    rev_df["month"] = rev_df["date_dt"].dt.to_period("M").dt.to_timestamp()

                    monthly_rev = rev_df.groupby("month", as_index=False)["amount_n"].sum()
                    monthly_rev.columns = ["month", "Revenue"]
                else:
                    monthly_rev = pd.DataFrame(columns=["month", "Revenue"])

                exp_date_col = first_existing(expenses_df, ["payment_date", "date", "created_at"])
                if not expenses_df.empty and exp_date_col:
                    exp_df = expenses_df.copy()
                    exp_df["date_dt"] = pd.to_datetime(exp_df[exp_date_col], errors="coerce")
                    exp_df["amount_n"] = pd.to_numeric(exp_df["amount"], errors="coerce").fillna(0.0)
                    exp_df = exp_df.dropna(subset=["date_dt"])
                    exp_df["month"] = exp_df["date_dt"].dt.to_period("M").dt.to_timestamp()

                    monthly_exp = exp_df.groupby("month", as_index=False)["amount_n"].sum()
                    monthly_exp.columns = ["month", "Expenses"]
                else:
                    monthly_exp = pd.DataFrame(columns=["month", "Expenses"])

                if not monthly_rev.empty or not monthly_exp.empty:
                    trend_df = pd.merge(monthly_rev, monthly_exp, on="month", how="outer").fillna(0.0)
                    trend_df = trend_df.sort_values("month")

                    fig = px.line(
                        trend_df,
                        x="month",
                        y=["Revenue", "Expenses"],
                        template="plotly_white",
                        color_discrete_map={"Revenue": "#10B981", "Expenses": "#EF4444"}
                    )
                    fig.update_traces(mode="lines+markers")
                    fig.update_layout(
                        height=320,
                        margin=dict(l=0, r=0, t=20, b=0),
                        legend_title="",
                        xaxis_title="",
                        yaxis_title="Amount (UGX)",
                        hovermode="x unified"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Insufficient timeline records available to build lines.")
            except Exception as e:
                st.warning(f"Trend analytics interface skipped: {e}")

        with col_r:
            st.markdown("#### 🎯 Portfolio Health")
            try:
                if not loans_df.empty and "status" in loans_df.columns:
                    clean_status = loans_df["status"].astype(str).str.strip().str.upper()
                    clean_status = clean_status.replace({
                        "CURRENT": "ACTIVE", "ONGOING": "ACTIVE",
                        "COMPLETE": "PAID", "CLOSED": "PAID",
                        "LATE PAYMENT": "LATE", "DEFAULTED": "DEFAULT"
                    })

                    status_data = clean_status.value_counts().reset_index()
                    status_data.columns = ["status", "count"]
                    total_loans = status_data["count"].sum()

                    color_map = {"ACTIVE": "#10B981", "PAID": "#3B82F6", "LATE": "#F59E0B", "DEFAULT": "#EF4444"}

                    fig_pie = px.pie(
                        status_data, names="status", values="count",
                        hole=0.65, color="status", color_discrete_map=color_map
                    )
                    fig_pie.update_traces(
                        textposition="inside", textinfo="percent",
                        hovertemplate="<b>%{label}</b><br>Loans: %{value}<extra></extra>"
                    )
                    fig_pie.update_layout(
                        height=320, margin=dict(l=10, r=10, t=20, b=10),
                        legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
                        annotations=[dict(text=f"{total_loans}<br>Total", x=0.5, y=0.5, font_size=16, showarrow=False)]
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                else:
                    st.info("No status variables loaded.")
            except Exception as e:
                st.info(f"Breakdown slice hidden: {e}")

        # --- ACTIVITY TIMELINES ---
        st.write("---")
        t1, t2 = st.columns(2)

        with t1:
            st.markdown("#### 📊 Monthly Lending Volume")
            try:
                if not loans_df.empty:
                    graph_df = loans_df.copy()
                    graph_df["date_dt"] = safe_date(graph_df, ["start_date", "created_at"])
                    graph_df = graph_df.dropna(subset=["date_dt"])
                    graph_df["month"] = graph_df["date_dt"].dt.to_period("M").dt.to_timestamp()

                    timeline_df = graph_df.groupby("month")[["principal_n", "interest_n"]].sum().reset_index()
                    timeline_df = timeline_df.sort_values("month")
                    timeline_df.rename(columns={"principal_n": "Loans Issued", "interest_n": "Interest Expected"}, inplace=True)

                    fig_portfolio = px.line(
                        timeline_df, x="month", y=["Loans Issued", "Interest Expected"],
                        template="plotly_white", markers=True,
                        color_discrete_map={"Loans Issued": brand_color, "Interest Expected": "#10B981"}
                    )
                    fig_portfolio.update_layout(
                        height=350, hovermode="x unified",
                        xaxis_title="", yaxis_title="Amount (UGX)", legend_title=""
                    )
                    st.plotly_chart(fig_portfolio, use_container_width=True)
                else:
                    st.info("Lending matrix records look empty.")
            except Exception as e:
                st.info(f"Growth visualizer bypass: {e}")

        with t2:
            st.markdown("### 💸 Latest Expenses")
            try:
                if not expenses_df.empty:
                    df = expenses_df.copy()
                    df["amount"] = safe_numeric(df, ["amount"])
                    exp_date_col = first_existing(df, ["date", "payment_date", "created_at"])
                    df["date"] = pd.to_datetime(df[exp_date_col], errors="coerce") if exp_date_col else pd.NaT

                    df = df.sort_values("date", ascending=False).dropna(subset=["date"])
                    latest = df.head(5)

                    total = latest["amount"].sum()
                    avg = latest["amount"].mean()
                    count = len(latest)

                    k1, k2, k3 = st.columns(3)
                    k1.markdown(f"""<div style="background:#FEE2E2; padding:12px; border-radius:10px;"><div style="font-size:11px; color:#991B1B;">Total (Top 5)</div><div style="font-size:18px; font-weight:700; color:#B91C1C;">UGX {total:,.0f}</div></div>""", unsafe_allow_html=True)
                    k2.markdown(f"""<div style="background:#E0F2FE; padding:12px; border-radius:10px;"><div style="font-size:11px; color:#075985;">Average</div><div style="font-size:18px; font-weight:700; color:#0369A1;">UGX {avg:,.0f}</div></div>""", unsafe_allow_html=True)
                    k3.markdown(f"""<div style="background:#ECFDF5; padding:12px; border-radius:10px;"><div style="font-size:11px; color:#065F46;">Entries</div><div style="font-size:18px; font-weight:700; color:#047857;">{count}</div></div>""", unsafe_allow_html=True)

                    st.write("")
                    
                    display_df = latest.copy()
                    display_df["category"] = display_df.get("category", "General").fillna("General")
                    display_df["date_str"] = display_df["date"].dt.strftime("%Y-%m-%d")

                    final_df = display_df[["category", "amount", "date_str"]].rename(columns={"date_str": "date"})
                    styled_df = final_df.style.format({"amount": "UGX {:,.0f}"}).map(lambda x: "color: #EF4444; font-weight: 600;", subset=["amount"])

                    st.dataframe(styled_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No corporate outflows mapped.")
            except Exception as e:
                st.error(f"Loss ledger layout anomaly: {e}")

        # --- EXPORT MANAGEMENT UTILITIES ---
        st.write("---")
        c1, c2 = st.columns(2)

        with c1:
            csv_data = loans_df.to_csv(index=False).encode("utf-8") if not loans_df.empty else b""
            st.download_button(
                label="📥 Download Underlying Portfolio (CSV)",
                data=csv_data,
                file_name=f"portfolio_data_{pd.Timestamp.now().strftime('%Y-%m-%d')}.csv",
                mime="text/csv",
                disabled=loans_df.empty,
                use_container_width=True
            )

        with c2:
            csv2 = expenses_df.to_csv(index=False).encode("utf-8") if not expenses_df.empty else b""
            st.download_button(
                label="⬇️ Export Operating Expenses CSV",
                data=csv2,
                file_name="expenses_report.csv",
                mime="text/csv",
                disabled=expenses_df.empty,
                use_container_width=True
            )

    except Exception as e:
        st.error(f"Critical execution error inside systemic dashboard wrapper: {str(e)}")
