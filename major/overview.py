import streamlit as st
import pandas as pd
import plotly.express as px
import streamlit as st
import pandas as pd

# Core DB utilities
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas
# ==========================================
# 1. CORE PAGE FUNCTIONS (BRANDING & WIDE LAYOUT)
# ==========================================

st.set_page_config(layout="wide")

# ------------------------------
# AUTO REFRESH EVERY 60 SECONDS
# ------------------------------
if "auto_refresh_tick" not in st.session_state:
    st.session_state.auto_refresh_tick = 0


def soft_refresh():
    st.session_state.auto_refresh_tick += 1


# ------------------------------
# THEME COLOR
# ------------------------------
def get_Active_color():
    return st.session_state.get("theme_color", "#1E3A8A")


# ------------------------------
# SAFE CACHE LAYER
# ------------------------------
@st.cache_data(ttl=60, show_spinner=False)
def load_cached(name):
    try:
        return get_cached_data(name)
    except:
        return pd.DataFrame()


# =========================================================
# HELPERS
# =========================================================

def normalize(df):
    try:
        if df is None:
            return pd.DataFrame()

        if not isinstance(df, pd.DataFrame):
            return pd.DataFrame()

        if df.empty:
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
            return pd.Series(dtype="float64")

        for c in cols:
            if c in df.columns:
                return pd.to_numeric(
                    df[c],
                    errors="coerce"
                ).fillna(0)

        return pd.Series(0.0, index=df.index)

    except:
        return pd.Series(0.0)


def safe_date(df, cols):
    try:
        if df is None or df.empty:
            return pd.Series(dtype="datetime64[ns]")

        for c in cols:
            if c in df.columns:
                return pd.to_datetime(
                    df[c],
                    errors="coerce"
                )

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

    brand_color = get_Active_color()

    try:

        # --- SLEEK METRIC CARD STYLE ---
        st.markdown(
            f"""
            <style>
            .metric-card {{
                background: rgba(255,255,255,0.92);
                padding: 10px 14px;
                border-radius: 12px;
                border: 1px solid rgba(0,0,0,0.05);
                box-shadow: 0 2px 8px rgba(0,0,0,0.04);
                backdrop-filter: blur(6px);
                transition: all 0.2s ease;
                max-width: 170px;
                margin: auto;
            }}

            .metric-card:hover {{
                transform: translateY(-2px);
                box-shadow: 0 6px 14px rgba(0,0,0,0.08);
                border-color: rgba(0,0,0,0.08);
            }}

            .metric-card h3 {{
                font-size: 0.9rem;
                margin-bottom: 4px;
                color: #666;
                font-weight: 500;
            }}

            .metric-card h1 {{
                font-size: 1.4rem;
                margin: 0;
                font-weight: 700;
                color: #111;
            }}

            @media (max-width:768px) {{
                .metric-card {{
                    padding: 8px 12px;
                    max-width: 145px;
                }}

                .metric-card h1 {{
                    font-size: 1.2rem;
                }}
            }}
            </style>
            """,
            unsafe_allow_html=True
        )

        # --- UI HEADER ---
        st.markdown(
            f"""
            <div style='background:{brand_color}; padding:25px; border-radius:15px; margin-bottom:25px; color:white;'>
                <h1 style='margin:0; font-size:28px;'>🏛️ Financial Control Center</h1>
                <p style='margin:0; opacity:0.8;'>Real-time insights across loans & Expenses</p>
            </div>
            """,
            unsafe_allow_html=True
        )

        # --- 1. DATA INGESTION ---
        loans_df = normalize(load_cached("loans"))
        payments_df = normalize(load_cached("payments"))
        expenses_df = normalize(load_cached("expenses"))
        borrowers_df = normalize(load_cached("borrowers"))

        if loans_df.empty:
            st.info(
                "👋 Welcome! No active data found. Add your first borrower or loan to populate this dashboard."
            )
            return

        # =========================================================
        # 🛡️ SMART STATUS LOGIC (CRITICAL FIX FOR DASHBOARD SYNC)
        # =========================================================

        if "sn" in loans_df.columns:

            loans_df["sn"] = (
                loans_df["sn"]
                .astype(str)
                .str.strip()
                .str.upper()
            )

            bal_col = first_existing(
                loans_df,
                ["balance", "total_repayable"]
            )

            loans_df["tmp_bal"] = pd.to_numeric(
                loans_df[bal_col],
                errors="coerce"
            ).fillna(0)

            loans_df = loans_df.sort_values(
                by=["sn", "cycle_no", "start_date"]
            )

            for _, grp in loans_df.groupby("sn"):

                indices = grp.index.tolist()

                # Mark all previous cycles as BCF
                if len(indices) > 1:
                    loans_df.loc[indices[:-1], "status"] = "BCF"

                # Check latest cycle
                latest_idx = indices[-1]

                if abs(loans_df.at[latest_idx, "tmp_bal"]) < 1.0:
                    loans_df.at[latest_idx, "status"] = "CLEARED"

            # Global Override
            loans_df.loc[
                (
                    (loans_df["tmp_bal"] <= 0)
                    & (loans_df["status"] != "BCF")
                ),
                "status"
            ] = "CLEARED"

            loans_df.drop(columns=["tmp_bal"], inplace=True)

        # ==============================
        # ENSURE REQUIRED COLUMNS EXIST
        # ==============================

        required_loan_cols = [
            "status",
            "principal",
            "amount",
            "interest",
            "interest_amount",
            "balance",
            "total_repayable",
            "amount_paid",
            "paid",
            "end_date",
            "due_date"
        ]

        for col in required_loan_cols:
            if col not in loans_df.columns:
                loans_df[col] = 0

        # ==============================
        # SAFE NUMERIC ENGINE
        # ==============================

        def get_numeric(df, cols):

            for c in cols:
                if c in df.columns:
                    return pd.to_numeric(
                        df[c],
                        errors="coerce"
                    ).fillna(0)

            return pd.Series([0] * len(df), index=df.index)

        # ==============================
        # SAFE DATE ENGINE
        # ==============================

        def get_dates(df, cols):

            for c in cols:
                if c in df.columns:
                    return pd.to_datetime(
                        df[c],
                        errors="coerce"
                    )

            return pd.Series(
                [pd.NaT] * len(df),
                index=df.index
            )

        # ==============================
        # ENGINE: UNIFIED CALCULATIONS
        # ==============================

        loans_df["principal_n"] = pd.to_numeric(
            loans_df.get("principal", 0),
            errors="coerce"
        ).fillna(0)

        loans_df["interest_n"] = get_numeric(
            loans_df,
            ["interest", "interest_amount"]
        )

        total_repayable = get_numeric(
            loans_df,
            ["balance", "total_repayable"]
        )

        amount_paid = get_numeric(
            loans_df,
            ["amount_paid", "paid"]
        )

        # Safer balance calc
        loans_df["balance_n"] = (
            total_repayable - amount_paid
        ).clip(lower=0)

        # ==============================
        # EXPENSES
        # ==============================

        if expenses_df is None or expenses_df.empty:

            total_expenses = 0

        else:

            if "amount" not in expenses_df.columns:
                expenses_df["amount"] = 0

            expenses_df["amount"] = pd.to_numeric(
                expenses_df["amount"],
                errors="coerce"
            ).fillna(0)

            total_expenses = float(
                expenses_df["amount"].sum()
            )

        # ==============================
        # OVERDUE ENGINE
        # ==============================

        today = pd.Timestamp.now().normalize()

        loans_df["due_date_dt"] = get_dates(
            loans_df,
            ["end_date", "due_date"]
        )

        loans_df["status"] = (
            loans_df["status"]
            .astype(str)
            .str.upper()
        )

        overdue_mask = (
            loans_df["due_date_dt"].notna()
            & (loans_df["due_date_dt"] < today)
            & (
                ~loans_df["status"].isin(
                    ["CLEARED", "BCF", "CLOSED"]
                )
            )
        )

        overdue_count = int(overdue_mask.sum())

        # ==============================
        # TOTALS
        # ==============================

        if "cycle_no" not in loans_df.columns:
            loans_df["cycle_no"] = 1

        loans_df["cycle_no"] = pd.to_numeric(
            loans_df["cycle_no"],
            errors="coerce"
        ).fillna(1)

        original_loans = loans_df[
            loans_df["cycle_no"] == 1
        ].copy()

        total_principal = float(
            original_loans["principal_n"].sum()
        )

        total_interest = float(
            loans_df["interest_n"].sum()
        )

        # ==============================
        # SMART ALERTS
        # ==============================

        if overdue_count >= 5:
            st.warning(
                f"⚠️ {overdue_count} overdue loans need urgent attention."
            )

        # ==============================
        # SLEEK + MUTED METRIC CARD STYLES
        # ==============================

        st.markdown(
            """
            <style>
            .metric-box {
                padding: 16px;
                border-radius: 16px;
                color: #F9FAFB;
                backdrop-filter: blur(10px);
                border: 1px solid rgba(255,255,255,0.08);
                box-shadow: 0 4px 14px rgba(0,0,0,0.06);
                margin-bottom: 10px;
                transition: all 0.22s ease;
                overflow: hidden;
                position: relative;
            }

            .metric-box:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 18px rgba(0,0,0,0.10);
            }

            .metric-title {
                font-size: 11px;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 1.1px;
                opacity: 0.78;
                margin-bottom: 6px;
            }

            .metric-value {
                font-size: 26px;
                font-weight: 700;
                line-height: 1.1;
                margin-bottom: 4px;
            }

            .metric-sub {
                font-size: 11px;
                font-weight: 500;
                opacity: 0.72;
            }

            .blue-card {
                background: linear-gradient(135deg, #5B6B8C, #3E4C68);
            }

            .green-card {
                background: linear-gradient(135deg, #5E7C6B, #3E5A4C);
            }

            .red-card {
                background: linear-gradient(135deg, #8B5E5E, #5E3E3E);
            }

            .orange-card {
                background: linear-gradient(135deg, #9A7B5F, #6B523E);
            }

            @media (max-width:768px) {

                .metric-box {
                    padding: 14px;
                    border-radius: 14px;
                }

                .metric-value {
                    font-size: 22px;
                }

                .metric-title,
                .metric-sub {
                    font-size: 10px;
                }
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        # ==============================
        # CARD HELPER
        # ==============================

        def render_metric_card(
            container,
            title,
            value,
            subtitle,
            css_class
        ):

            with container:

                st.markdown(
                    f"""
                    <div class="metric-box {css_class}">
                        <div class="metric-title">{title}</div>
                        <div class="metric-value">{value}</div>
                        <div class="metric-sub">{subtitle}</div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

        # ==============================
        # TOP METRICS
        # ==============================

        try:

            m1, m2, m3, m4 = st.columns(4)

            render_metric_card(
                m1,
                "Active Principal",
                f"UGX {total_principal:,.0f}",
                "Portfolio Value",
                "blue-card"
            )

            render_metric_card(
                m2,
                "Interest Income",
                f"UGX {total_interest:,.0f}",
                "Expected Earnings",
                "green-card"
            )

            render_metric_card(
                m3,
                "Operational Costs",
                f"UGX {total_expenses:,.0f}",
                "Total Expenses",
                "red-card"
            )

            render_metric_card(
                m4,
                "Critical Alerts",
                str(overdue_count),
                "Overdue Loans",
                "orange-card"
            )

        except NameError:
            pass

        st.write("")

        # --- 4. DATA VISUALIZATION SECTION ---
        col_l, col_r = st.columns([2, 1])

        with col_l:

            st.markdown("#### 📈 Revenue Trend vs Expenses")

            try:

                if not payments_df.empty:

                    pay_date_col = first_existing(
                        payments_df,
                        ["date", "payment_date", "created_at"]
                    )

                    pay_amt_col = first_existing(
                        payments_df,
                        ["amount", "paid", "payment"]
                    )

                    if pay_date_col and pay_amt_col:

                        rev_df = payments_df.copy()

                        rev_df["date_dt"] = pd.to_datetime(
                            rev_df[pay_date_col],
                            errors="coerce"
                        )

                        rev_df["amount_n"] = pd.to_numeric(
                            rev_df[pay_amt_col],
                            errors="coerce"
                        ).fillna(0)

                        rev_df = rev_df.dropna(
                            subset=["date_dt"]
                        )

                        rev_df["month"] = (
                            rev_df["date_dt"]
                            .dt.to_period("M")
                            .dt.to_timestamp()
                        )

                        monthly_rev = (
                            rev_df.groupby(
                                "month",
                                as_index=False
                            )["amount_n"]
                            .sum()
                        )

                        monthly_rev.rename(
                            columns={"amount_n": "Revenue"},
                            inplace=True
                        )

                        # EXPENSES
                        if not expenses_df.empty:

                            exp_df = expenses_df.copy()

                            exp_date_col = first_existing(
                                exp_df,
                                ["payment_date", "date", "created_at"]
                            )

                            exp_df["date_dt"] = pd.to_datetime(
                                exp_df[exp_date_col],
                                errors="coerce"
                            )

                            exp_df["amount_n"] = pd.to_numeric(
                                exp_df["amount"],
                                errors="coerce"
                            ).fillna(0)

                            exp_df = exp_df.dropna(
                                subset=["date_dt"]
                            )

                            exp_df["month"] = (
                                exp_df["date_dt"]
                                .dt.to_period("M")
                                .dt.to_timestamp()
                            )

                            monthly_exp = (
                                exp_df.groupby(
                                    "month",
                                    as_index=False
                                )["amount_n"]
                                .sum()
                            )

                            monthly_exp.rename(
                                columns={"amount_n": "Expenses"},
                                inplace=True
                            )

                        else:

                            monthly_exp = pd.DataFrame(
                                columns=["month", "Expenses"]
                            )

                        # MERGE
                        trend_df = pd.merge(
                            monthly_rev,
                            monthly_exp,
                            on="month",
                            how="outer"
                        ).fillna(0)

                        trend_df = trend_df.sort_values("month")

                        # PLOT
                        fig = px.line(
                            trend_df,
                            x="month",
                            y=["Revenue", "Expenses"],
                            template="plotly_white",
                            color_discrete_map={
                                "Revenue": "#10B981",
                                "Expenses": "#EF4444"
                            }
                        )

                        fig.update_traces(
                            mode="lines+markers"
                        )

                        fig.update_layout(
                            height=320,
                            margin=dict(
                                l=0,
                                r=0,
                                t=20,
                                b=0
                            ),
                            legend_title="",
                            xaxis_title="",
                            yaxis_title="amount (UGX)",
                            hovermode="x unified"
                        )

                        st.plotly_chart(
                            fig,
                            use_container_width=True
                        )

                    else:
                        st.info("Payment columns missing.")

                else:
                    st.info(
                        "Insufficient payment history for trend analysis."
                    )

            except Exception as e:
                st.warning(
                    f"Revenue chart temporarily unavailable: {e}"
                )

        with col_r:

            st.markdown("#### 🎯 Portfolio Health")

            try:

                if (
                    not loans_df.empty
                    and "status" in loans_df.columns
                ):

                    clean_status = (
                        loans_df["status"]
                        .astype(str)
                        .str.strip()
                        .str.upper()
                    )

                    clean_status = clean_status.replace({
                        "CURRENT": "ACTIVE",
                        "ONGOING": "ACTIVE",
                        "COMPLETE": "PAID",
                        "CLOSED": "PAID",
                        "LATE PAYMENT": "LATE",
                        "DEFAULTED": "DEFAULT"
                    })

                    status_data = (
                        clean_status
                        .value_counts()
                        .reset_index()
                    )

                    status_data.columns = [
                        "status",
                        "count"
                    ]

                    total_loans = status_data["count"].sum()

                    color_map = {
                        "ACTIVE": "#10B981",
                        "PAID": "#3B82F6",
                        "LATE": "#F59E0B",
                        "DEFAULT": "#EF4444"
                    }

                    fig_pie = px.pie(
                        status_data,
                        names="status",
                        values="count",
                        hole=0.65,
                        color="status",
                        color_discrete_map=color_map
                    )

                    fig_pie.update_traces(
                        textposition="inside",
                        textinfo="percent+label",
                        hovertemplate="<b>%{label}</b><br>Loans: %{value}<br>Share: %{percent}<extra></extra>"
                    )

                    fig_pie.update_layout(
                        height=320,
                        margin=dict(
                            l=10,
                            r=10,
                            t=20,
                            b=10
                        ),
                        annotations=[
                            dict(
                                text=f"{total_loans}<br>Total",
                                x=0.5,
                                y=0.5,
                                font_size=18,
                                showarrow=False
                            )
                        ]
                    )

                    st.plotly_chart(
                        fig_pie,
                        use_container_width=True
                    )

                else:
                    st.info("No portfolio data available.")

            except Exception as e:
                st.info(
                    f"Portfolio chart unavailable: {e}"
                )

        # --- 5. ACTIVITY FEEDS ---
        st.write("---")

        t1, t2 = st.columns(2)

        with t1:

            st.markdown("#### 📊 Monthly Lending vs Interest")

            try:

                graph_df = loans_df.copy()

                graph_df["date_dt"] = safe_date(
                    graph_df,
                    ["start_date", "created_at"]
                )

                graph_df = graph_df.dropna(
                    subset=["date_dt"]
                )

                if not graph_df.empty:

                    graph_df["month"] = (
                        graph_df["date_dt"]
                        .dt.to_period("M")
                        .dt.to_timestamp()
                    )

                    timeline_df = (
                        graph_df.groupby("month")[
                            ["principal_n", "interest_n"]
                        ]
                        .sum()
                        .reset_index()
                        .sort_values("month")
                    )

                    timeline_df.rename(
                        columns={
                            "principal_n": "Loans Issued",
                            "interest_n": "Interest Expected"
                        },
                        inplace=True
                    )

                    fig_portfolio = px.line(
                        timeline_df,
                        x="month",
                        y=[
                            "Loans Issued",
                            "Interest Expected"
                        ],
                        template="plotly_white",
                        markers=True,
                        color_discrete_map={
                            "Loans Issued": brand_color,
                            "Interest Expected": "#10B981"
                        }
                    )

                    fig_portfolio.update_layout(
                        height=350,
                        hovermode="x unified",
                        xaxis_title="",
                        yaxis_title="amount (UGX)",
                        legend_title=""
                    )

                    st.plotly_chart(
                        fig_portfolio,
                        use_container_width=True
                    )

                else:
                    st.info(
                        "Not enough dated records to generate a trend."
                    )

            except Exception as e:
                st.info(
                    f"Growth chart unavailable: {e}"
                )

        with t2:

            st.markdown("### 💸 Latest Expenses")

            try:

                if not expenses_df.empty:

                    df = expenses_df.copy()

                    df["amount"] = pd.to_numeric(
                        df["amount"],
                        errors="coerce"
                    ).fillna(0)

                    df["date"] = pd.to_datetime(
                        df["date"],
                        errors="coerce"
                    )

                    df = df.sort_values(
                        "date",
                        ascending=False
                    )

                    latest = df.head(5)

                    total = latest["amount"].sum()
                    avg = latest["amount"].mean()
                    count = len(latest)

                    k1, k2, k3 = st.columns(3)

                    k1.markdown(
                        f"""<div style="background:#FEE2E2; padding:16px; border-radius:12px;"><div style="font-size:12px; color:#991B1B;">Total (Top 5)</div><div style="font-size:22px; font-weight:700; color:#B91C1C;">UGX {total:,.0f}</div></div>""",
                        unsafe_allow_html=True
                    )

                    k2.markdown(
                        f"""<div style="background:#E0F2FE; padding:16px; border-radius:12px;"><div style="font-size:12px; color:#075985;">Average</div><div style="font-size:22px; font-weight:700; color:#0369A1;">UGX {avg:,.0f}</div></div>""",
                        unsafe_allow_html=True
                    )

                    k3.markdown(
                        f"""<div style="background:#ECFDF5; padding:16px; border-radius:12px;"><div style="font-size:12px; color:#065F46;">Entries</div><div style="font-size:22px; font-weight:700; color:#047857;">{count}</div></div>""",
                        unsafe_allow_html=True
                    )

                    st.divider()

                    display_df = latest.copy()

                    display_df["category"] = (
                        display_df["category"]
                        .fillna("General")
                    )

                    display_df["date"] = (
                        display_df["date"]
                        .dt.strftime("%Y-%m-%d")
                    )

                    final_df = display_df[
                        ["category", "amount", "date"]
                    ]

                    def style_amount(val):
                        return "color: #EF4444; font-weight: 600;"

                    styled_df = (
                        final_df.style
                        .format({"amount": "UGX {:,.0f}"})
                        .map(
                            style_amount,
                            subset=["amount"]
                        )
                    )

                    st.dataframe(
                        styled_df,
                        use_container_width=True,
                        hide_index=True
                    )

                else:
                    st.info("No recorded expenses.")

            except Exception as e:
                st.error(
                    f"Expenses feed error: {e}"
                )

        # --- EXPORT SECTION ---
        st.write("---")

        c1, c2 = st.columns(2)

        with c1:

            csv_data = loans_df.to_csv(
                index=False
            ).encode("utf-8")

            st.download_button(
                label="📥 Download Underlying Data (CSV)",
                data=csv_data,
                file_name=f"portfolio_data_{pd.Timestamp.now().strftime('%Y-%m-%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )

        with c2:

            csv2 = expenses_df.to_csv(
                index=False
            ).encode("utf-8")

            st.download_button(
                label="⬇️ Export Expenses CSV",
                data=csv2,
                file_name="expenses_report.csv",
                mime="text/csv",
                use_container_width=True
            )

    except Exception as e:

        st.error(
            f"Dashboard recovered from an internal issue: {str(e)}"
        )
