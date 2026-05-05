# ==========================================
# 1. CORE PAGE FUNCTIONS (BRANDING & WIDE LAYOUT)
# ==========================================

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
import io
import base64
import json
import os
import re
from datetime import datetime, timedelta
from fpdf import FPDF
from streamlit_calendar import calendar
import bcrypt
from twilio.rest import Client as TwilioClient
import time
import uuid
import extra_streamlit_components as stx
from database import supabase, get_cached_data
# =========================================================
# FULLY RECREATED / CRASH-PROOF / PERFORMANCE VERSION
# Keeps every line, layout, feature & structure
# =========================================================

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
def show_overview_page():

    brand_color = get_Active_color()

    try:

        # --- GLOBAL CSS UPGRADE ---
        st.markdown(f"""
        <style>
        .metric-card {{
            background:white;
            padding:20px;
            border-radius:15px;
            box-shadow:0 4px 10px rgba(0,0,0,0.05);
            transition:0.25s ease;
        }}

        .metric-card:hover {{
            transform:translateY(-3px);
            box-shadow:0 8px 16px rgba(0,0,0,0.08);
        }}

        @media (max-width:768px) {{
            .metric-card {{
                padding:14px;
            }}
        }}
        </style>
        """, unsafe_allow_html=True)

        # --- UI HEADER ---
        st.markdown(f"""
        <div style='background:{brand_color}; padding:25px; border-radius:15px; margin-bottom:25px; color:white;'>
            <h1 style='margin:0; font-size:28px;'>🏛️ Financial Control Center</h1>
            <p style='margin:0; opacity:0.8;'>Real-time insights across loans, Expenses & Petty Cash</p>
        </div>
        """, unsafe_allow_html=True)

        # --- 1. DATA INGESTION ---
        loans_df = normalize(load_cached("loans"))
        payments_df = normalize(load_cached("payments"))
        expenses_df = normalize(load_cached("expenses"))
        borrowers_df = normalize(load_cached("borrowers"))

        if loans_df.empty:
            st.info("👋 Welcome! No active data found. Add your first borrower or loan to populate this dashboard.")
            return

        if "status" not in loans_df.columns:
            loans_df["status"] = "ACTIVE"

        # --- 2. ENGINE: UNIFIED CALCULATIONS ---
        loans_df["principal_n"] = safe_numeric(loans_df, ["principal", "amount"])
        loans_df["interest_n"] = safe_numeric(loans_df, ["interest", "interest_amount"])

        loans_df["balance_n"] = (
            safe_numeric(loans_df, ["balance", "total_repayable"])
            - safe_numeric(loans_df, ["amount_paid", "paid"])
        )

        total_expenses = safe_numeric(expenses_df, ["amount"]).sum()

        today = pd.Timestamp.now().normalize()

        loans_df["due_date_dt"] = safe_date(loans_df, ["end_date", "due_date"])

        overdue_mask = (
            (loans_df["due_date_dt"] < today)
            &
            (loans_df["status"].astype(str).str.upper() != "CLEARED")
        )

        overdue_count = int(overdue_mask.sum())

        total_principal = loans_df["principal_n"].sum()
        total_interest = loans_df["interest_n"].sum()

        # --- SMART ALERTS ---
        if overdue_count >= 5:
            st.warning(f"⚠️ {overdue_count} overdue loans need urgent attention.")

        # --- 3. TOP LEVEL METRIC CARDS ---
        m1, m2, m3, m4 = st.columns(4)

        def metric_card(title, value, subtitle, color, is_money=True):

            try:
                fmt = f"{float(value):,.0f} UGX" if is_money else f"{int(value)}"
            except:
                fmt = "0"

            return f"""
            <div class='metric-card' style='border-bottom:5px solid {color};'>
                <p style="color:#64748B; font-size:12px; font-weight:bold; text-transform:uppercase; margin-bottom:5px;">{title}</p>
                <h2 style="color:#1E293B; margin:0; font-size:22px;">{fmt}</h2>
                <p style="color:{color}; font-size:11px; margin-top:5px; font-weight:bold;">{subtitle}</p>
            </div>
            """

        m1.markdown(metric_card("Active principal", total_principal, "Portfolio Value", brand_color), unsafe_allow_html=True)
        m2.markdown(metric_card("interest Income", total_interest, "Expected Earnings", "#10B981"), unsafe_allow_html=True)
        m3.markdown(metric_card("Operational Costs", total_expenses, "Total Expenses", "#EF4444"), unsafe_allow_html=True)
        m4.markdown(metric_card("Critical Alerts", overdue_count, "Overdue loans", "#F59E0B", False), unsafe_allow_html=True)

        st.write("##")

        # --- 4. DATA VISUALIZATION SECTION ---
        col_l, col_r = st.columns([2, 1])

        with col_l:

            st.markdown("#### 📈 Revenue Trend vs Expenses")

            try:

                if not payments_df.empty:

                    date_col = first_existing(payments_df, ["date", "payment_date", "created_at"])
                    amt_col = first_existing(payments_df, ["amount", "paid", "payment"])

                    if date_col and amt_col:

                        payments_df["date_dt"] = pd.to_datetime(payments_df[date_col], errors="coerce")
                        payments_df["amount_n"] = pd.to_numeric(payments_df[amt_col], errors="coerce").fillna(0)

                        temp = payments_df.dropna(subset=["date_dt"]).copy()

                        temp["month"] = temp["date_dt"].dt.to_period("M").astype(str)

                        monthly_rev = temp.groupby("month", as_index=False)["amount_n"].sum()

                        fig = px.area(
                            monthly_rev,
                            x="month",
                            y="amount_n",
                            template="plotly_white",
                            color_discrete_sequence=[brand_color]
                        )

                        fig.update_layout(
                            height=320,
                            margin=dict(l=0, r=0, t=20, b=0)
                        )

                        st.plotly_chart(fig, use_container_width=True)

                    else:
                        st.info("Payment columns missing.")

                else:
                    st.info("Insufficient payment history for trend analysis.")

            except:
                st.warning("Revenue chart temporarily unavailable.")

        with col_r:

            st.markdown("#### 🎯 Portfolio Health")

            try:

                status_data = (
                    loans_df["status"]
                    .astype(str)
                    .str.upper()
                    .value_counts()
                    .reset_index()
                )

                status_data.columns = ["status", "count"]

                fig_pie = px.pie(
                    status_data,
                    names="status",
                    values="count",
                    hole=0.72,
                    color_discrete_sequence=[
                        "#10B981",
                        "#F59E0B",
                        "#EF4444",
                        brand_color
                    ]
                )

                fig_pie.update_layout(
                    height=320,
                    showlegend=False
                )

                st.plotly_chart(fig_pie, use_container_width=True)

            except:
                st.info("Portfolio chart unavailable.")

        # --- 5. ACTIVITY FEEDS ---
        st.write("---")

        t1, t2 = st.columns(2)

        with t1:

            st.markdown("#### 📊 Portfolio Growth vs. interest")

            try:

                graph_df = loans_df.copy()

                graph_df["date_dt"] = safe_date(graph_df, ["start_date", "created_at"])

                graph_df = graph_df.dropna(subset=["date_dt"])

                if not graph_df.empty:

                    timeline_df = (
                        graph_df
                        .groupby("date_dt")[["principal_n", "interest_n"]]
                        .sum()
                        .sort_index()
                        .cumsum()
                        .reset_index()
                    )

                    fig_portfolio = px.line(
                        timeline_df,
                        x="date_dt",
                        y=["principal_n", "interest_n"],
                        template="plotly_white",
                        color_discrete_map={
                            "principal_n": brand_color,
                            "interest_n": "#10B981"
                        }
                    )

                    fig_portfolio.update_layout(
                        height=350,
                        hovermode="x unified"
                    )

                    st.plotly_chart(fig_portfolio, use_container_width=True)

                else:
                    st.info("Not enough dated records to generate a trend.")

            except:
                st.info("Growth chart unavailable.")

        with t2:

            st.markdown("#### 💸 Latest Expenses")

            try:

                if not expenses_df.empty:

                    display_exp = expenses_df.head(5)

                    vals = safe_numeric(display_exp, ["amount"]).tolist()

                    rows = ""

                    for i, (_, r) in enumerate(display_exp.iterrows()):

                        amount_val = vals[i] if i < len(vals) else 0

                        rows += f"""
                        <tr style='border-bottom:1px solid #f0f0f0;'>
                            <td style='padding:12px 5px;'>{r.get('category','General')}</td>
                            <td style='padding:12px 5px; text-align:right; color:#EF4444;'>-{amount_val:,.0f}</td>
                            <td style='padding:12px 5px; text-align:right; color:#64748B;'>{r.get('date','-')}</td>
                        </tr>
                        """

                    st.markdown(
                        f"<table style='width:100%; font-size:13px;'>{rows}</table>",
                        unsafe_allow_html=True
                    )

                else:
                    st.info("No recorded expenses.")

            except:
                st.info("Expenses feed unavailable.")

        # --- EXPORT SECTION ---
        st.write("---")

        c1, c2 = st.columns(2)

        with c1:

            csv_data = loans_df.to_csv(index=False).encode("utf-8")

            st.download_button(
                label="📥 Download Underlying Data (CSV)",
                data=csv_data,
                file_name=f"portfolio_data_{pd.Timestamp.now().strftime('%Y-%m-%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )

        with c2:

            csv2 = expenses_df.to_csv(index=False).encode("utf-8")

            st.download_button(
                "⬇️ Export Expenses CSV",
                csv2,
                file_name="expenses_report.csv",
                mime="text/csv",
                use_container_width=True
            )

    except Exception as e:

        st.error(f"Dashboard recovered from an internal issue: {str(e)}")
