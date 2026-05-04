# ==============================
# 🚨 20. AI OVERDUE TRACKER (BULLETPROOF)
# ==============================
import pandas as pd
import streamlit as st
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

def show_overdue_tracker():
    """
    Tracks overdue loans with AI-style risk scoring and tenant isolation.
    """
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    # ==============================
    # 🎨 UI SYSTEM
    # ==============================
    st.markdown("""
    <style>
    .glass-card {
        backdrop-filter: blur(10px);
        background: linear-gradient(145deg, rgba(255,255,255,0.9), rgba(245,247,255,0.8));
        border-radius: 16px;
        padding: 20px;
        border: 1px solid rgba(239, 68, 68, 0.1);
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
    }
    .metric-title { font-size: 11px; color: #6b7280; font-weight: 600; text-transform: uppercase; }
    .metric-value { font-size: 24px; font-weight: 700; margin-top: 4px; }
    .ai-badge {
        background: #F0F4FF;
        color: #2B3F87;
        padding: 2px 8px;
        border-radius: 8px;
        font-size: 10px;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f"<h2 style='color:{brand_color};'>🚨 AI Overdue Intelligence</h2>", unsafe_allow_html=True)

    # ==============================
    # 📥 FETCH & PROTECT DATA
    # ==============================
    loans_df = get_cached_data("loans")
    
    if loans_df is None or loans_df.empty:
        st.info("📅 No loan records found in the system.")
        return

    # 🛡️ Tenant Isolation & Normalization
    loans_df = loans_df[loans_df["tenant_id"].astype(str) == str(current_tenant)].copy()
    
    required_cols = ["id", "amount", "due_date", "borrower_id", "status"]
    for col in required_cols:
        if col not in loans_df.columns: loans_df[col] = None

    loans_df["amount"] = pd.to_numeric(loans_df["amount"], errors="coerce").fillna(0)
    loans_df["due_date"] = pd.to_datetime(loans_df["due_date"], errors="coerce")
    
    # Filter for unpaid loans only
    active_df = loans_df[~loans_df["status"].astype(str).str.upper().isin(["PAID", "CLOSED", "CLEARED"])].copy()

    if active_df.empty:
        st.success("✅ Great job! All loans are currently up to date or cleared.")
        return

    # ==============================
    # 🧠 AI RISK SCORING ENGINE
    # ==============================
    today = pd.Timestamp.today().normalize()
    active_df["days_overdue"] = (today - active_df["due_date"]).dt.days
    
    # Only keep loans that are actually overdue
    overdue_df = active_df[active_df["days_overdue"] > 0].copy()

    if overdue_df.empty:
        st.success("🎉 No overdue payments detected today.")
        return

    def compute_risk_score(row):
        score = 0
        # 1. Time Component (Max 50 points)
        score += min(row["days_overdue"] * 1.5, 50)
        # 2. Value Component (Max 30 points)
        if row["amount"] > 1000000: score += 30
        elif row["amount"] > 500000: score += 20
        elif row["amount"] > 100000: score += 10
        # 3. Critical Thresholds
        if row["days_overdue"] > 30: score += 20 
        return min(score, 100)

    overdue_df["risk_score"] = overdue_df.apply(compute_risk_score, axis=1)

    def classify_risk(score):
        if score >= 75: return "🔴 High Risk"
        elif score >= 40: return "🟠 Watch"
        return "🟢 Stable"

    overdue_df["risk_level"] = overdue_df["risk_score"].apply(classify_risk)

    # ==============================
    # 💎 KPI DASHBOARD
    # ==============================
    total_count = len(overdue_df)
    critical_count = len(overdue_df[overdue_df["risk_score"] >= 75])
    total_exposure = overdue_df["amount"].sum()

    c1, c2, c3 = st.columns(3)
    c1.markdown(f"""<div class="glass-card"><div class="metric-title">Overdue Cases</div><div class="metric-value">{total_count}</div></div>""", unsafe_allow_html=True)
    c2.markdown(f"""<div class="glass-card"><div class="metric-title">Critical Attention</div><div class="metric-value" style="color:#EF4444;">{critical_count}</div></div>""", unsafe_allow_html=True)
    c3.markdown(f"""<div class="glass-card"><div class="metric-title">Total Exposure</div><div class="metric-value">UGX {total_exposure:,.0f}</div></div>""", unsafe_allow_html=True)

    # ==============================
    # 🔍 FILTER & SEARCH
    # ==============================
    st.markdown("<br>", unsafe_allow_html=True)
    f1, f2 = st.columns([1, 2])
    risk_filter = f1.selectbox("Filter Risk", ["All Levels", "🔴 High Risk", "🟠 Watch", "🟢 Stable"])
    search = f2.text_input("🔍 Search borrower or Loan ID")

    display_df = overdue_df.copy()
    if risk_filter != "All Levels":
        display_df = display_df[display_df["risk_level"] == risk_filter]
    if search:
        display_df = display_df[display_df.astype(str).apply(lambda x: search.lower() in x.str.lower().any(), axis=1)]

    # ==============================
    # 🎨 AI RANKED TABLE
    # ==============================
    st.markdown("### 🔥 Collection Priority List")
    
    st.dataframe(
        display_df.sort_values("risk_score", ascending=False)[
            ["days_overdue", "amount", "risk_level", "risk_score", "id"]
        ].rename(columns={
            "days_overdue": "Days Late",
            "amount": "balance (UGX)",
            "risk_level": "Risk Level",
            "risk_score": "Score/100",
            "id": "Loan ID"
        }),
        use_container_width=True,
        hide_index=True
    )

    # ==============================
    # 🧠 SMART INSIGHT PANEL
    # ==============================
    st.markdown("<br>", unsafe_allow_html=True)
    try:
        worst_case = overdue_df.loc[overdue_df["risk_score"].idxmax()]
        st.markdown(f"""
        <div class="glass-card" style="border-left: 5px solid #EF4444;">
            <span class="ai-badge">AI ANALYTICS</span><br><br>
            <b>Priority Alert:</b> Loan <b>#{worst_case['id']}</b> requires immediate legal or field intervention.<br>
            It is <b>{int(worst_case['days_overdue'])} days</b> overdue with a risk score of <b>{worst_case['risk_score']:.0f}/100</b>.<br>
            <p style="font-size:12px; color:#666; margin-top:10px;"><i>Strategy: Debt has entered the critical recovery phase. Check collateral status immediately.</i></p>
        </div>
        """, unsafe_allow_html=True)
    except:
        pass

    # ==============================
    # ⚙️ QUICK ACTIONS
    # ==============================
    with st.expander("⚙️ Recovery Actions"):
        target_loan = st.selectbox(
            "Select Loan to Action", 
            overdue_df.apply(lambda x: f"ID: {x['id']} | Late: {x['days_overdue']} days", axis=1)
        )
        
        sel_id = target_loan.split(" | ")[0].replace("ID: ", "")
        
        act1, act2 = st.columns(2)
        if act1.button("📞 Log Contact Made", use_container_width=True):
            st.toast(f"Contact log updated for Loan #{sel_id}")
            
        if act2.button("✅ Mark Fully Recovered", use_container_width=True):
            # Safe logic for status update
            update_data = pd.DataFrame([{"id": sel_id, "status": "Paid", "tenant_id": str(current_tenant)}])
            if save_data_saas("loans", update_data):
                st.success(f"Loan #{sel_id} moved to Paid status.")
                st.cache_data.clear()
                st.rerun()
