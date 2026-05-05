# ==============================
# 📅 17. ACTIVITY CALENDAR PAGE
# ==============================
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

def show_calendar_page():
    st.markdown("<h2 style='color: #2B3F87;'>📅 Activity Calendar</h2>", unsafe_allow_html=True)

    # 1. FETCH DATA (SAFE ADAPTERS)
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    # --- 👤 INJECT borrower nameS (MAPPING) ---
    if borrowers_df is not None and not borrowers_df.empty:
        borrowers_df['id'] = borrowers_df['id'].astype(str)
        loans_df['borrower_id'] = loans_df['borrower_id'].astype(str)
        
        bor_map = dict(zip(borrowers_df['id'], borrowers_df['name']))
        loans_df['borrower'] = loans_df['borrower_id'].map(bor_map).fillna("Unknown borrower")
    else:
        loans_df['borrower'] = "Unknown borrower"

    # --- 🛡️ STANDARDIZATION ---
    loans_df["end_date"] = pd.to_datetime(loans_df["end_date"], errors="coerce")
    loans_df["total_repayable"] = pd.to_numeric(loans_df["total_repayable"], errors="coerce").fillna(0)
    
    today = pd.Timestamp.today().normalize()
    
    # Filter for active loans
    active_loans = loans_df[~loans_df["status"].astype(str).str.upper().isin(["CLEARED", "CLOSED"])].copy()

    # --- 🎨 VISUAL CALENDAR WIDGET ---
    calendar_events = []
    for _, r in active_loans.iterrows():
        if pd.notna(r['end_date']):
            is_overdue = r['end_date'].date() < today.date()
            ev_color = "#FF4B4B" if is_overdue else "#4A90E2"
            
            amount_fmt = f"UGX {float(r['total_repayable']):,.0f}"
            calendar_events.append({
                "title": f"{amount_fmt} - {r['borrower']}",
                "start": r['end_date'].strftime("%Y-%m-%d"),
                "end": r['end_date'].strftime("%Y-%m-%d"),
                "color": ev_color,
                "allDay": True,
            })

    calendar_options = {
        "headerToolbar": {"left": "prev,next today", "center": "title", "right": "dayGridMonth,timeGridWeek"},
        "initialView": "dayGridMonth",
        "selectable": True,
    }

    calendar(events=calendar_events, options=calendar_options, key="collection_cal")
    
    st.markdown("---")

    # 2. 📊 DAILY WORKLOAD METRICS
    due_today_df = active_loans[active_loans["end_date"].dt.date == today.date()]
    upcoming_df = active_loans[
        (active_loans["end_date"] > today) & 
        (active_loans["end_date"] <= today + pd.Timedelta(days=7))
    ]
    overdue_count = active_loans[active_loans["end_date"] < today].shape[0]

    m1, m2, m3 = st.columns(3)
    m1.markdown(f"""<div style="background-color:#fff;padding:20px;border-radius:15px;border-left:5px solid #2B3F87;box-shadow:2px 2px 10px rgba(0,0,0,0.05);"><p style="margin:0;font-size:12px;color:#666;font-weight:bold;">DUE TODAY |</p><p style="margin:0;font-size:18px;color:#2B3F87;font-weight:bold;">{len(due_today_df)} Accounts</p></div>""", unsafe_allow_html=True)
    m2.markdown(f"""<div style="background-color:#F0F8FF;padding:20px;border-radius:15px;border-left:5px solid #2B3F87;box-shadow:2px 2px 10px rgba(0,0,0,0.05);"><p style="margin:0;font-size:12px;color:#666;font-weight:bold;">UPCOMING (7 DAYS) |</p><p style="margin:0;font-size:18px;color:#2B3F87;font-weight:bold;">{len(upcoming_df)} Accounts</p></div>""", unsafe_allow_html=True)
    m3.markdown(f"""<div style="background-color:#FFF5F5;padding:20px;border-radius:15px;border-left:5px solid #D32F2F;box-shadow:2px 2px 10px rgba(0,0,0,0.05);"><p style="margin:0;font-size:12px;color:#D32F2F;font-weight:bold;">TOTAL OVERDUE |</p><p style="margin:0;font-size:18px;color:#D32F2F;font-weight:bold;">{overdue_count} Accounts</p></div>""", unsafe_allow_html=True)

    # 3. 📈 REVENUE FORECAST
    st.markdown("---")
    st.markdown("<h4 style='color: #2B3F87;'>📊 Revenue Forecast (This Month)</h4>", unsafe_allow_html=True)
    this_month_df = active_loans[active_loans["end_date"].dt.month == today.month]
    total_expected = this_month_df["total_repayable"].sum()
    f1, f2 = st.columns(2)
    f1.metric("Expected Collections", f"{total_expected:,.0f} UGX")
    f2.metric("Deadlines This Month", len(this_month_df))

    # 4. 📌 ACTION ITEMS
    st.markdown("<h4 style='color: #2B3F87;'>📌 Action Items for Today</h4>", unsafe_allow_html=True)
    if due_today_df.empty:
        st.success("✨ No collection deadlines for today.")
    else:
        today_rows = "".join([f"""
            <tr style="background:#F0F8FF;">
                <td style="padding:10px;"><b>#{r.get('loan_id_label', str(r['id'])[:8])}</b></td>
                <td style="padding:10px;">{r['borrower']}</td>
                <td style="padding:10px;text-align:right;">{r['total_repayable']:,.0f}</td>
                <td style="padding:10px;text-align:center;">
                    <span style="background:#2B3F87;color:white;padding:2px 8px;border-radius:10px;font-size:10px;">💰 COLLECT NOW</span>
                </td>
            </tr>""" for _, r in due_today_df.iterrows()])
        st.markdown(f"""<div style="border:2px solid #2B3F87;border-radius:10px;overflow:hidden;"><table style="width:100%;border-collapse:collapse;font-size:12px;"><tr style="background:#2B3F87;color:white;"><th style="padding:10px;">Loan ID</th><th style="padding:10px;">borrower</th><th style="padding:10px;text-align:right;">Amount</th><th style="padding:10px;text-align:center;">Action</th></tr>{today_rows}</table></div>""", unsafe_allow_html=True)

    # 5. 🔴 OVERDUE FOLLOW-UP (Now safely inside the function)
    st.markdown("<br><h4 style='color: #FF4B4B;'>🔴 Overdue Follow-up</h4>", unsafe_allow_html=True)
    try:
        # Re-using the active_loans we already filtered at the top of the function
        overdue_df = active_loans[active_loans["end_date"] < today].copy()

        if not overdue_df.empty:
            overdue_df["days_late"] = (today - overdue_df["end_date"]).dt.days
            od_rows = ""
            for _, r in overdue_df.iterrows():
                late_color = "#FF4B4B" if r['days_late'] > 7 else "#FFA500"
                label = r.get('loan_id_label') if pd.notna(r.get('loan_id_label')) else str(r['id'])[:8]
                od_rows += f"""
                    <tr style="background:#FFF5F5;">
                        <td style="padding:10px; border-bottom:1px solid #eee;"><b>#{label}</b></td>
                        <td style="padding:10px; border-bottom:1px solid #eee;">{r['borrower']}</td>
                        <td style="padding:10px; border-bottom:1px solid #eee; color:{late_color}; font-weight:bold;">{r['days_late']} Days</td>
                        <td style="padding:10px; border-bottom:1px solid #eee; text-align:center;">
                            <span style="background:{late_color}; color:white; padding:2px 8px; border-radius:10px; font-size:10px;">{r['status']}</span>
                        </td>
                    </tr>"""
            st.markdown(f"""<div style="border:2px solid #FF4B4B; border-radius:10px; overflow:hidden;"><table style="width:100%; border-collapse:collapse; font-size:12px;"><tr style="background:#FF4B4B; color:white;"><th style="padding:10px; text-align:left;">Loan ID</th><th style="padding:10px; text-align:left;">borrower</th><th style="padding:10px; text-align:center;">Late By</th><th style="padding:10px; text-align:center;">Status</th></tr>{od_rows}</table></div>""", unsafe_allow_html=True)
        else:
            st.info("No overdue loans currently. Everything is on track! ✨")
    except Exception as e:
        st.error(f"Error generating overdue table: {e}") 
