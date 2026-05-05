# ==============================
# 📁 18. EXPENSE MANAGEMENT PAGE (SAAS + ENTERPRISE UPGRADE)
# ==============================
import plotly.express as px
import uuid
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
from database import supabase, get_cached_data


def show_expenses_page():
    """
    Tracks business operational costs for specific tenants.
    """
    st.markdown("<h2 style='color: #2B3F87;'>📁 Expense Management</h2>", unsafe_allow_html=True)
    
    # ==============================
    # 🔐 SAAS TENANT CONTEXT
    # ==============================
    current_tenant = st.session_state.get('tenant_id')
    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    # ==============================
    # 📦 1. FETCH DATA (SAFE ADAPTER)
    # ==============================
    try:
        # Pulling data using your existing cache logic
        df = get_cached_data("expenses")
    except Exception:
        df = pd.DataFrame()

    # ==============================
    # 🛡️ SAAS FILTER & NORMALIZATION
    # ==============================
    if df is not None and not df.empty:
        # Standardize column naming
        df.columns = df.columns.str.lower().str.strip()
        
        # Enforce Tenant Isolation
        if "tenant_id" in df.columns:
            df = df[df["tenant_id"].astype(str) == str(current_tenant)].copy()
        else:
            df["tenant_id"] = current_tenant
    else:
        # Initialize empty schema if no data exists
        df = pd.DataFrame(columns=[
            "id", "category", "amount", "date",
            "description", "payment_date", "receipt_no", "tenant_id"
        ])

    EXPENSE_CATS = ["Rent", "Insurance", "Utilities", "Salaries", "Marketing", "Office Expenses", "Taxes", "Other"]

    # ==============================
    # 📑 TABS
    # ==============================
    tab_add, tab_view, tab_manage = st.tabs([
        "➕ Record Expense", "📊 Spending Analysis", "⚙️ Manage Records"
    ])

    # --- TAB 1: RECORD EXPENSE ---
    with tab_add:
        with st.form("add_expense_form", clear_on_submit=True):
            st.write("### Log Operational Cost")
            col1, col2 = st.columns(2)

            category = col1.selectbox("Category", EXPENSE_CATS)
            amount = col2.number_input("Amount (UGX)", min_value=0, step=1000)
            desc = st.text_input("Description / Particulars")

            c_date, c_receipt = st.columns(2)
            p_date = c_date.date_input("Actual Payment Date", value=datetime.now())
            receipt_no = c_receipt.text_input("Receipt / Invoice Reference #")

            if st.form_submit_button("🚀 Save Expense Record", use_container_width=True):
                if amount > 0 and desc:
                    try:
                        new_entry = pd.DataFrame([{
                            "id": str(uuid.uuid4()),
                            "category": category,
                            "amount": float(amount),
                            "date": datetime.now().strftime("%Y-%m-%d"),
                            "description": desc,
                            "payment_date": p_date.strftime("%Y-%m-%d"),
                            "receipt_no": receipt_no,
                            "tenant_id": str(current_tenant)
                        }])

                        # Utilize your global save_data adapter
                        if save_data("expenses", pd.concat([df, new_entry], ignore_index=True)):
                            st.success("✅ Expense successfully recorded!")
                            st.cache_data.clear() 
                            st.rerun()
                    except Exception as e:
                        st.error(f"🚨 Save failed: {e}")
                else:
                    st.warning("⚠️ Please provide a valid amount and description.")

    # --- TAB 2: SPENDING ANALYSIS ---
    with tab_view:
        if df.empty:
            st.info("💡 No expenses recorded yet for this period.")
        else:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
            total_spent = df["amount"].sum()
            
            # Metric Card
            st.markdown(f"""
                <div style="background-color:#fff;padding:20px;border-radius:15px;border-left:5px solid #FF4B4B;box-shadow:2px 2px 10px rgba(0,0,0,0.05);">
                    <p style="margin:0;font-size:12px;color:#666;font-weight:bold;">TOTAL CUMULATIVE OUTFLOW</p>
                    <h2 style="margin:0;color:#FF4B4B;">UGX {total_spent:,.0f}</h2>
                </div><br>""", unsafe_allow_html=True)
            
            # 📊 PIE CHART ANALYSIS
            cat_summary = df.groupby("category")["amount"].sum().reset_index()
            fig_exp = px.pie(
                cat_summary, 
                names="category", 
                values="amount", 
                title="Spending Distribution by Category",
                hole=0.4, 
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_exp.update_layout(paper_bgcolor='rgba(0,0,0,0)', font_color="#2B3F87")
            st.plotly_chart(fig_exp, use_container_width=True)
            
            # 📋 DETAILED LEDGER
            st.markdown("### Expense Ledger")
            rows_html = ""
            # Sort by date descending
            sorted_df = df.sort_values("payment_date", ascending=False).reset_index()
            
            for i, r in sorted_df.iterrows():
                bg = "#F9FBFF" if i % 2 == 0 else "#FFFFFF"
                rows_html += f"""
                    <tr style="background-color:{bg}; border-bottom: 1px solid #eee;">
                        <td style="padding:10px;">{r['payment_date']}</td>
                        <td style="padding:10px;"><b>{r['category']}</b></td>
                        <td style="padding:10px; font-size:11px;">{r['description']}</td>
                        <td style="padding:10px; text-align:right; font-weight:bold; color:#D32F2F;">{float(r['amount']):,.0f}</td>
                        <td style="padding:10px; text-align:center; color:#666;">{r['receipt_no']}</td>
                    </tr>"""

            st.markdown(f"""
                <div style="border:1px solid #2B3F87; border-radius:10px; overflow:hidden;">
                    <table style="width:100%; border-collapse:collapse; font-size:12px;">
                        <thead>
                            <tr style="background:#2B3F87; color:white; text-align:left;">
                                <th style="padding:12px;">Date</th>
                                <th style="padding:12px;">Category</th>
                                <th style="padding:12px;">Description</th>
                                <th style="padding:12px; text-align:right;">Amount (UGX)</th>
                                <th style="padding:12px; text-align:center;">Ref #</th>
                            </tr>
                        </thead>
                        <tbody>{rows_html}</tbody>
                    </table>
                </div>""", unsafe_allow_html=True)

    # --- TAB 3: MANAGE (CRUD) ---
    with tab_manage:
        st.markdown("### 🛠️ Record Maintenance")

        if df.empty:
            st.info("No expense records available to modify.")
        else:
            df["id"] = df["id"].astype(str)
            # Create identifiable label for selection
            df["selector_label"] = df.apply(
                lambda r: f"{r['payment_date']} | {r['category']} | UGX {r['amount']:,.0f}", axis=1
            )

            record_map = {row["selector_label"]: row for _, row in df.iterrows()}
            selected_label = st.selectbox("Select Record to Edit/Delete", list(record_map.keys()))

            if selected_label:
                target_record = record_map[selected_label]
                
                with st.form("edit_expense_form"):
                    new_amt = st.number_input("Update Amount (UGX)", value=float(target_record['amount']))
                    new_desc = st.text_input("Update Description", value=target_record['description'])
                    
                    c1, c2 = st.columns(2)
                    save_btn = c1.form_submit_button("💾 Save Changes", use_container_width=True)
                    delete_btn = c2.form_submit_button("🗑️ Delete Record", use_container_width=True)

                    if save_btn:
                        df.loc[df["id"] == target_record["id"], ["amount", "description"]] = [new_amt, new_desc]
                        if save_data("expenses", df.drop(columns=['selector_label'])):
                            st.success("✅ Record updated!")
                            st.cache_data.clear()
                            st.rerun()

                    if delete_btn:
                        df = df[df["id"] != target_record["id"]]
                        if save_data("expenses", df.drop(columns=['selector_label'])):
                            st.warning("🗑️ Record deleted.")
                            st.cache_data.clear()
                            st.rerun()
