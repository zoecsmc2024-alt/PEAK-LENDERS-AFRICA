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
from database import supabase, get_cached_data, save_data


# --- Page Config stays here ---
st.set_page_config(
    page_title="Lending Manager Pro",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)
# Constants
SESSION_TIMEOUT = 30





def render_sidebar():
    """Renders the business selector, branding, and main navigation."""
    # 1️⃣ Fetch tenants
    try:
        tenants_res = supabase.table("tenants")\
            .select("id, name, brand_color, logo_url")\
            .execute()
        tenant_map = {row['name']: row for row in tenants_res.data} if tenants_res.data else {}
    except Exception as e:
        st.sidebar.error(f"Error fetching tenants: {e}")
        tenant_map = {}

    # 2️⃣ Sidebar UI
    with st.sidebar:
        st.markdown('<div style="padding-top:10px;"></div>', unsafe_allow_html=True)

        active_company = None  # ✅ Initialize to avoid undefined variable

        if tenant_map:
            options = list(tenant_map.keys())
            current_tenant_id = st.session_state.get('tenant_id')

            # Determine default index in dropdown
            default_index = 0
            if current_tenant_id:
                for i, name in enumerate(options):
                    if str(tenant_map[name]['id']) == str(current_tenant_id):
                        default_index = i
                        break

            selected_name = st.selectbox(
                "🏢 Business Portal",
                options,
                index=default_index,
                key="sidebar_portal_select"
            )

            active_company = tenant_map.get(selected_name)
            
            # ✅ ADD THIS LINE to define the missing variable
            active_company_name = selected_name
            # 🔑 Login form with forced white text for visibility
            if active_company and (st.session_state.get('tenant_id') != active_company['id']):
                # Force the header to be white
                st.markdown(f"<h3 style='color:white;'>🔑 Login to {selected_name}</h3>", unsafe_allow_html=True)
                
                # Use a container to target sub-labels if needed, 
                # but standard markdown with styling is most reliable:
                st.markdown("<p style='color:white; margin-bottom:-15px;'>Email</p>", unsafe_allow_html=True)
                email = st.text_input("", key="login_email", placeholder="Enter your email")
                
                st.markdown("<p style='color:white; margin-bottom:-15px; margin-top:10px;'>Password</p>", unsafe_allow_html=True)
                pwd = st.text_input("", type="password", key="login_pwd", placeholder="Enter password")
                
                if st.button("Access Dashboard", key="login_button", use_container_width=True):
                    # ... (your existing login logic stays the same)
                    try:
                        # Attempt Supabase login
                        res = supabase.auth.sign_in_with_password({"email": email, "password": pwd})
                        if not res.user:
                            st.error("Login failed. Check credentials.")
                        else:
                            # Successful login → update session state
                            st.session_state.update({
                                'tenant_id': active_company['id'],
                                'company': selected_name,
                                'theme_color': active_company.get('brand_color', '#1E3A8A'),
                                'user_id': res.user.id,
                                'logged_in': True
                            })
                            st.success(f"✅ Logged in to {selected_name}")
                            st.cache_data.clear()
                            st.rerun()
                    except Exception as e:
                        st.error(f"Login error: {e}")

        else:
            st.sidebar.warning("No business entities found.")
            st.stop()
        # ==============================
        # 💎 BRANDING (LOGO & name)
        # ==============================
        logo_val = active_company.get('logo_url') if active_company else None
        final_logo_url = None

        if logo_val and str(logo_val).lower() not in ["0", "none", "null", ""]:
            if str(logo_val).startswith("http"):
                final_logo_url = logo_val
            else:
                # Construct public Supabase storage URL
                proj_url = st.secrets.get("SUPABASE_URL", "").strip("/")
                if proj_url:
                    final_logo_url = f"{proj_url}/storage/v1/object/public/company-logos/{logo_val}"

        # Render Logo with CSS "Glow"
        if final_logo_url:
            st.markdown(f"""
            <div style="display:flex; justify-content:center; align-items:center; margin-top:10px;">
                <div style="padding:10px; border-radius:50%; background: radial-gradient(circle, rgba(255,255,255,0.2) 0%, rgba(255,255,255,0) 70%);">
                    <img src="{final_logo_url}?t={int(time.time())}" width="75" style="border-radius:50%; object-fit:cover; aspect-ratio: 1/1;" />
                </div>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("<h1 style='text-align:center; margin-top:10px;'>🏢</h1>", unsafe_allow_html=True)

        st.markdown(f"""
            <div style='text-align:center; font-weight:600; font-size:16px; color:#f1f5f9;'>
                {active_company_name} <span style="color:#22c55e;">✔</span>
            </div>
            <div style='text-align:center; font-size:10px; color:rgba(255,255,255,0.6); letter-spacing:2px; margin-bottom:10px;'>FINANCE CORE</div>
        """, unsafe_allow_html=True)

        st.divider()

        # ==============================
        # 📍 NAVIGATION MENU
        # ==============================
        menu = {
            "Overview": "📈", "loans": "💵", "borrowers": "👥", "Collateral": "🛡️",
            "Calendar": "📅", "Ledger": "📄", "Payroll": "💳", "Expenses": "📉",
            "Petty Cash": "🪙", "Overdue Tracker": "🚨", "Payments": "💰", "Reports": "📊", "Settings": "⚙️"
        }

        menu_options = [f"{emoji} {name}" for name, emoji in menu.items()]
        current_p = st.session_state.get('current_page', "Overview")

        try:
            default_ix = list(menu.keys()).index(current_p)
        except ValueError:
            default_ix = 0

        selection = st.radio(
            "Navigation",
            menu_options,
            index=default_ix,
            label_visibility="collapsed",
            key="navigation_radio"
        )

        selected_page = selection.split(" ", 1)[1]
        st.session_state['current_page'] = selected_page

        # ==============================
        # 🔐 LOGOUT BUTTON
        # ==============================
        if st.session_state.get("authenticated"):
            st.markdown("<div style='margin-top:30px;'></div>", unsafe_allow_html=True)
            if st.button("🚪 Logout", use_container_width=True, type="secondary"):
                # Clear all session state keys
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                
                st.success("Logged out successfully.")
                time.sleep(0.5)
                st.rerun()

    return selected_page

# ==============================
# 🚀 borrowers ENGINE (PRODUCTION)
# ==============================

def show_borrowers_page():

    # ==============================
    # 🎨 BRANDING & THEME
    # ==============================
    brand_color = st.session_state.get("theme_color", "#1E3A8A")
    st.markdown(f"<h2 style='color:{brand_color};'>🚀 borrowers Registry</h2>", unsafe_allow_html=True)

    # ==============================
    # 🔐 TENANT SESSION CHECK
    # ==============================
    tenant_id = st.session_state.get("tenant_id")
    if not tenant_id:
        st.error("Session expired. Please log in again.")
        st.stop()

    # ==============================
    # 🧠 SAFE HELPERS (INTERNAL)
    # ==============================
    def safe_df(df):
        """
        Ensure the object is a valid pandas DataFrame.
        Returns an empty DataFrame if not.
        """
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    def safe_numeric(df, col, default=0.0, as_int=False):
        """
        Safely extract a numeric column from a DataFrame.
        - If column doesn't exist, fills with default.
        - If as_int=True, returns int64 dtype (useful for BIGINT DB fields).
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.Series(dtype="int64" if as_int else "float64")

        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
        else:
            s = pd.Series([default] * len(df), index=df.index)

        s = s.fillna(default)

        if as_int:
            return s.astype("int64")
        return s

    # ==============================
    # 📥 LOAD & NORMALIZE DATA
    # ==============================
    borrowers_df = safe_df(get_data("borrowers"))
    loans_df = safe_df(get_data("loans"))

    # Force lowercase column names for consistency
    for df in [borrowers_df, loans_df]:
        if not df.empty:
            df.columns = df.columns.str.strip().str.lower()

    # Apply Tenant Filters
    tenant_id = get_current_tenant() 
    if not borrowers_df.empty and "tenant_id" in borrowers_df.columns:
        borrowers_df = borrowers_df[borrowers_df["tenant_id"].astype(str) == str(tenant_id)]
    if not loans_df.empty and "tenant_id" in loans_df.columns:
        loans_df = loans_df[loans_df["tenant_id"].astype(str) == str(tenant_id)]

    # Ensure required structural columns exist
    required_cols = ["id", "name", "phone", "email", "status", "national_id", "next_of_kin"]
    for col in required_cols:
        if col not in borrowers_df.columns:
            borrowers_df[col] = ""

    # ==============================
    # 🔗 THE name FIX: DATA LINKAGE
    # ==============================
    if not borrowers_df.empty and not loans_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str).str.strip()
        loans_df["borrower_id"] = loans_df["borrower_id"].astype(str).str.strip()
        
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].map(bor_map).fillna("Unknown borrower")
    else:
        loans_df["borrower"] = "Unknown"

    # ==============================
    # 🔥 REAL-TIME RISK ENGINE
    # ==============================
    risk_map = {}
    if not loans_df.empty:
        loans_df["balance"] = safe_numeric(loans_df, "balance")
        # Handle different end_date column naming possibilities
        date_col = "end_date" if "end_date" in loans_df.columns else "due_date"
        loans_df["parsed_due_date"] = pd.to_datetime(loans_df[date_col], errors="coerce")

        today = pd.Timestamp.today().normalize()
        loans_df["days_overdue"] = (today - loans_df["parsed_due_date"]).dt.days
        loans_df["days_overdue"] = loans_df["days_overdue"].apply(lambda x: x if x > 0 else 0)
        loans_df["is_overdue"] = (loans_df["days_overdue"] > 0) & (loans_df["balance"] > 0)

        risk_df = loans_df.groupby("borrower_id").agg({
            "balance": "sum",
            "is_overdue": "sum",
            "days_overdue": "max"
        }).reset_index()

        risk_df.rename(columns={"balance": "exposure", "is_overdue": "overdue_count", "days_overdue": "max_days"}, inplace=True)

        def classify_risk(row):
            if row["overdue_count"] == 0: return "🟢 Healthy"
            elif row["max_days"] <= 7: return "🟡 Watch"
            elif row["max_days"] <= 30: return "🟠 Risk"
            else: return "🔴 Critical"

        risk_df["risk_label"] = risk_df.apply(classify_risk, axis=1)
        risk_map = risk_df.set_index("borrower_id").to_dict("index")

    # ==============================
    # 📑 UI NAVIGATION
    # ==============================
    tab_view, tab_add = st.tabs(["📋 View borrowers", "➕ Add borrower"])

    with tab_add:
        with st.form("add_borrower_form", clear_on_submit=True):
            st.markdown(f"<h4 style='color: {brand_color};'>📝 Register New borrower</h4>", unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            name = c1.text_input("Full name*")
            phone = c2.text_input("Phone Number*")
            email = c1.text_input("Email Address")
            nid = c2.text_input("National ID / NIN")
            addr = c1.text_input("Physical Address")
            nok = c2.text_input("Next of Kin (name & Contact)")
            
            if st.form_submit_button("🚀 Save borrower Profile", use_container_width=True):
                if name and phone:
                    new_id = str(uuid.uuid4())
                    new_entry = pd.DataFrame([{
                        "id": new_id, "name": name, "phone": phone, "email": email,
                        "national_id": nid, "address": addr, "next_of_kin": nok,
                        "status": "Active", "tenant_id": str(tenant_id)
                    }])
                    if save_data_saas("borrowers", new_entry):
                        st.success(f"✅ {name} registered successfully!")
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.error("⚠️ Full name and Phone Number are required.")

    with tab_view:
        search = st.text_input("🔍 Search by name or phone...").lower()

        if not borrowers_df.empty:
            df_to_show = borrowers_df.copy()
            for col in ["name", "phone", "national_id", "next_of_kin"]:
                df_to_show[col] = df_to_show[col].astype(str)

            mask = (
                df_to_show["name"].str.lower().str.contains(search, na=False) |
                df_to_show["phone"].str.contains(search, na=False)
            )
            filtered_df = df_to_show[mask]

            if not filtered_df.empty:
                rows_html = ""
                for i, r in filtered_df.reset_index().iterrows():
                    zebra_striping = "#F8FAFC" if i % 2 == 0 else "#FFFFFF"
                    b_id = str(r.get("id", ""))
                    
                    risk_data = risk_map.get(b_id, {})
                    label = risk_data.get("risk_label", "🟢 Healthy")
                    
                    if "🔴" in label: badge_color = "#EF4444"
                    elif "🟠" in label: badge_color = "#F97316"
                    elif "🟡" in label: badge_color = "#F59E0B"
                    else: badge_color = "#10B981"

                    rows_html += f"""
                    <tr style="background-color: {zebra_striping}; border-bottom: 1px solid #E2E8F0;">
                        <td style="padding:12px; font-weight:600; color:#1E293B;">{r['name']}</td>
                        <td style="padding:12px;">{r['phone']}</td>
                        <td style="padding:12px; font-family:monospace; color:#64748B;">{r['national_id']}</td>
                        <td style="padding:12px; font-size:11px;">{r['next_of_kin']}</td>
                        <td style="padding:12px;">
                            <span style="background:{badge_color}; color:white; padding:4px 10px; border-radius:20px; font-size:11px; font-weight:600;">
                                {label}
                            </span>
                        </td>
                        <td style="padding:12px; text-align:center;">
                            <span style="background:{brand_color}22; color:{brand_color}; padding:4px 10px; border-radius:6px; font-size:10px; font-weight:700; border:1px solid {brand_color}44;">
                                {str(r['status']).upper()}
                            </span>
                        </td>
                    </tr>"""

                st.markdown(f"""
                <div style='border:1px solid #E2E8F0; border-radius:12px; overflow:hidden; margin-top:10px; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1);'>
                    <table style='width:100%; border-collapse:collapse; font-family:Inter, sans-serif; font-size:13px;'>
                        <thead>
                            <tr style='background:{brand_color}; color:white; text-align:left;'>
                                <th style='padding:14px;'>borrower name</th>
                                <th style='padding:14px;'>Phone</th>
                                <th style='padding:14px;'>National ID</th>
                                <th style='padding:14px;'>Next of Kin</th>
                                <th style='padding:14px;'>Risk status</th>
                                <th style='padding:14px; text-align:center;'>status</th>
                            </tr>
                        </thead>
                        <tbody>{rows_html}</tbody>
                    </table>
                </div>""", unsafe_allow_html=True)

                st.write("")
                selected_name = st.selectbox(
                    "🎯 Management Actions:", 
                    options=["-- Select a borrower to edit/view profile --"] + filtered_df["name"].tolist()
                )
                if selected_name != "-- Select a borrower to edit/view profile --":
                    sel_id = filtered_df[filtered_df["name"] == selected_name]["id"].values[0]
                    st.session_state["selected_borrower"] = sel_id
                
            else:
                st.info("No records match your search criteria.")
        else:
            st.info("The registry is currently empty.")

    # ==============================
    # 👤 borrower PROFILE PANEL (EXPANDED)
    # ==============================
    selected_id = st.session_state.get("selected_borrower")

    if selected_id:
        st.write("---")
        st.markdown(f"### 👤 Profile Detail: {str(selected_id)[:8]}")

        borrower_query = borrowers_df[borrowers_df["id"].astype(str) == str(selected_id)]

        if not borrower_query.empty:
            borrower = borrower_query.iloc[0]

            with st.container(border=True):
                c1, c2 = st.columns(2)
                upd_name = c1.text_input("name", borrower["name"])
                upd_phone = c2.text_input("Phone", borrower["phone"])
                upd_email = c1.text_input("Email", borrower["email"])
                upd_nid = c2.text_input("National ID", borrower.get("national_id", ""))
                
                c3, c4 = st.columns(2)
                upd_nok = c3.text_input("Next of Kin", borrower.get("next_of_kin", ""))
                upd_addr = c4.text_input("Address", borrower.get("address", ""))

                # 📊 NESTED LOAN HISTORY
                st.markdown("#### 💳 Loan Statement")
                user_loans = loans_df[loans_df["borrower_id"].astype(str) == str(selected_id)].copy()

                if not user_loans.empty:
                    # Apply currency formatting with commas
                    st.dataframe(
                        user_loans, 
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "id": None, "tenant_id": None, "borrower_id": None, "borrower": None,
                            "principal": st.column_config.NumberColumn("principal", format="%d UGX"),
                            "interest": st.column_config.NumberColumn("interest", format="%d UGX"),
                            "balance": st.column_config.NumberColumn("balance", format="%d UGX"),
                            "total_repayable": st.column_config.NumberColumn("Total Due", format="%d UGX"),
                            "start_date": st.column_config.DateColumn("Date Issued"),
                            "end_date": st.column_config.DateColumn("Due Date"),
                        }
                    )
                    
                    csv = user_loans.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Statement (CSV)",
                        data=csv,
                        file_name=f"Statement_{upd_name.replace(' ', '_')}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("This borrower has no loan history.")

                # 🛠️ ACTION BUTTONS
                st.write("---")
                act_c1, act_c2, act_c3 = st.columns([1, 1, 2])

                if act_c1.button("💾 Save Changes", use_container_width=True):
                    # Logic to update row in DataFrame
                    idx = borrowers_df.index[borrowers_df["id"].astype(str) == str(selected_id)].tolist()[0]
                    borrowers_df.at[idx, "name"] = upd_name
                    borrowers_df.at[idx, "phone"] = upd_phone
                    borrowers_df.at[idx, "email"] = upd_email
                    borrowers_df.at[idx, "national_id"] = upd_nid
                    borrowers_df.at[idx, "next_of_kin"] = upd_nok
                    borrowers_df.at[idx, "address"] = upd_addr
                    
                    if save_data_saas("borrowers", borrowers_df):
                        st.success("Profile Updated Successfully")
                        st.cache_data.clear()
                        st.rerun()

                if act_c2.button("🗑️ Delete", use_container_width=True):
                    # Filter out the deleted user
                    updated_df = borrowers_df[borrowers_df["id"].astype(str) != str(selected_id)]
                    if save_data_saas("borrowers", updated_df):
                        st.warning("Profile Removed")
                        st.cache_data.clear()
                        st.session_state.pop("selected_borrower", None)
                        st.rerun()
                
                if act_c3.button("❌ Close Profile", use_container_width=True):
                    st.session_state.pop("selected_borrower", None)
                    st.rerun()



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


# ==============================                           
# 🛡️ 15. COLLATERAL MANAGEMENT PAGE (SAAS + ENTERPRISE UPGRADE)
# ==============================

def show_collateral_page():
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    # ==============================
    # 🔐 SAFETY CHECK
    # ==============================
    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    st.markdown(f"<h2 style='color: {brand_color};'>🛡️ Collateral & Security</h2>", unsafe_allow_html=True)

    # ==============================
    # 📦 FETCH DATA
    # ==============================
    collateral_df = get_data("collateral") 
    loans_df = get_data("loans")

    # ==============================
    # 🔍 FILTER ELIGIBLE LOANS
    # ==============================
    if loans_df is not None and not loans_df.empty:
        # Clean column names: remove spaces and make lowercase
        loans_df.columns = [str(c).strip().lower() for c in loans_df.columns]
        
        active_statuses = ["active", "overdue", "pending", "bcf"]
        
        # Safely filter by status if it exists
        if 'status' in loans_df.columns:
            available_loans = loans_df[loans_df["status"].str.lower().isin(active_statuses)].copy()
        else:
            available_loans = loans_df.copy()
    else:
        available_loans = pd.DataFrame()

    # --- TABS ---
    tab_reg, tab_view = st.tabs(["➕ Register Asset", "📋 Inventory & Status"])

    # ==============================
    # ➕ TAB 1: REGISTER ASSET
    # ==============================
    with tab_reg:
        if available_loans.empty:
            st.info("ℹ️ No Active or Overdue loans found to attach collateral to.")
        else:
            # 1. FIND THE BORROWER COLUMN DYNAMICALLY
            all_cols = [str(c).strip().lower() for c in loans_df.columns]
            borrower_col = None
            
            # Look for any column that sounds like 'borrower' or 'client'
            for col in loans_df.columns:
                c_clean = str(col).strip().lower()
                if c_clean in ['borrower', 'client', 'borrower_name', 'client_name']:
                    borrower_col = col
                    break
            
            # Fallback: if no name match, use the 3rd column (index 2) 
            # based on your previous table screenshots
            if borrower_col is None and len(loans_df.columns) >= 3:
                borrower_col = loans_df.columns[2]

            # 2. CREATE MASTER LOOKUP
            if borrower_col:
                name_lookup = dict(zip(loans_df['id'], loans_df[borrower_col]))
            else:
                name_lookup = {}

            with st.form("collateral_reg_form", clear_on_submit=True):
                st.write("### Link Asset to Loan")
                c1, c2 = st.columns(2)

                loan_map = {}
                for _, row in available_loans.iterrows():
                    loan_id = row['id']
                    
                    # Pull name using our dynamic column discovery
                    b_name = name_lookup.get(loan_id, "Unknown")
                    
                    # Double-check for 'nan' or empty strings
                    if str(b_name).lower() in ['nan', 'none', '']:
                        b_name = "Unknown"

                    ref = row.get('loan_id_label', 'N/A')
                    amt = f"UGX {row.get('principal', 0):,.0f}"
                    
                    clean_label = f"{b_name} | {amt} (Ref: {ref})"
                    loan_map[loan_id] = clean_label

                selected_loan_id = c1.selectbox(
                    "Select Loan/Borrower",
                    options=list(loan_map.keys()),
                    format_func=lambda x: loan_map.get(x, "Select Loan")
                )
                # ----------------------------

                asset_type = c2.selectbox(
                    "Asset Type",
                    ["Logbook (Car)", "Land Title", "Electronics", "House Deed", "Business Stock", "Other"]
                )

                desc = st.text_input("Detailed Asset Description (e.g. Plate No, Plot No)")
                est_value = st.number_input("Estimated Market Value (UGX)", min_value=0, step=100000)
                
                st.markdown("---")
                uploaded_photo = st.file_uploader("Upload Asset Photo (Verification)", type=["jpg", "png", "jpeg"])

                submit_save = st.form_submit_button("💾 Save & Secure Asset", use_container_width=True)

            if submit_save:
                if not desc or est_value <= 0:
                    st.error("❌ Please provide a description and valid market value.")
                else:
                    try:
                        # Extract just the Name for the record
                        full_label = loan_map[selected_loan_id]
                        borrower_for_db = full_label.split(" | ")[0]

                        new_asset = pd.DataFrame([{
                            "loan_id": selected_loan_id,
                            "tenant_id": str(current_tenant),
                            "borrower": borrower_for_db,
                            "type": asset_type,
                            "description": desc,
                            "value": float(est_value),
                            "status": "In Custody",
                            "date_added": datetime.now().strftime("%Y-%m-%d")
                        }])

                        if save_data_saas("collateral", new_asset):
                            st.success(f"✅ Asset secured for {borrower_for_db}!")
                            st.cache_data.clear()
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Save failed: {e}")

    # ==============================
    # 📋 TAB 2: INVENTORY & STATUS
    # ==============================
    with tab_view:
        if collateral_df is None or collateral_df.empty:
            st.info("💡 No assets currently in the registry.")
        else:
            # 📊 METRIC DASHBOARD
            collateral_df["value"] = pd.to_numeric(collateral_df["value"], errors="coerce").fillna(0)
            total_value = collateral_df["value"].sum()
            held_count = len(collateral_df[collateral_df["status"] == "In Custody"])
            
            m1, m2 = st.columns(2)
            m1.metric("Total Asset Value (Security)", f"UGX {total_value:,.0f}")
            m2.metric("Items in Custody", held_count)

            st.markdown("### Asset Ledger")
            
            display_df = collateral_df.copy()
            display_df["value_fmt"] = display_df["value"].apply(lambda x: f"{x:,.0f}")
            
            st.dataframe(
                display_df[["date_added", "borrower", "type", "description", "value_fmt", "status"]].rename(
                    columns={"value_fmt": "Value (UGX)", "date_added": "Date Registered"}
                ),
                use_container_width=True,
                hide_index=True
            )

            # ==============================
            # ⚙️ ASSET MANAGEMENT & PHOTO VIEW
            # ==============================
            st.markdown("---")
            with st.expander("🛠️ View Details & Manage Lifecycle", expanded=True):
                # Filter out released assets for the selection list
                manageable = collateral_df.copy()
                
                if manageable.empty:
                    st.write("No assets found.")
                else:
                    # 1. Select the Asset
                    asset_labels = manageable.apply(lambda x: f"{x['borrower']} - {x['description']}", axis=1).tolist()
                    asset_to_manage = st.selectbox("Select Asset to View/Update", options=asset_labels)
                    
                    # 2. Get the specific row for that asset
                    selected_row = manageable[manageable.apply(lambda x: f"{x['borrower']} - {x['description']}", axis=1) == asset_to_manage].iloc[0]
                    asset_id = selected_row["id"]

                    # --- PHOTO EVIDENCE SECTION ---
                    st.markdown("#### 📸 Photo Evidence")
                    
                    # Check if 'photo' or 'image' column exists in your DB
                    # If you saved the photo in the previous step, it's likely stored as a URL or Base64
                    asset_photo = selected_row.get('photo', selected_row.get('image_url', None))

                    if asset_photo:
                        st.image(asset_photo, caption=f"Current Photo of {selected_row['description']}", use_column_width=True)
                    else:
                        st.warning("⚠️ No photo was uploaded for this asset.")
                    
                    st.markdown("---")

                    # 3. Update Status Section
                    st.markdown("#### 🔄 Update Status")
                    col_stat, col_btn = st.columns([2,1])
                    new_stat = col_stat.selectbox(
                        "Set New Status", 
                        ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"],
                        index=["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"].index(selected_row['status']) if selected_row['status'] in ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"] else 0
                    )
                    
                    if col_btn.button("Update Status", use_container_width=True):
                        update_row = pd.DataFrame([{
                            "id": asset_id, 
                            "status": new_stat,
                            "tenant_id": str(current_tenant)
                        }])
                        
                        if save_data_saas("collateral", update_row):
                            st.success("✅ Asset status updated!")
                            st.cache_data.clear()
                            st.rerun()

# ==============================
# 📁 18. EXPENSE MANAGEMENT PAGE (SAAS + ENTERPRISE UPGRADE)
# ==============================

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

# ==============================
# 21. MASTER LEDGER (SAAS + ENTERPRISE)
# ==============================

def generate_pdf_statement(client_name, loans_df, payments_df):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)

    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph(f"<b>{st.session_state.get('company_name', 'ZOE CONSULTS').upper()}</b>", styles["Title"]))
    elements.append(Paragraph(f"Client: {client_name}", styles["Normal"]))
    elements.append(Paragraph(f"Statement Date: {datetime.now().strftime('%d %b %Y')}", styles["Normal"]))
    elements.append(Spacer(1, 20))

    grand_total = 0

    for _, loan in loans_df.iterrows():
        loan_id = str(loan["id"])
        # We use a readable label if possible, or truncate the long UUID for the header
        display_id = str(loan.get("loan_id_label", loan_id)) 
        principal = float(loan.get("principal", 0))
        interest = float(loan.get("interest", 0))
        initial_amount = principal + interest

        loan_payments = pd.DataFrame()
        if payments_df is not None and not payments_df.empty:
            loan_payments = payments_df[
                payments_df["loan_id"].astype(str) == loan_id
            ].copy()

        if not loan_payments.empty:
            date_col = "payment_date" if "payment_date" in loan_payments.columns else "date"
            if date_col in loan_payments.columns:
                loan_payments = loan_payments.sort_values(by=date_col)

        balance = initial_amount

        elements.append(Paragraph(f"<b>Loan Ref:</b> {display_id}", styles["Heading3"]))

        data = [["Date", "Description", "Debit", "Credit", "balance"]]

        # Truncate dates to YYYY-MM-DD to prevent overwriting
        start_date_raw = str(loan.get("created_at", loan.get("start_date", "")))
        clean_start_date = start_date_raw[:10] if len(start_date_raw) > 10 else start_date_raw

        data.append([
            clean_start_date,
            "Loan Disbursement",
            f"{initial_amount:,.0f}",
            "0",
            f"{balance:,.0f}"
        ])

        if not loan_payments.empty:
            for _, p in loan_payments.iterrows():
                amount = float(p.get("amount", 0))
                balance -= amount
                
                pay_date_raw = str(p.get("payment_date", p.get("date", "")))
                clean_pay_date = pay_date_raw[:10] if len(pay_date_raw) > 10 else pay_date_raw

                data.append([
                    clean_pay_date,
                    "Repayment",
                    "0",
                    f"{amount:,.0f}",
                    f"{balance:,.0f}"
                ])
        else:
            data.append(["-", "No payments", "-", "-", f"{balance:,.0f}"])

        grand_total += balance

        # Adjusted colWidths: widened the Description and balance columns
        table = Table(data, repeatRows=1, colWidths=[75, 170, 85, 85, 100])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.darkblue),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("FONTname", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9), # Slightly smaller font for better fit
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))

        elements.append(table)
        elements.append(Spacer(1, 20))

    elements.append(Paragraph(f"<b>Total Outstanding: {grand_total:,.0f} UGX</b>", styles["Title"]))

    doc.build(elements)
    buffer.seek(0)
    return buffer


# ==============================
# MAIN LEDGER FUNCTION (BABY BLUE EDITION)
# ==============================
def show_ledger_page():
    # 🎨 THEME COLORS & FONTS
    baby_blue = "#89CFF0"
    st.markdown(f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
            .ledger-header {{
                font-family: 'Inter', sans-serif;
                color: {baby_blue};
                font-weight: 700;
                letter-spacing: -0.5px;
            }}
            .snapshot-text {{
                font-family: 'Inter', sans-serif;
                font-weight: 600;
                color: #555;
            }}
        </style>
        <h2 class='ledger-header'>📘 Master Ledger</h2>
    """, unsafe_allow_html=True)

    # 📥 LOAD DATA
    loans_df = get_cached_data("loans")
    payments_df = get_cached_data("payments")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("💡 Your system is clear! No active loans found.")
        return

    # Normalize column names
    loans_df.columns = loans_df.columns.str.strip().str.lower().str.replace(" ", "_")
    if payments_df is not None and not payments_df.empty:
        payments_df.columns = payments_df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Map borrower names
    bor_map = {}
    if borrowers_df is not None and not borrowers_df.empty:
        borrowers_df.columns = borrowers_df.columns.str.strip().str.lower().str.replace(" ", "_")
        bor_map = dict(zip(borrowers_df["id"].astype(str), borrowers_df["name"]))

    if "borrower" not in loans_df.columns:
        loans_df["borrower"] = loans_df["borrower_id"].astype(str).map(bor_map).fillna("Unknown")

    # ==============================
    # 🎯 SELECTION INTERFACE
    # ==============================
    loan_map = {
        f"ID: {r.get('loan_id_label', r['id'])} - {r['borrower']}": str(r["id"])
        for _, r in loans_df.iterrows()
    }

    selected_label = st.selectbox("🎯 Select Loan Account", list(loan_map.keys()))
    raw_id = loan_map[selected_label]
    
    # Corrected data check to avoid nameError 'df'
    filtered_loan = loans_df[loans_df["id"].astype(str) == raw_id]
    if filtered_loan.empty:
        st.error("Loan data not found.")
        return
        
    loan_info = filtered_loan.iloc[0]

    # ==============================
    # 📊 STATEMENT PREVIEW (BABY BLUE SNAPSHOT)
    # ==============================
    st.markdown("<h4 class='snapshot-text'>📑 Account Snapshot</h4>", unsafe_allow_html=True)
    
    p = float(loan_info.get("principal", 0))
    i = float(loan_info.get("interest", 0))
    total_due = p + i
    paid = float(loan_info.get("amount_paid", 0))
    bal = float(loan_info.get("balance", 0))

    # Metric Cards with Styled Font
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("principal", f"UGX {p:,.0f}")
    m2.metric("Total interest", f"UGX {i:,.0f}")
    m3.metric("Total Paid", f"UGX {paid:,.0f}", delta=f"{paid/total_due:.1%}" if total_due > 0 else None)
    m4.metric("Current balance", f"UGX {bal:,.0f}", delta_color="inverse", delta=f"-{paid:,.0f}")

    # ==============================
    # 📜 TRANSACTION HISTORY (LEDGER)
    # ==============================
    ledger_data = []
    running_bal = p + i

    # Entry 1: Disbursement
    ledger_data.append({
        "Date": str(loan_info.get("start_date", "-"))[:10],
        "Description": "🏦 Loan Disbursement",
        "Debit (Due)": p,
        "Credit (Paid)": 0,
        "balance": running_bal
    })

    # Entry 2: interest Charge
    if i > 0:
        ledger_data.append({
            "Date": str(loan_info.get("start_date", "-"))[:10],
            "Description": "📈 Monthly interest Applied",
            "Debit (Due)": i,
            "Credit (Paid)": 0,
            "balance": running_bal
        })

    # Entry 3+: Repayments
    if payments_df is not None and not payments_df.empty:
        rel_payments = payments_df[payments_df["loan_id"].astype(str) == raw_id]
        if not rel_payments.empty:
            for _, p_row in rel_payments.iterrows():
                amt = float(p_row.get("amount", 0))
                running_bal -= amt
                ledger_data.append({
                    "Date": str(p_row.get("date", p_row.get("payment_date", "-")))[:10],
                    "Description": "💰 Repayment Received",
                    "Debit (Due)": 0,
                    "Credit (Paid)": amt,
                    "balance": running_bal
                })

    # Render Modern Styled Ledger
    st.markdown("<br>", unsafe_allow_html=True)
    st.dataframe(
        pd.DataFrame(ledger_data),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Date": st.column_config.TextColumn("Date"),
            "Description": st.column_config.TextColumn("Transaction Details"),
            "Debit (Due)": st.column_config.NumberColumn("Debit (UGX)", format="%,d"),
            "Credit (Paid)": st.column_config.NumberColumn("Credit (UGX)", format="%,d"),
            "balance": st.column_config.NumberColumn("Running balance (UGX)", format="%,d"),
        }
    )

    st.markdown("---")

    # ==============================
    # 📄 PREMIUM DOWNLOAD SECTION
    # ==============================
    # Container for Download Area with Baby Blue Border
    st.markdown(f"""
        <div style="border: 1px solid {baby_blue}55; padding: 1.5rem; border-radius: 12px; background-color: {baby_blue}10;">
            <p style="font-family: 'Inter', sans-serif; font-weight: 600; margin-bottom: 5px;">Ready to share this ledger?</p>
            <p style="font-family: 'Inter', sans-serif; font-size: 0.9rem; color: #666; margin-bottom: 15px;">
                The premium PDF statement includes full history, company letterhead, and a formal stamp section.
            </p>
        </div>
    """, unsafe_allow_html=True)

    if st.button("✨ Generate PDF Statement", use_container_width=True):
        client_name = loan_info.get("borrower", "Unknown")
        client_loans = loans_df[loans_df["borrower"] == client_name]

        with st.spinner("Compiling Ledger..."):
            pdf = generate_pdf_statement(client_name, client_loans, payments_df)

        st.download_button(
            label=f"⬇️ Download PDF for {client_name}",
            data=pdf,
            file_name=f"Statement_{client_name.replace(' ', '_')}.pdf",
            mime="application/pdf",
            use_container_width=True
        )


def show_settings_page():
    """
    Manages tenant identity and UI branding.
    Only displays when the 'Settings' page is selected.
    """

    # ==============================
    # 🔐 TENANT SAFETY LAYER (HARD GUARD)
    # ==============================
    tenant_id = st.session_state.get("tenant_id")

    if not tenant_id:
        st.warning("⚠️ No Active tenant detected. Please log in.")
        return

    # ==============================
    # 1. FETCH TENANT DATA (SAFE + HARDENED)
    # ==============================
    try:
        # Fetching the business profile specifically for this tenant
        tenant_resp = supabase.table("tenants").select("*").eq("id", tenant_id).execute()

        if not tenant_resp.data:
            st.error("❌ Business profile not found.")
            return

        active_company = tenant_resp.data[0]

    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        return

    # ==============================
    # BRANDING FALLBACK SAFETY
    # ==============================
    # Priority: Session State -> Database -> Default Navy
    brand_color = st.session_state.get(
        "theme_color", 
        active_company.get("brand_color", "#2B3F87")
    )

    st.markdown(
        f"<h2 style='color: {brand_color};'>⚙️ Portal Settings & Branding</h2>",
        unsafe_allow_html=True
    )

    # --- BUSINESS IDENTITY SECTION ---
    st.subheader("🏢 Business Identity")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown(f"**Current Business name:** {active_company.get('name', 'Unknown')}")

        new_color = st.color_picker(
            "🎨 Change Brand Color",
            active_company.get('brand_color', '#2B3F87'),
            key="settings_color_picker"
        )

        st.markdown("**Preview:**")
        st.markdown(
            f"""
            <div style='padding:15px; 
                        background-color:{new_color}; 
                        color:white; 
                        border-radius:10px; 
                        text-align:center; 
                        font-weight:bold;'>
                Brand Color Preview
            </div>
            """, 
            unsafe_allow_html=True
        )

    with col2:
        st.markdown("**Company Logo:**")
        
        logo_url = active_company.get("logo_url")

        # ==============================
        # LOGO DISPLAY SAFETY (CACHE BUSTING)
        # ==============================
        if logo_url:
            try:
                # Append timestamp to URL to force browser refresh on logo update
                logo_display_url = f"{logo_url}?t={int(time.time())}"
                st.image(logo_display_url, use_container_width=True, caption="Current Logo")
            except Exception:
                st.caption("⚠️ Logo could not be loaded.")
        else:
            st.caption("No logo uploaded yet.")

        logo_file = st.file_uploader("Upload New Logo (PNG/JPG)", type=["png", "jpg", "jpeg"])

    st.divider()

    # --- SAVE ACTION ---
    if st.button("💾 Save Branding Changes", use_container_width=True):
        
        updated_data = {"brand_color": new_color}

        # ==============================
        # LOGO UPLOAD SAFETY (STORAGE BUCKET)
        # ==============================
        if logo_file:
            try:
                bucket_name = "company-logos"
                # Use tenant ID in file path to ensure uniqueness and security
                file_path = f"logos/{active_company.get('id')}_logo.png"

                # Upload to Supabase Storage with upsert enabled
                supabase.storage.from_(bucket_name).upload(
                    path=file_path,
                    file=logo_file.getvalue(),
                    file_options={
                        "x-upsert": "true",
                        "content-type": "image/png"
                    }
                )

                # Generate public URL for database storage
                public_url = supabase.storage.from_(bucket_name).get_public_url(file_path)
                updated_data["logo_url"] = public_url

            except Exception as e:
                st.error(f"❌ Storage Error: {str(e)}")
                st.stop()

        # ==============================
        # DATABASE UPDATE (PERSISTENCE)
        # ==============================
        try:
            supabase.table("tenants").update(updated_data).eq("id", active_company.get("id")).execute()
            
            # Immediately update session state for real-time UI feel
            st.session_state["theme_color"] = new_color
            if "logo_url" in updated_data:
                st.session_state["logo_url"] = updated_data["logo_url"]

            st.success("✅ Branding updated successfully!")
            time.sleep(1)
            st.rerun()

        except Exception as e:
            st.error(f"❌ Database Error: {str(e)}")


# ==========================================
# FINAL APP ROUTER
# ==========================================
if __name__ == "__main__":
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if not st.session_state.get("logged_in"):
        # Your existing Auth UI call
        run_auth_ui(supabase) 
    else:
        try:
            # check_session_timeout() # Ensure this is defined or imported
            
            # Sidebar Navigation
            page = render_sidebar().strip()
            
            # 🗺️ NEW MODULAR ROUTER
            # We call the functions from the files in your /modules folder
            if page == "Overview":
                overview.show_overview_page()
                
            elif page == "loans":
                loans.show_loans_page()
                
            elif page == "borrowers":
                borrowers.show_borrowers_page()
                
            elif page == "Collateral":
                collateral.show_collateral_page()
                
            elif page == "Calendar":
                calendar.show_calendar_page()
                
            elif page == "Ledger":
                ledger.show_ledger_page()
                
            elif page == "Payments":
                payments.show_payments_page()
                
            elif page == "Expenses":
                expenses.show_expenses_page()
                
            elif page == "Petty Cash":
                petty_cash.show_petty_cash_page()
                
            elif page == "Overdue Tracker":
                overdue_tracker.show_overdue_tracker_page()
                
            elif page == "Payroll":
                payroll.show_payroll_page()
                
            elif page == "Reports":
                reports.show_reports_page()
                
            elif page == "Settings":
                settings.show_settings_page()
                
        except Exception as e:
            st.error(f"🚨 Application Error: {e}")
