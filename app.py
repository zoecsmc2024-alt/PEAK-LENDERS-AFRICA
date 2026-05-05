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
