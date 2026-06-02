from core import database as db
from services.payroll_engine import compute_payroll
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
from core.database import supabase
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from major.borrowers import show_borrowers
from major.loans import show_loans
from major.collateral import show_collateral
from major.overview import show_overview
from major.calendar import show_calendar
from major.expenses import show_expenses
from major.payments import show_payments
from major.ledger import show_ledger
from major.reports import show_reports
from major.payroll import show_payroll
from major.staff import show_staff
# --- Page Config stays here ---
st.set_page_config(
    page_title="Lending Manager Pro",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)
# Constants
SESSION_TIMEOUT = 30

# ==============================
# 🔄 SESSION & AUTH MANAGEMENT
# ==============================
if "auth_session" in st.session_state and supabase:
    try:
        supabase.auth.set_session(
            st.session_state["auth_session"].access_token,
            st.session_state["auth_session"].refresh_token
        )
        user = supabase.auth.get_user()
        if user and not st.session_state.get("logged_in"):
            st.session_state["logged_in"] = True
            st.session_state["authenticated"] = True
    except:
        pass

if "data_version" not in st.session_state:
    st.session_state["data_version"] = 0

def restore_session():
    session_data = cookie_manager.get("auth_session")
    if session_data:
        st.session_state["auth_session"] = session_data
        st.session_state["authenticated"] = True
        st.session_state["user_id"] = cookie_manager.get("user_id")
        st.session_state["tenant_id"] = cookie_manager.get("tenant_id")



# ==============================
# 🎨 1. THEME ENGINE (ENTERPRISE SAFE)
# ==============================
def apply_master_theme():

    brand_color = st.session_state.get("theme_color", "#1E3A8A")

    st.markdown(f"""
    <style>

    /* SELECTBOX FIX */
    div[data-baseweb="select"] > div {{
        background: rgba(255,255,255,0.9) !important;
        border-radius: 12px !important;
        border: none !important;
        font-weight: 500;
    }}

    /* 🔥 NAV TEXT VISIBILITY */
    div[role="radiogroup"] label {{
        color: rgba(255,255,255,0.95) !important;
        font-weight: 500 !important;
        opacity: 0.85;
        padding: 10px !important;
        border-radius: 10px;
        transition: 0.2s ease;
    }}

    /* 🔥 ICON + TEXT ROW */
    div[role="radiogroup"] label span {{
        color: rgba(255,255,255,0.95) !important;
    }}

    /* 🔥 ACTIVE ITEM */
    div[role="radiogroup"] input:checked + div {{
        opacity: 1 !important;
        color: #ffffff !important;
        background: rgba(255,255,255,0.18) !important;
        border-radius: 10px;
        box-shadow: 0 0 10px rgba(255,255,255,0.1);
    }}

    div[role="radiogroup"] label div {{
        color: white !important;
    }}

    /* HOVER */
    div[role="radiogroup"] label:hover {{
        background: rgba(255,255,255,0.08);
    }}

    /* SIDEBAR BACKGROUND */
    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {brand_color} 0%, #0F172A 100%) !important;
    }}

    /* REMOVE PADDING */
    [data-testid="stSidebar"] > div:first-child {{
        padding-top: 0rem;
    }}

    /* NAV TEXT */
    [data-testid="stSidebar"] .stRadio label {{
        color: white !important;
        font-size: 15px !important;
        font-weight: 500 !important;
    }}

    /* BUTTONS */
    [data-testid="stSidebar"] button {{
        background-color: white !important;
        color: {brand_color} !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
    }}

    /* LOGO */
    .logo-container img {{
        border-radius: 50%;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    }}

    </style>
    """, unsafe_allow_html=True)


import streamlit as st
import pandas as pd
import uuid
import datetime as dt_mod
from datetime import datetime, timedelta
from core.database import save_data_saas, get_cached_data

# Ensure supabase connection client context is available globally
from core.database import supabase 

# ============================================================
# 🔑 3. MULTI-TENANT SESSION CORE
# ============================================================
if "tenant_id" not in st.session_state:
    st.session_state.tenant_id = None
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "theme_color" not in st.session_state:
    # Slightly refined shade for enterprise look
    st.session_state.theme_color = "#1E3A8A" 

def get_tenant_id():
    """Safely retrieves the tenant ID from session state."""
    return st.session_state.get("tenant_id")

def require_tenant():
    """Stops execution if the user isn't assigned to a tenant (security check)."""
    if not st.session_state.get("tenant_id"):
        st.error("Session expired or unauthorized access. Please log in again.")
        st.stop()

# ============================================================
# 📤 4. STORAGE HELPERS (FIXED + SAFE)
# ============================================================
def generate_invite_token():
    import secrets
    return secrets.token_urlsafe(32)

def upload_image(file, bucket="collateral-photos"):
    """Uploads files to Supabase Storage organized by tenant_id."""
    try:
        if supabase is None:
            st.error("Storage unavailable: Supabase client not initialized.")
            return None

        require_tenant()
        tenant_id = get_tenant_id()

        # Sanitize filename: remove special characters for URL safety
        clean_name = re.sub(r'[^a-zA-Z0-9._-]', '_', file.name)
        file_path = f"{tenant_id}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{clean_name}"

        file_content = file.getvalue()
        content_type = file.type

        # Perform the upload
        supabase.storage.from_(bucket).upload(
            path=file_path,
            file=file_content,
            file_options={"content-type": content_type}
        )

        # Retrieve the public URL for display
        response = supabase.storage.from_(bucket).get_public_url(file_path)
        return response

    except Exception as e:
        st.error(f"Upload failed: {e}")
        return None

# ============================================================
# 📊 5. DATA LAYER (OPTIMIZED)
# ============================================================
def safe_series(df, col, default=0):
    """Safely converts a column to numeric, handling missing data."""
    if df is None or df.empty or col not in df.columns:
        return pd.Series([default] * len(df) if df is not None else [], dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)

# ============================================================
# 🚦 6. AUTHENTICATION ROUTER
# ============================================================
def run_auth_ui(supabase_client):
    """Determines which auth page to show."""
    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    view = st.session_state["view"]
    if view == "login":
        login_page(supabase_client)
    elif view == "signup":
        staff_signup_page(supabase_client)
    elif view == "create_company":
        admin_company_registration(supabase_client)

# ============================================================
# 🔌 4. SUPABASE INIT (ROBUST & UNIFIED)
# ============================================================
# We use the cached client from Part 2, but keep this fallback
# to ensure the app doesn't crash if secrets are missing.
SUPABASE_URL = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = st.secrets.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.warning("⚠️ Supabase credentials not configured. Please check your secrets.")
    SUPABASE_DISABLED = True
else:
    SUPABASE_DISABLED = False

# ============================================================
# 🔐 5. AUTHENTICATION LOGIC
# ============================================================
def authenticate(supabase_client, company_code, email, password):
    """Handles multi-step validation: Auth -> Profile -> Company Check."""
    try:
        # Step 1: Identity Authentication
        res = supabase_client.auth.sign_in_with_password({
            "email": email, 
            "password": password
        })

        if not res.user:
            return {"success": False, "error": "Invalid email or password"}

        # Step 2: Fetch Profile & Join with Tenants table
        # We fetch the tenant_id and company_code to ensure they belong to the right business
        profile = supabase_client.table("users")\
            .select("tenant_id, role, tenants(company_code, name)")\
            .eq("id", res.user.id)\
            .execute()

        if not profile.data:
            return {"success": False, "error": "User profile not found"}

        record = profile.data[0]
        tenant_info = record.get("tenants")

        if not tenant_info:
            return {"success": False, "error": "No business entity linked to this account"}

        # -----------------------------
        # Updated Security Validation Layer
        # -----------------------------
        # Clean up what the user typed in the login form
        input_company = company_code.strip().upper()
        
        # Clean up database values for comparison
        db_code = tenant_info["company_code"].strip().upper()
        db_name = tenant_info.get("name", "").strip().upper()
        
        # Allow access if what they typed matches EITHER the short code OR the full company name
        if input_company != db_code and input_company != db_name:
            return {"success": False, "error": "Incorrect Company Code or Name"}
        return {
            "success": True,
            "user_id": res.user.id,
            "tenant_id": record["tenant_id"],
            "role": record.get("role", "Staff"),
            "company": tenant_info.get("name")
        }

    except Exception as e:
        return {"success": False, "error": str(e)}

# ==============================
# 🔐 SAAS TENANT CONTEXT (UUID SAFE)
# ==============================

def get_current_tenant():
    """Returns tenant UUID only"""
    tenant_id = st.session_state.get("tenant_id", None)

    if tenant_id in [None, "", "default_tenant"]:
        return None

    return str(tenant_id)


# ==============================
# 🧠 DATABASE ADAPTER (MULTI-TENANT SAFE)
# ==============================
def get_data(table_name):
    tenant_id = str(get_current_tenant()).strip()
    
    # FIX: Pass tenant_id into the cached function so Streamlit tracks it as a cache key!
    df = get_cached_data(table_name, tenant_id)

    if df is None:
        return pd.DataFrame()

    if df.empty:
        return df

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    if "tenant_id" in df.columns:
        df["tenant_id"] = df["tenant_id"].astype(str).str.strip()
        
        # This local filtering acts as an excellent secondary security safety net
        df = df[df["tenant_id"] == tenant_id].copy()

    return df.reset_index(drop=True)

def save_data_saas_local(table_name, df):
    """Local fallback connector bypassing recursion loop names"""
    tenant_id = get_current_tenant()

    if tenant_id:
        df["tenant_id"] = str(tenant_id)

    return save_data_saas(table_name, df)


# ==============================
# 13. LOANS MANAGEMENT PAGE
# ==============================
def show_loans():
    # ==============================
    # 🎨 1. MASTER BUTTON STYLING (GLOBAL OVERRIDE)
    # ==============================
    st.markdown("""
    <style>
    /* MASTER BUTTON SELECTOR - Applies the premium look to ALL standard buttons */
    div.stButton > button,
    div.stFormSubmitButton > button {
        background: linear-gradient(135deg, #1d4ed8 0%, #1e3a8a 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.7rem 1.5rem !important;
        font-weight: 700 !important;
        font-size: 15px !important;
        height: 48px !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        gap: 8px !important;
        transition: all 0.25s ease-out !important;
        box-shadow: 0 4px 14 rgba(30, 58, 138, 0.2) !important;
        width: auto;
    }
    
    div.stButton > button[width="100%"] {
        width: 100% !important;
    }

    /* HOVER STATE (FLOAT EFFECT) */
    div.stButton > button:hover,
    div.stFormSubmitButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 25px rgba(30, 58, 138, 0.35) !important;
        background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%) !important;
    }
    
    /* ACTIVE/CLICK STATE */
    div.stButton > button:active,
    div.stFormSubmitButton > button:active {
        transform: translateY(1px) !important;
        box-shadow: 0 2px 8px rgba(30, 58, 138, 0.15) !important;
    }

    /* Specific icon adjustments */
    .save-icon { font-size: 1.1em; color: #a3e635; margin-right: 2px; }
    .cancel-icon { font-size: 1.1em; color: #f87171; margin-right: 2px; }
    .download-icon { font-size: 1.1em; color: #60a5fa; margin-right: 2px; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown(
        "<h2 style='color: #0A192F;'>💵 Loans Management</h2>",
        unsafe_allow_html=True
    )

    # ------------------------------
    # LOAD DATA
    # ------------------------------
    loans_df = get_data("loans")
    borrowers_df = get_data("borrowers")
    payments_df = get_data("payments")

    # ------------------------------
    # SAFETY FALLBACKS
    # ------------------------------
    if loans_df is None or loans_df.empty:
        loans_df = pd.DataFrame(columns=[
            "id", "sn", "loan_id_label", "parent_loan_id", "borrower_id",
            "borrower", "loan_type", "principal", "interest", "total_repayable",
            "amount_paid", "balance", "status", "start_date", "end_date",
            "cycle_no", "tenant_id"
        ])

    if borrowers_df is None:
        borrowers_df = pd.DataFrame()

    if payments_df is None:
        payments_df = pd.DataFrame()

    # ------------------------------
    # REQUIRED DEFAULTS
    # ------------------------------
    required_defaults = {
        "id": "", "sn": "", "loan_id_label": "", "parent_loan_id": "",
        "borrower_id": "", "borrower": "", "loan_type": "", "principal": 0.0,
        "interest": 0.0, "total_repayable": 0.0, "amount_paid": 0.0,
        "balance": 0.0, "status": "ACTIVE", "start_date": "", "end_date": "",
        "cycle_no": 1, "tenant_id": ""
    }

    for col, val in required_defaults.items():
        if col not in loans_df.columns:
            loans_df[col] = val

    loans_df = loans_df.copy()
    payments_df = payments_df.copy()

    # ------------------------------
    # TYPE CLEANUP
    # ------------------------------
    loans_df["id"] = loans_df["id"].astype(str)
    loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)
    loans_df["parent_loan_id"] = loans_df["parent_loan_id"].fillna("").astype(str)

    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].astype(str)

    # ------------------------------
    # NUMERIC CLEANUP
    # ------------------------------
    for col in ["principal", "interest", "total_repayable", "amount_paid", "balance"]:
        loans_df[col] = pd.to_numeric(loans_df[col], errors="coerce").fillna(0)

    if not payments_df.empty and "amount" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(payments_df["amount"], errors="coerce").fillna(0)

    # ------------------------------
    # DATE CLEANUP
    # ------------------------------
    for col in ["start_date", "end_date"]:
        loans_df[col] = pd.to_datetime(loans_df[col], errors="coerce")

    # ------------------------------
    # PAYMENT SYNC
    # ------------------------------
    loans_df["amount_paid"] = 0

    if not payments_df.empty and "loan_id" in payments_df.columns:
        pay_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(pay_sums).fillna(0)

    loans_df["balance"] = (loans_df["total_repayable"] - loans_df["amount_paid"]).clip(lower=0)

    # ==============================
    # SERIAL ENGINE
    # ==============================
    existing_nums = []
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip()
    existing_sn_map = dict(zip(loans_df["id"], loans_df["sn"]))
    
    for val in loans_df["sn"]:
        val = val.strip()
        if val.startswith("LN-"):
            try:
                existing_nums.append(int(val.replace("LN-", "")))
            except:
                pass
    
    next_sn_val = max(existing_nums, default=0)
    
    for i in loans_df.index:
        current_id = loans_df.at[i, "id"]
        existing_sn = str(existing_sn_map.get(current_id, "")).strip()
        if existing_sn.startswith("LN-"):
            continue
    
        parent_id = str(loans_df.at[i, "parent_loan_id"]).strip()
        inherited_sn = ""
    
        while parent_id != "":
            parent_match = loans_df[loans_df["id"] == parent_id]
            if parent_match.empty:
                break
    
            parent_row = parent_match.iloc[0]
            parent_sn = str(parent_row["sn"]).strip()
    
            if parent_sn.startswith("LN-"):
                inherited_sn = parent_sn
                break
    
            parent_id = str(parent_row["parent_loan_id"]).strip()
    
        if inherited_sn:
            loans_df.at[i, "sn"] = inherited_sn
    
        if not str(loans_df.at[i, "sn"]).startswith("LN-"):
            next_sn_val += 1
            loans_df.at[i, "sn"] = f"LN-{next_sn_val:04d}"
    
    loans_df = loans_df.sort_values(by=["sn", "start_date", "id"])
    loans_df["cycle_no"] = loans_df.groupby("sn").cumcount() + 1
    
    # ------------------------------
    # REVISED SMART STATUS LOGIC (V2)
    # ------------------------------
    partition_tenant_id = get_current_tenant()
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip().str.upper()
    loans_df = loans_df.sort_values(by=["sn", "cycle_no", "start_date"])
    
    for sn_val, grp in loans_df.groupby("sn"):
        indices = grp.index.tolist()
        if len(indices) > 1:
            loans_df.loc[indices[:-1], "status"] = "BCF"
        
        latest_idx = indices[-1]
        latest_row = loans_df.loc[latest_idx]
        
        if abs(latest_row["balance"]) < 1.0: 
            loans_df.at[latest_idx, "status"] = "CLEARED"
        else:
            if int(latest_row["cycle_no"]) == 1:
                loans_df.at[latest_idx, "status"] = "ACTIVE"
            else:
                loans_df.at[latest_idx, "status"] = "PENDING"
    
    mask_zero = (loans_df["balance"] <= 0) & (loans_df["status"] != "BCF")
    loans_df.loc[mask_zero, "status"] = "CLEARED"
    loans_df.loc[loans_df["balance"] <= 0, "status"] = "CLEARED"
    
    loans_df = loans_df.sort_values(by=["sn", "cycle_no"], ascending=[True, True]).reset_index(drop=True)
    loans_df["loan_id_label"] = loans_df["sn"].str.replace("LN-", "", regex=False).str.zfill(4)

    # ==============================
    # 🔄 DATABASE SYNC ENGINE (OPTIMIZED)
    # ==============================
    # FIX: Pass partition_tenant_id here to prevent multi-tenant cache pollution leaks!
    raw_db_df = get_cached_data("loans", partition_tenant_id)
    
    def needs_update(row):
        if raw_db_df is None or raw_db_df.empty:
            return True
        db_match = raw_db_df[raw_db_df["id"] == row["id"]]
        if db_match.empty:
            return True
        db_row = db_match.iloc[0]
        return (str(db_row.get("sn", "")) != str(row["sn"]) or 
                str(db_row.get("loan_id_label", "")) != str(row["loan_id_label"]) or
                int(db_row.get("cycle_no", 0)) != int(row["cycle_no"]))

    to_sync = loans_df[loans_df.apply(needs_update, axis=1)]
    
    if not to_sync.empty:
        with st.status("🔄 Syncing Serial Numbers to Database...", expanded=False) as status:
            for _, row in to_sync.iterrows():
                sync_data = {
                    "sn": row["sn"],
                    "loan_id_label": row["loan_id_label"],
                    "cycle_no": int(row["cycle_no"])
                }
                try:
                    # FIX: Enforce tenancy filtering directly inside the backend query engine update phase
                    supabase.table("loans").update(sync_data).eq("id", row["id"]).eq("tenant_id", partition_tenant_id).execute()
                except Exception as e:
                    st.error(f"Error syncing row {row['id']}: {e}")
            
            st.cache_data.clear()
            status.update(label="✅ Database Serial Numbers Synced!", state="complete", expanded=False)
            st.rerun()

    # ------------------------------
    # BORROWER MAP
    # ------------------------------
    if not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        mapped_names = loans_df["borrower_id"].map(bor_map)
        loans_df["borrower"] = mapped_names.fillna(loans_df["borrower"]).fillna("Unknown")

    if not borrowers_df.empty and "status" in borrowers_df.columns:
        Active_borrowers = borrowers_df[borrowers_df["status"].astype(str).str.upper() == "ACTIVE"]
    else:
        Active_borrowers = pd.DataFrame(columns=["id", "name"])

    # ==============================
    # TABS INTERFACE
    # ==============================
    tab_view, tab_add, tab_manage, tab_actions = st.tabs([
        "📑 Portfolio View", "➕ New Loan", "🛠️ Manage/Edit", "⚙️ Actions"
    ])

    # ==============================
    # TAB VIEW
    # ==============================
    with tab_view:
        search_query = st.text_input("🔍 Search Loan / borrower", key="loan_search_main")
        filtered_loans = loans_df.copy() if not loans_df.empty else pd.DataFrame()

        if not filtered_loans.empty and search_query:
            filtered_loans = filtered_loans[
                filtered_loans.apply(lambda r: search_query.lower() in str(r).lower(), axis=1)
            ]

        if not filtered_loans.empty:
            total_loans = filtered_loans["sn"].nunique()
            original_loans = filtered_loans[filtered_loans["cycle_no"] == 1]  
            total_principal = original_loans["principal"].sum()
            total_repayable = filtered_loans["total_repayable"].sum()
            total_paid = filtered_loans["amount_paid"].sum()
            total_pending = filtered_loans[filtered_loans["status"] == "PENDING"]["total_repayable"].sum()

            col1, col2, col3, col4 = st.columns(4)
            col1.markdown(f'<div style="background: linear-gradient(135deg, #3b82f6, #1e3a8a); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">📄 Total Loans</div><div style="font-size:22px;font-weight:bold;">{total_loans}</div></div>', unsafe_allow_html=True)
            col2.markdown(f'<div style="background: linear-gradient(135deg, #10b981, #065f46); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">💰 Principal</div><div style="font-size:22px;font-weight:bold;">{total_principal:,.0f}</div></div>', unsafe_allow_html=True)
            col3.markdown(f'<div style="background: linear-gradient(135deg, #f59e0b, #92400e); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">💳 Paid</div><div style="font-size:22px;font-weight:bold;">{total_paid:,.0f}</div></div>', unsafe_allow_html=True)
            col4.markdown(f'<div style="background: linear-gradient(135deg, #ef4444, #991b1b); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">⏳ Total Pending</div><div style="font-size:22px;font-weight:bold;">{total_pending:,.0f}</div></div>', unsafe_allow_html=True)

            st.markdown("---")

            if "fiscal_year" not in filtered_loans.columns:
                start_dt = pd.to_datetime(filtered_loans.get("start_date"), errors="coerce")
                start_dt = start_dt.fillna(pd.to_datetime(filtered_loans.get("created_at", pd.Timestamp.today())))
                fiscal_years_list = [f"{dt.year}/{dt.year + 1}" if dt.month >= 7 else f"{dt.year - 1}/{dt.year}" for dt in start_dt]
                filtered_loans["fiscal_year"] = fiscal_years_list
            
            fy_unique = sorted(filtered_loans["fiscal_year"].dropna().unique().tolist())
            fy_selected = st.selectbox("Filter by Fiscal Year", ["All"] + fy_unique)
            
            if fy_selected != "All":
                filtered_loans = filtered_loans[filtered_loans["fiscal_year"] == fy_selected]
        
            filtered_loans["balance"] = filtered_loans["total_repayable"] - filtered_loans["amount_paid"]
        
            show_cols = ["sn", "loan_id_label", "borrower", "cycle_no", "principal", "total_repayable", "amount_paid", "balance", "start_date", "end_date", "status"]
            
            def style_entire_row(row):
                val = str(row["status"]).upper().strip()
                color_map = {
                    "ACTIVE": "background-color:#dbeafe;color:#1e40af;font-weight:bold;",
                    "PENDING": "background-color:#fee2e2;color:#991b1b;font-weight:bold;",
                    "CLEARED": "background-color:#d1fae5;color:#065f46;",
                    "BCF": "background-color:#ffedd5;color:#9a3412;",
                    "CLOSED": "background-color:#f3f4f6;color:#374151;"
                }
                return [color_map.get(val, "")] * len(row)
        
            styled_df = (filtered_loans[show_cols].style.apply(style_entire_row, axis=1).format({
                "principal": "{:,.0f}", "amount_paid": "{:,.0f}", "total_repayable": "{:,.0f}", "balance": "{:,.0f}"
            }))
        
            st.dataframe(styled_df, column_order=show_cols, use_container_width=True, hide_index=True)

    # ==============================         
    # TAB ADD LOAN
    # ==============================
    with tab_add:
        if Active_borrowers.empty:
            st.info("💡 Tip: Activate borrower first.")
        else:
            with st.form("loan_issue_form_v2"):
                st.markdown("<h4 style='color:#0A192F;'>📝 Create New Loan Agreement</h4>", unsafe_allow_html=True)
                col1, col2 = st.columns(2)

                borrower_map = dict(zip(Active_borrowers["name"], Active_borrowers["id"]))
                selected_name = col1.selectbox("Select borrower", list(borrower_map.keys()))
                selected_id = str(borrower_map[selected_name]).strip()

                amount = col1.number_input("Principal amount (UGX)", min_value=0, step=50000)
                date_issued = col1.date_input("Start date", value=datetime.now())

                loan_type = col2.selectbox("Loan type", ["Business", "Personal", "Emergency", "Other"])
                interest_rate = col2.number_input("Monthly Interest Rate (%)", min_value=0.0, step=0.5)
                date_due = col2.date_input("Due date", value=date_issued + timedelta(days=30))

                total_due = amount + (amount * interest_rate / 100)
                st.info(f"Preview: Total Repayable {total_due:,.0f} UGX")

                submit = st.form_submit_button("🚀 Confirm & Issue Loan")

                if submit:
                    tenant_id = get_current_tenant()
                    if not tenant_id:
                        st.error("Tenant session missing.")
                        st.stop()
                    if selected_id == "":
                        st.error("borrower ID missing.")
                        st.stop()

                    loan_data = {
                        "id": str(uuid.uuid4()), "sn": "", "loan_id_label": "", "parent_loan_id": None,
                        "borrower_id": selected_id, "borrower": selected_name, "loan_type": loan_type,
                        "principal": float(amount), "interest": float(amount * interest_rate / 100),
                        "total_repayable": float(total_due), "amount_paid": 0.0, "balance": float(total_due),
                        "status": "ACTIVE", "start_date": str(date_issued), "end_date": str(date_due),
                        "cycle_no": 1, "tenant_id": tenant_id
                    }

                    new_loan_df = pd.DataFrame([loan_data])
                    if save_data_saas("loans", new_loan_df):
                        st.success("🎉 Loan Agreement Issued Successfully!")
                        st.cache_data.clear()
                        st.rerun()

    # ==============================
    # TAB ACTIONS
    # ==============================
    with tab_actions:
        st.markdown("<h4 style='color: #0A192F;'>🔄 Multi-Stage Loan Rollover</h4>", unsafe_allow_html=True)
        eligible_loans = loans_df[(~loans_df["status"].isin(["CLEARED"])) & (loans_df["balance"] > 0)]
    
        if eligible_loans.empty:
            st.success("All loans brought up to date! ✨")
        else:
            roll_map = {f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']} • Bal {row['balance']:,.0f}": row["id"] for _, row in eligible_loans.iterrows()}
            roll_sel = st.selectbox("Select Loan to Roll Forward", list(roll_map.keys()))
            parent_id = roll_map[roll_sel]
            loan_to_roll = eligible_loans[eligible_loans["id"] == parent_id].iloc[0]
    
            new_interest_rate = st.number_input("New Monthly Interest (%)", value=3.0, step=0.5)
    
            if st.button("🔥 Execute Next Rollover", use_container_width=True):
                old_due = pd.to_datetime(loan_to_roll["end_date"], errors="coerce")
                if pd.isna(old_due):
                    old_due = datetime.now()
    
                new_start = old_due
                new_due = old_due + timedelta(days=30)
                current_status = str(loan_to_roll["status"]).strip().upper()
    
                if current_status == "PENDING":
                    loans_df.loc[loans_df["id"] == parent_id, "status"] = "BCF"
    
                save_data_saas("loans", loans_df)
    
                unpaid = float(loan_to_roll["balance"])
                new_interest = unpaid * (new_interest_rate / 100)
    
                new_row = {
                    "id": str(uuid.uuid4()), "sn": "", "loan_id_label": "", "parent_loan_id": parent_id,
                    "borrower_id": loan_to_roll["borrower_id"], "loan_type": loan_to_roll["loan_type"],
                    "principal": unpaid, "interest": new_interest, "total_repayable": unpaid + new_interest,
                    "amount_paid": 0.0, "balance": unpaid + new_interest, "status": "PENDING",
                    "start_date": str(new_start.date()), "end_date": str(new_due.date()),
                    "cycle_no": int(loan_to_roll["cycle_no"]) + 1, "tenant_id": get_current_tenant()
                }
    
                if save_data_saas("loans", pd.DataFrame([new_row])):
                    st.success("✅ Loan rolled forward.")
                    st.cache_data.clear()
                    st.rerun()

    # ==============================
    # TAB MANAGE (FIXED & SECURE)
    # ==============================
    with tab_manage:

        if not loans_df.empty:

            edit_map = {
                f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']}": row["id"]
                for _, row in loans_df.iterrows()
            }

            selected = st.selectbox(
                "Select Loan to Edit",
                list(edit_map.keys())
            )

            target_id = edit_map[selected]

            loan_match = loans_df[
                loans_df["id"] == target_id
            ]

            if loan_match.empty:
                st.error("Loan not found.")
                st.stop()

            loan_to_edit = loan_match.iloc[0]

            # =====================================
            # FORM
            # =====================================
            with st.form(key=f"edit_form_container_{target_id}"):

                e_princ = st.number_input(
                    "Principal",
                    value=float(loan_to_edit["principal"])
                )

                raw_date = loan_to_edit.get("start_date")

                if isinstance(raw_date, str) and raw_date != "":
                    default_date = datetime.strptime(
                        raw_date[:10],
                        "%Y-%m-%d"
                    ).date()

                elif hasattr(raw_date, "date"):
                    default_date = raw_date.date()

                elif hasattr(raw_date, "strftime"):
                    default_date = raw_date

                else:
                    default_date = dt_mod.date.today()

                e_date_val = st.date_input(
                    "Date",
                    value=default_date
                )

                e_interest_rate = st.number_input(
                    "Interest Rate (%)",
                    value=float(
                        loan_to_edit.get(
                            "interest_rate",
                            loan_to_edit.get("interest", 0.0)
                        )
                    ),
                    step=0.01
                )

                e_type = st.text_input(
                    "Loan Type",
                    value=str(
                        loan_to_edit.get("loan_type", "")
                    )
                )

                status_options = [
                    "ACTIVE",
                    "PENDING",
                    "CLEARED",
                    "BCF",
                    "CLOSED"
                ]

                current_stat = str(
                    loan_to_edit["status"]
                ).upper().strip()

                idx = (
                    status_options.index(current_stat)
                    if current_stat in status_options
                    else 0
                )

                e_stat = st.selectbox(
                    "Status",
                    status_options,
                    index=idx
                )

                save_changes = st.form_submit_button(
                    "💾 Save Changes"
                )

            # =====================================
            # SAVE LOGIC
            # =====================================
            if save_changes:

                formatted_start_date = e_date_val.strftime("%Y-%m-%d")
                
                calculated_end_date = (
                    dt_mod.datetime.combine(e_date_val, dt_mod.time.min) + timedelta(days=30)
                ).date().strftime("%Y-%m-%d")

                updated_row = {
                    "id": target_id,
                    "sn": loan_to_edit["sn"],
                    "loan_id_label": loan_to_edit["loan_id_label"],
                    "parent_loan_id": (
                        loan_to_edit["parent_loan_id"]
                        if pd.notna(loan_to_edit["parent_loan_id"]) and loan_to_edit["parent_loan_id"] != ""
                        else None
                    ),
                    "borrower_id": loan_to_edit["borrower_id"],
                    "borrower": loan_to_edit["borrower"],
                    "loan_type": e_type,
                    "principal": float(e_princ),
                    "interest": float(e_princ * e_interest_rate / 100),
                    "total_repayable": float(e_princ + (e_princ * e_interest_rate / 100)),
                    "amount_paid": float(loan_to_edit["amount_paid"]),
                    "balance": float((e_princ + (e_princ * e_interest_rate / 100)) - loan_to_edit["amount_paid"]),
                    "status": e_stat,
                    "start_date": formatted_start_date,
                    "end_date": calculated_end_date,
                    "cycle_no": int(loan_to_edit["cycle_no"]),
                    "tenant_id": get_current_tenant()
                }

                payload_df = pd.DataFrame([updated_row])
                success = save_data_saas("loans", payload_df)

                if success:
                    st.toast("🎉 Loan configurations updated cleanly!", icon="✅")
                    st.rerun()
        
            # =====================================
            # DELETE BUTTON
            # =====================================
            if st.button(
                "🗑️ Delete Loan Permanently",
                use_container_width=True,
                key=f"delete_{target_id}"
            ):
                tenant_id = get_current_tenant()
                # FIX: Explicitly bundle tenant_id within match logic to safeguard destructive queries
                supabase.table("loans").delete().match({
                    "id": target_id,
                    "tenant_id": tenant_id
                }).execute()
    
                st.warning("Loan Deleted.")
                st.cache_data.clear()
                st.rerun()

import streamlit as st
import uuid
import time
from datetime import datetime, timedelta

# =========================================
# 🔒 CONFIGURATION
# =========================================
SESSION_TIMEOUT = 15  # minutes
MAX_ATTEMPTS = 5
LOCKOUT_MINUTES = 10

# =========================================
# 🏢 SESSION MANAGEMENT
# =========================================
def create_session(user_data, remember_me=False):
    """Initialize session after login."""
    st.session_state.update({
        "logged_in": True,
        "authenticated": True,
        "user_id": user_data["user_id"],
        "tenant_id": user_data["tenant_id"],
        "role": user_data["role"],
        "company": user_data["company"],
        "last_activity": datetime.now(),
        "current_view": "dashboard"
    })
    if remember_me:
        st.session_state["remember"] = True

    st.success(f"Welcome back to {user_data['company']}!")
    time.sleep(0.5)
    st.rerun()


def check_session_timeout():
    """Logout after inactivity."""
    if not st.session_state.get("logged_in"):
        return

    last_activity = st.session_state.get("last_activity", datetime.now())
    idle_time = (datetime.now() - last_activity).total_seconds() / 60

    if idle_time > SESSION_TIMEOUT:
        # Clear sensitive session keys
        auth_keys = ["logged_in", "authenticated", "user_id", "tenant_id", "company", "auth_session"]
        for key in auth_keys:
            st.session_state.pop(key, None)
        st.warning("Session timed out due to inactivity. Redirecting to login...")
        st.session_state["view"] = "login"
        st.rerun()

    # Update last activity
    st.session_state["last_activity"] = datetime.now()

# =========================================
# ⚡ RATE LIMITING / BRUTE FORCE
# =========================================
def check_rate_limit(email):
    """Check if user is locked out due to repeated failed login."""
    attempts = st.session_state.setdefault("login_attempts", {})
    if email in attempts:
        count, last_time = attempts[email]
        if count >= MAX_ATTEMPTS and (datetime.now() - last_time) < timedelta(minutes=LOCKOUT_MINUTES):
            return False
    return True

def record_failed_attempt(email):
    """Record failed login attempt with timestamp."""
    attempts = st.session_state.setdefault("login_attempts", {})
    count, _ = attempts.get(email, (0, datetime.now()))
    attempts[email] = (count + 1, datetime.now())

# =========================================
# 🏢 TENANT UTILS
# =========================================
def tenant_filter(df):
    """Filter dataframe by tenant."""
    if df is None or df.empty:
        return pd.DataFrame()
    if "tenant_id" not in df.columns:
        return df
    return df[df["tenant_id"] == st.session_state.get("tenant_id")].copy()

# =========================================
# 🌟 BEAUTIFUL MODERN AUTH UI (FIXED)
# =========================================

import streamlit as st
import uuid
from datetime import datetime


# =========================================
# 🛡️ SAFE AUDIT LOGGER
# =========================================
def safe_audit_log(supabase, payload):
    """
    Prevent audit logging from crashing auth
    if audit_logs table doesn't exist.
    """
    try:
        supabase.table("audit_logs").insert(payload).execute()
    except Exception:
        pass


# =========================================
# 🎨 GLOBAL STYLES
# =========================================

import streamlit as st
import uuid

def auth_styles():
    st.markdown("""
    <style>

    /* Background */
    .stApp {
        background: linear-gradient(135deg, #F0F4F8, #E2E8F0);
    }

    /* Auth Card */
    .auth-card {
        padding: 2.5rem;
        border-radius: 20px;
        background: #FFFFFF;
        box-shadow: 0 15px 35px rgba(0,0,0,0.05);
        border: 1px solid #E2E8F0;
        margin-top: 2rem;
    }

    /* Top Header Bar */
    .portal-badge-header {
        background-color: #1A252F;
        padding: 10px 14px;
        border-radius: 8px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1.5rem;
        color: #fff;
    }

    .badge-left {
        display: flex;
        gap: 12px;
        font-size: 12px;
    }

    /* Inputs */
    .stTextInput input {
        border-radius: 10px !important;
        border: 1px solid #CBD5E1 !important;
    }

    /* Primary button */
    .stFormSubmitButton button {
        background: linear-gradient(90deg, #1E3A8A, #2563EB) !important;
        color: white !important;
        border-radius: 10px !important;
        height: 44px;
        width: 100%;
        font-weight: 600;
        border: none !important;
    }

    /* Clean Secondary Action Buttons Style */
    div[data-testid="column"] .stButton > button {
        background-color: #F8FAFC !important;
        color: #475569 !important;
        border: 1px solid #E2E8F0 !important;
        border-radius: 8px !important;
        font-size: 13px !important;
        height: 38px !important;
        font-weight: 500;
        transition: all 0.2s ease;
    }
    
    div[data-testid="column"] .stButton > button:hover {
        background-color: #F1F5F9 !important;
        border-color: #CBD5E1 !important;
        color: #1E3A8A !important;
    }

    </style>
    """, unsafe_allow_html=True)


# =========================================
# 🏢 REGISTER COMPANY
# =========================================
@st.dialog("🏢 Register Organization")
def admin_company_registration(supabase):
    with st.form("company_form"):
        company = st.text_input("Company Name")
        admin = st.text_input("Admin Name")
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")

        if st.form_submit_button("Create Company"):
            if not all([company, admin, email, password]):
                st.error("All fields required")
                return

            try:
                user = supabase.auth.sign_up({"email": email, "password": password})

                tenant_id = str(uuid.uuid4())
                code = f"{company[:3].upper()}{uuid.uuid4().int % 999}"

                supabase.table("tenants").insert({
                    "id": tenant_id,
                    "name": company,
                    "company_code": code
                }).execute()

                supabase.table("users").insert({
                    "id": user.user.id,
                    "name": admin,
                    "email": email,
                    "tenant_id": tenant_id,
                    "role": "Admin"
                }).execute()

                st.success(f"Company created: {code}")

            except Exception as e:
                st.error(e)


# =========================================
# 👥 STAFF SIGNUP
# =========================================
@st.dialog("👥 Staff Signup")
def view_staff_signup(supabase):
    with st.form("staff_form"):
        company = st.text_input("Company Name")
        name = st.text_input("Full Name")
        email = st.text_input("Email")
        password = st.text_input("Password", type="password")

        if st.form_submit_button("Submit"):
            try:
                tenant = supabase.table("tenants").select("*").ilike("name", company).execute()

                if not tenant.data:
                    st.error("Company not found")
                    return

                user = supabase.auth.sign_up({"email": email, "password": password})

                supabase.table("users").insert({
                    "id": user.user.id,
                    "name": name,
                    "email": email,
                    "tenant_id": tenant.data[0]["id"],
                    "role": "Staff"
                }).execute()

                st.success("Staff created")

            except Exception as e:
                st.error(e)


# =========================================
# 🔑 RESET PASSWORD
# =========================================
@st.dialog("🔑 Reset Password")
def forgot_password_page(supabase):
    email = st.text_input("Email")
    if st.button("Send Reset Link", use_container_width=True):
        try:
            supabase.auth.reset_password_for_email(email)
            st.success("Reset link sent")
        except Exception as e:
            st.error(e)


# =========================================
# 🔐 SECURE LOGIN PAGE (INTEGRATED)
# =========================================
def login_page(supabase_client):
    auth_styles()

    _, center, _ = st.columns([1, 1.3, 1])

    with center:
        st.markdown('<div class="auth-card">', unsafe_allow_html=True)

        st.markdown("""
        <div class="portal-badge-header">
            <div class="badge-left">
                <span style="color:#3498DB; font-weight: 600;">🏢 Peak-Lenders Africa</span>
                <span>|</span>
                <span style="color:#2ECC71;">🔒 Secure Login</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<h2 style='color:#1E3A8A; margin-top:0;'>Login</h2>", unsafe_allow_html=True)

        with st.form("login_form"):
            company = st.text_input("Company Code / Name")
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")

            submit = st.form_submit_button("Login")

        # Divider line between form and footer utility buttons
        st.markdown("<div style='margin-top: 20px; border-top: 1px solid #F1F5F9; padding-top: 15px;'></div>", unsafe_allow_html=True)

        # Actions Row — Nested safely inside the card block layout context
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("New Company", use_container_width=True):
                admin_company_registration(supabase_client)

        with col2:
            if st.button("Staff Signup", use_container_width=True):
                view_staff_signup(supabase_client)

        with col3:
            if st.button("Reset Password", use_container_width=True):
                forgot_password_page(supabase_client)

        st.markdown("</div>", unsafe_allow_html=True)

    # login execution logic pipeline
    if submit:
        if not all([company, email, password]):
            st.error("Please fill out all login fields.")
            return

        # ⚡ Check Brute Force protection
        if not check_rate_limit(email):
            st.error(f"Too many failed attempts. Locked out for {LOCKOUT_MINUTES} minutes.")
            return

        with st.spinner("Authenticating secure session..."):
            # 🔥 CRITICAL FIX: Direct submission to your robust multi-tenant auth layer
            auth_result = authenticate(supabase_client, company, email, password)

        if auth_result["success"]:
            # Reset rate limit metrics on success
            if "login_attempts" in st.session_state and email in st.session_state["login_attempts"]:
                del st.session_state["login_attempts"][email]
            
            # Securely log operational audit events
            safe_audit_log(supabase_client, {
                "user_id": auth_result["user_id"],
                "tenant_id": auth_result["tenant_id"],
                "action": "LOGIN_SUCCESS",
                "timestamp": datetime.now().isoformat()
            })

            # 🔥 CRITICAL FIX: Establish global session states & update tenant verification tokens
            create_session(auth_result, remember_me=False)
        else:
            # Register failures for lockouts
            record_failed_attempt(email)
            st.error(f"❌ Verification Failed: {auth_result['error']}")

def run_auth_ui(supabase):
    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    view = st.session_state["view"]

    if view == "login":
        login_page(supabase)
    elif view == "signup":
        view_staff_signup(supabase)
    elif view == "create_company":
        admin_company_registration(supabase)
    elif view == "reset":
        forgot_password_page(supabase)
    elif view == "main":
        st.empty()

# ============================================================
# ⚡ ENTERPRISE SIDEBAR (FAST + CLEAN + SAFE)
# ============================================================

import streamlit as st
import time
import pandas as pd


# ============================================================
# 🧠 CACHE: TENANTS (CRITICAL SPEED FIX)
# ============================================================
@st.cache_data(ttl=600, show_spinner=False)
def get_tenants():
    try:
        res = supabase.table("tenants")\
            .select("id, name, brand_color, logo_url")\
            .execute()
        return res.data or []
    except:
        return []


# ============================================================
# 🖼️ CACHE: LOGO BUILDER
# ============================================================
@st.cache_data(ttl=600)
def build_logo_url(logo_val):
    if not logo_val:
        return None

    if str(logo_val).startswith("http"):
        return logo_val

    base = st.secrets.get("SUPABASE_URL", "").strip("/")
    if not base:
        return None

    return f"{base}/storage/v1/object/public/company-logos/{logo_val}"


# ============================================================
# 🚦 SIDEBAR (ENTERPRISE VERSION)
# ============================================================
def render_sidebar():

    # --------------------------------------------------------
    # 1. FETCH TENANTS (CACHED)
    # --------------------------------------------------------
    tenants = get_tenants()
    tenant_map = {t["name"]: t for t in tenants}

    selected_page = "Overview"

    with st.sidebar:

        st.markdown("")

        # ----------------------------------------------------
        # 2. BUSINESS SELECTOR
        # ----------------------------------------------------
        if not tenant_map:
            st.warning("No businesses found")
            st.stop()

        options = list(tenant_map.keys())

        current_tenant_id = st.session_state.get("tenant_id")

        default_index = 0
        for i, name in enumerate(options):
            if tenant_map[name]["id"] == current_tenant_id:
                default_index = i
                break

        selected_name = st.selectbox(
            "🏢 Business Portal",
            options,
            index=default_index,
            key="sidebar_business_selector"
        )

        active_company = tenant_map[selected_name]

        # ----------------------------------------------------
        # 3. UPDATE THEME (FAST STATE ONLY)
        # ----------------------------------------------------
        # ==============================
        # 🎨 GLOBAL BRAND COLOR
        # ==============================
        brand_color = st.session_state.get(
            "theme_color",
            "#2B3F87"
        )
        
        # ==============================
        # 🎨 SIDEBAR BACKGROUND ONLY
        # ==============================
        st.markdown(f"""
        <style>
        
        /* MAIN SIDEBAR BACKGROUND */
        section[data-testid="stSidebar"] {{
            background-color: {brand_color} !important;
        }}
        
        /* REMOVE INNER BLOCK COLORS */
        section[data-testid="stSidebar"] .stButton > button {{
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            color: white !important;
        }}
        
        /* TEXT */
        section[data-testid="stSidebar"] * {{
            color: white !important;
        }}
        
        </style>
        """, unsafe_allow_html=True)
        # ----------------------------------------------------
        # 4. LOGO + BRANDING
        # ----------------------------------------------------
        logo_url = build_logo_url(active_company.get("logo_url"))

        if logo_url:
            st.markdown(
                f"""
                <div style="display:flex; justify-content:center;">
                    <img src="{logo_url}?t={int(time.time())}"
                        width="80"
                        style="border-radius:50%; object-fit:cover;" />
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            st.markdown("### 🏢")

        st.markdown(
            f"""
            <div style='text-align:center; font-weight:600; color:white;'>
                {selected_name}
            </div>
            <div style='text-align:center; font-size:10px; color:rgba(255,255,255,0.6);'>
                FINANCE CORE
            </div>
            """,
            unsafe_allow_html=True
        )

        st.divider()

        # ----------------------------------------------------
        # 5. NAVIGATION MENU (FAST STATE ONLY)
        # ----------------------------------------------------
        menu = {
            "Overview": "📈",
            "loans": "💵",
            "borrowers": "👥",
            "Collateral": "🛡️",
            "Calendar": "📅",
            "Ledger": "📄",
            "Payroll": "💳",
            "Expenses": "📉",
            "Payments": "💰",
            "Reports": "📊",
            "Staff": "👥",
            "Settings": "⚙️"
        }

        menu_options = [f"{v} {k}" for k, v in menu.items()]

        current_page = st.session_state.get("current_page", "Overview")

        try:
            default_ix = list(menu.keys()).index(current_page)
        except:
            default_ix = 0

        selection = st.radio(
            "Navigation",
            menu_options,
            index=default_ix,
            label_visibility="collapsed",
            key="nav_radio"
        )

        selected_page = selection.split(" ", 1)[1]
        st.session_state["current_page"] = selected_page

        st.divider()

        # ----------------------------------------------------
        # 6. LOGOUT (SAFE + CLEAN)
        # ----------------------------------------------------
        if st.session_state.get("logged_in"):

            if st.button("🚪 Logout", use_container_width=True):

                # clear session safely
                for k in list(st.session_state.keys()):
                    del st.session_state[k]

                st.session_state["logged_in"] = False
                st.session_state["view"] = "login"

                st.rerun()

    return selected_page





def show_settings():
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
        # DATABASE UPdate (PERSISTENCE)
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
# FINAL APP ROUTER (REACTIVE & STABLE)
# ==========================================

if __name__ == "__main__":

    # 1. Initialize Default State
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    # 2. 🔐 AUTH FLOW
    if not st.session_state.get("logged_in"):
        st.session_state['theme_color'] = "#1E3A8A"
        apply_master_theme()
        run_auth_ui(db.supabase)
        # Note: run_auth_ui handles its own views. 
        # We don't want the rest of the script to run if not logged in.
    
    # 3. 🚀 MAIN APP (Only runs if logged_in is True)
    else:
        try:
            check_session_timeout()

            # Sidebar (Get the selected page)
            raw_page = render_sidebar()
            
            # Theme
            apply_master_theme()

            # 🔥 Clean the page string to ensure matching works perfectly
            # This handles any accidental spaces or casing issues
            page = str(raw_page).strip()

            # 4. 🗺️ NAVIGATION ROUTER
            if page == "Overview":
                show_overview()
                
            elif page == "loans":
                show_loans()
                
            elif page == "borrowers":
                show_borrowers()
                
            elif page == "Collateral":
                show_collateral()
                
            elif page == "Calendar":
                show_calendar()
                
            elif page == "Ledger":
                show_ledger()
                
            elif page == "Payments":
                show_payments()
                
            elif page == "Expenses":
                show_expenses()
                
            elif page == "Payroll":
                show_payroll()
                
            elif page == "Reports":
                show_reports()
            elif page == "Staff":
                show_staff()
                
            elif page == "Settings":
                show_settings()

            else:
                # If it falls through here, we show what exactly was received
                st.info(f"Module '{page}' is coming online soon.")
                # Debugging help:
                # st.write(f"DEBUG: Sidebar returned '{page}'")

        except Exception as e:
            st.error(f"🚨 Application Error: {e}")
            if st.button("Clear Cache & Retry"):
                st.cache_data.clear()
                st.rerun()
