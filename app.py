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

        # Step 3: Company Code Validation (Security Layer)
        if tenant_info["company_code"].strip().upper() != company_code.strip().upper():
            return {"success": False, "error": "Incorrect Company Code"}

        return {
            "success": True,
            "user_id": res.user.id,
            "tenant_id": record["tenant_id"],
            "role": record.get("role", "Staff"),
            "company": tenant_info.get("name")
        }

    except Exception as e:
        return {"success": False, "error": str(e)}

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
# 🎨 GLOBAL STYLING & HEADERS
# =========================================
import streamlit as st
import uuid
from datetime import datetime

def auth_styles():
    st.markdown("""
    <style>
    /* Clean, modern light background */
    .stApp {
        background: linear-gradient(135deg, #F0F4F8, #E2E8F0);
    }

    /* Cohesive Login Card Container */
    .auth-card {
        padding: 2.5rem;
        border-radius: 20px;
        background: #FFFFFF;
        box-shadow: 0 15px 35px rgba(0, 0, 0, 0.05), 0 5px 15px rgba(0, 0, 0, 0.03);
        border: 1px solid #E2E8F0;
        margin-top: 2rem;
    }

    /* Custom Form Embedded Header */
    .portal-badge-header {
        background-color: #1A252F;
        padding: 10px 14px;
        border-radius: 8px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1.5rem;
        color: #FFFFFF;
    }
    
    .badge-left {
        display: flex;
        gap: 12px;
        font-family: sans-serif;
        font-size: 12px;
    }

    /* Core Input Customizations */
    .stTextInput > div > div > input {
        border-radius: 10px !important;
        border: 1px solid #CBD5E1 !important;
    }

    /* Primary Interactive Buttons */
    .stFormSubmitButton > button {
        background: linear-gradient(90deg, #1E3A8A, #2563EB) !important;
        color: white !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        height: 44px !important;
        width: 100% !important;
        border: none !important;
    }

    /* Secondary Utility Buttons Alignment */
    div[data-testid="column"] .stButton > button {
        background-color: #F8FAFC !important;
        color: #334155 !important;
        border: 1px solid #E2E8F0 !important;
        border-radius: 10px !important;
        font-size: 13px !important;
        height: 38px !important;
    }
    div[data-testid="column"] .stButton > button:hover {
        background-color: #F1F5F9 !important;
        border-color: #CBD5E1 !important;
    }
    </style>
    """, unsafe_allow_html=True)


# =========================================
# 🏢 REGISTER COMPANY (MODAL DIALOG)
# =========================================
@st.dialog("🏢 Register Organization Profile")
def admin_company_registration(supabase):
    st.caption("Establish a new isolated multi-tenant environment")
    with st.form("company_reg_form"):
        company_name = st.text_input("Organization Name", placeholder="e.g., Zoe Consults")
        admin_name = st.text_input("Administrator Full Name")
        email = st.text_input("Root Administrative Email")
        pwd = st.text_input("Master Account Password", type="password")
        
        if st.form_submit_button("✨ Initialize Environment", use_container_width=True):
            if not all([company_name, admin_name, email, pwd]):
                st.error("All parameters must be supplied.")
                return
            try:
                res = supabase.auth.sign_up({"email": email, "password": pwd})
                if not res.user:
                    st.error("Credential framework registration failed.")
                    return

                tenant_id = str(uuid.uuid4())
                company_code = f"{company_name[:3].upper()}{uuid.uuid4().int % 999}"

                supabase.table("tenants").insert({
                    "id": tenant_id, "name": company_name, "company_code": company_code
                }).execute()

                supabase.table("users").insert({
                    "id": res.user.id, "name": admin_name, "email": email, "tenant_id": tenant_id, "role": "Admin"
                }).execute()

                st.success(f"✅ Infrastructure generated! Company Code: {company_code}")
            except Exception as e:
                st.error(f"Execution halted: {e}")


# =========================================
# 👥 STAFF SIGNUP (MODAL DIALOG)
# =========================================
@st.dialog("👥 Request Staff Access Credentials")
def view_staff_signup(supabase):
    st.caption("Provision a user seat under an active firm profile")
    with st.form("staff_signup_form"):
        company = st.text_input("Target Organization Name", placeholder="e.g., Zoe Consults")
        name = st.text_input("Your Full Name")
        email = st.text_input("Assigned Email Address")
        pwd = st.text_input("Access Password", type="password")
        
        if st.form_submit_button("🚀 Submit Credentials Request", use_container_width=True):
            if not all([company, name, email, pwd]):
                st.error("All registration entries are required.")
                return
            try:
                tenant_query = supabase.table("tenants").select("*").ilike("name", company).execute()
                if not tenant_query.data:
                    st.error("Target company domain verification failed.")
                    return
                tenant = tenant_query.data[0]

                res = supabase.auth.sign_up({"email": email, "password": pwd})
                if not res.user:
                    st.error("Authentication vault registration failed.")
                    return

                supabase.table("users").insert({
                    "id": res.user.id, "name": name, "email": email, "tenant_id": tenant["id"], "role": "Staff"
                }).execute()

                st.success("✅ Staff identity file provisioned.")
            except Exception as e:
                st.error(f"Provisional registry failed: {e}")


# =========================================
# 🔑 FORGOT PASSWORD (MODAL DIALOG)
# =========================================
@st.dialog("🔑 Password Reset Gateway")
def forgot_password_page(supabase):
    st.caption("Request a secure cryptographic recovery vector")
    email = st.text_input("Registered Recovery Email Address")
    if st.button("📩 Dispatch Link Token", use_container_width=True):
        if not email:
            st.error("Email destination vector missing.")
            return
        try:
            supabase.auth.reset_password_for_email(email)
            st.success("✅ Security token transmitted successfully.")
        except Exception as e:
            st.error(f"Transmission failed: {e}")


# =========================================
# 🔐 CLEANED UP LOGIN INTERFACE
# =========================================
def login_page(supabase):
    auth_styles()

    # Center alignment column structure
    _, central_grid, _ = st.columns([1, 1.4, 1])

    with central_grid:
        # Wrap everything cleanly inside a single structured element block
        st.markdown('<div class="auth-card">', unsafe_allow_html=True)
        
        # Embedded Header Bar - Replaces the disorganized external top header
        st.markdown("""
        <div class="portal-badge-header">
            <div class="badge-left">
                <span style="color: #3498DB; font-weight: bold;">🏢 Zoe Consults Admin</span>
                <span style="color: #BDC3C7;">|</span>
                <span style="color: #2ECC71;">🔒 System Vault</span>
            </div>
            <span style="font-size: 10px; color: #95A5A6; letter-spacing: 0.5px;">PORTAL SECURE</span>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("<h2 style='text-align: center; margin-bottom: 2px; color: #1E3A8A;'>PEAK-LENDERS AFRICA</h2>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #64748B; font-size: 14px; margin-bottom: 24px;'>Enterprise Multi-Tenant Node Login</p>", unsafe_allow_html=True)

        # Main login form block
        with st.form("login_form", clear_on_submit=False):
            company_name = st.text_input("Business Name Identification", placeholder="e.g., Zoe Consults")
            email = st.text_input("Email Address Address", placeholder="operator@firm.com")
            pwd = st.text_input("Security Access Code", type="password", placeholder="••••••••")
            
            st.markdown("<div style='margin-top: 14px;'></div>", unsafe_allow_html=True)
            submit = st.form_submit_button("🔓 Authenticate & Access Dashboard")

        st.markdown("<div style='margin-top: 20px; border-top: 1px solid #F1F5F9; padding-top: 15px;'></div>", unsafe_allow_html=True)

        # Realigned bottom utility columns
        util_1, util_2, util_3 = st.columns(3)
        with util_1:
            if st.button("🏢 New Organization", use_container_width=True):
                admin_company_registration(supabase)
        with util_2:
            if st.button("👥 Staff Signup", use_container_width=True):
                view_staff_signup(supabase)
        with util_3:
            if st.button("🔑 Reset Pass", use_container_width=True):
                forgot_password_page(supabase)

        st.markdown('</div>', unsafe_allow_html=True)

    # =====================================
    # 🔐 REGISTRY & REDIRECT DISPATCHER
    # =====================================
    if submit:
        email = email.strip().lower()
        company_name = company_name.strip().lower()

        if not all([company_name, email, pwd]):
            st.error("All credential vectors are required.")
            return

        try:
            res = supabase.auth.sign_in_with_password({"email": email, "password": pwd})
            if not res or not res.user:
                st.error("Authentication sequence rejected.")
                return

            user_query = supabase.table("users").select("*, tenants(name)").eq("id", res.user.id).execute()
            data = getattr(user_query, "data", None)

            if not data:
                st.error("Identity data target record missing.")
                return

            user = data[0]
            db_company = ((user.get("tenants") or {}).get("name", "")).lower()

            if db_company != company_name:
                st.error(f"Identity profile is not provisioned under context: '{company_name}'")
                return

            # Save clean state variables
            st.session_state["logged_in"] = True
            st.session_state["authenticated"] = True
            st.session_state["user_id"] = user["id"]
            st.session_state["user_name"] = user.get("name", "Evans Ahuura")
            st.session_state["tenant_id"] = user["tenant_id"]
            st.session_state["role"] = user.get("role", "Staff")
            st.session_state["company"] = user.get("tenants", {}).get("name", "Zoe Consults")

            st.session_state["view"] = "main"
            st.success("Context token approved.")
            st.rerun()

        except Exception as e:
            st.error(f"Process failed: {e}")


def run_auth_ui(supabase):
    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    if st.session_state["view"] == "login":
        login_page(supabase)
    elif st.session_state["view"] == "main":
        st.empty()
    # 🔥 LOGIN FLOW
    if view == "login":
        login_page(supabase)

    # 👥 STAFF SIGNUP
    elif view == "signup":
        view_staff_signup(supabase)

    # 🏢 COMPANY REGISTRATION
    elif view == "create_company":
        admin_company_registration(supabase)

    # 🚀 AFTER LOGIN SAFE STATE
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
        st.session_state["theme_color"] = active_company.get(
            "brand_color",
            "#1E3A8A"
        )

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
