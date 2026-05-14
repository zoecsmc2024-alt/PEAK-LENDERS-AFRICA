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
# 🎨 GLOBAL STYLING
# =========================================
def auth_styles():

    st.markdown("""
    <style>

    .stApp {
        background: linear-gradient(135deg,#EEF4FF,#F8FAFC);
    }

    .auth-card {
        padding: 2rem;
        border-radius: 24px;
        background: white;
        box-shadow: 0 10px 30px rgba(0,0,0,0.08);
        border: 1px solid #E5E7EB;
    }

    .stButton > button {
        border-radius: 12px;
        height: 42px;
        border: none;
        font-weight: 600;
        transition: 0.3s;
    }

    .stFormSubmitButton > button {
        background: linear-gradient(90deg,#1D4ED8,#2563EB);
        color: white;
    }

    .stFormSubmitButton > button:hover {
        background: linear-gradient(90deg,#2563EB,#3B82F6);
        color: white;
    }

    .stTextInput > div > div > input {
        border-radius: 12px;
        border: 1px solid #CBD5E1;
        padding: 12px;
    }

    div[data-testid="column"] .stButton > button {
        background: white;
        color: #1E3A8A;
        border: 1px solid #CBD5E1;
        height: 38px;
    }

    div[data-testid="column"] .stButton > button:hover {
        border: 1px solid #2563EB;
        color: #2563EB;
        background: #EFF6FF;
    }

    </style>
    """, unsafe_allow_html=True)


# =========================================
# 🏢 REGISTER COMPANY
# =========================================
@st.dialog("🏢 Register Company")
def admin_company_registration(supabase):

    st.caption("Create your organization account")

    with st.form("company_reg_form"):

        company_name = st.text_input(
            "Organization Name",
            placeholder="Peak-Lenders Africa"
        )

        admin_name = st.text_input(
            "Admin Full Name",
            placeholder="John Doe"
        )

        email = st.text_input(
            "Business Email",
            placeholder="admin@company.com"
        )

        pwd = st.text_input(
            "Password",
            type="password"
        )

        submit = st.form_submit_button(
            "✨ Create Organization",
            use_container_width=True
        )

    if submit:

        if not all([company_name, admin_name, email, pwd]):
            st.error("All fields are required")
            return

        try:

            # CREATE AUTH USER
            res = supabase.auth.sign_up({
                "email": email,
                "password": pwd
            })

            if not res.user:
                st.error("Registration failed")
                return

            tenant_id = str(uuid.uuid4())

            company_code = (
                f"{company_name[:3].upper()}"
                f"{uuid.uuid4().int % 999}"
            )

            # CREATE TENANT
            supabase.table("tenants").insert({
                "id": tenant_id,
                "name": company_name,
                "company_code": company_code
            }).execute()

            # CREATE ADMIN USER
            supabase.table("users").insert({
                "id": res.user.id,
                "name": admin_name,
                "email": email,
                "tenant_id": tenant_id,
                "role": "Admin"
            }).execute()

            # SAFE AUDIT
            safe_audit_log(supabase, {
                "user_id": res.user.id,
                "action": "CREATE_COMPANY",
                "tenant_id": tenant_id,
                "timestamp": datetime.now().isoformat()
            })

            st.success(
                f"✅ Organization created!\n\n"
                f"Company Code: {company_code}"
            )

        except Exception as e:
            st.error(f"Registration failed: {e}")


# =========================================
# 👥 STAFF SIGNUP
# =========================================
@st.dialog("👥 Staff Registration")
def view_staff_signup(supabase):

    st.caption("Create a staff account")

    with st.form("staff_signup_form"):

        company = st.text_input(
            "Company Name",
            placeholder="Peak-Lenders Africa"
        )

        name = st.text_input(
            "Full Name",
            placeholder="Jane Doe"
        )

        email = st.text_input(
            "Email Address",
            placeholder="staff@company.com"
        )

        pwd = st.text_input(
            "Password",
            type="password"
        )

        submit = st.form_submit_button(
            "🚀 Create Staff Account",
            use_container_width=True
        )

    if submit:

        if not all([company, name, email, pwd]):
            st.error("All fields are required")
            return

        try:

            # FIND COMPANY
            tenant_query = supabase.table("tenants") \
                .select("*") \
                .ilike("name", company) \
                .execute()

            if not tenant_query.data:
                st.error("Company not found")
                return

            tenant = tenant_query.data[0]

            # CREATE AUTH USER
            res = supabase.auth.sign_up({
                "email": email,
                "password": pwd
            })

            if not res.user:
                st.error("Signup failed")
                return

            # CREATE PROFILE
            supabase.table("users").insert({
                "id": res.user.id,
                "name": name,
                "email": email,
                "tenant_id": tenant["id"],
                "role": "Staff"
            }).execute()

            # SAFE AUDIT
            safe_audit_log(supabase, {
                "user_id": res.user.id,
                "action": "CREATE_STAFF",
                "tenant_id": tenant["id"],
                "timestamp": datetime.now().isoformat()
            })

            st.success("✅ Staff account created!")

        except Exception as e:
            st.error(f"Signup failed: {e}")


# =========================================
# 🔑 FORGOT PASSWORD
# =========================================
@st.dialog("🔑 Forgot Password")
def forgot_password_page(supabase):

    st.caption("Reset your account password")

    email = st.text_input(
        "Registered Email",
        placeholder="you@example.com"
    )

    if st.button(
        "📩 Send Reset Link",
        use_container_width=True
    ):

        if not email:
            st.error("Email required")
            return

        try:

            supabase.auth.reset_password_for_email(email)

            st.success(
                "✅ Password reset email sent"
            )

        except Exception as e:
            st.error(f"Reset failed: {e}")


# =========================================
# 🔐 LOGIN PAGE
# =========================================
def login_page(supabase):

    auth_styles()

    st.write("")
    st.write("")
    st.write("")

    left, center, right = st.columns([1, 1.2, 1])

    with center:

        with st.container(border=True):

            st.markdown("# 💰 PEAK-LENDERS AFRICA")
            st.caption("Secure Business Login Portal")

            st.divider()

            # LOGIN FORM
            with st.form("login_form"):

                company_name = st.text_input(
                    "Business Name",
                    placeholder="Enter company name"
                )

                email = st.text_input(
                    "Email Address",
                    placeholder="Enter email"
                )

                pwd = st.text_input(
                    "Password",
                    type="password",
                    placeholder="Enter password"
                )

                submit = st.form_submit_button(
                    "🔓 Access Dashboard",
                    use_container_width=True
                )

            st.write("")

            # ACTION BUTTONS
            col1, col2, col3 = st.columns(3)

            with col1:
                if st.button(
                    "🏢 Register",
                    use_container_width=True
                ):
                    admin_company_registration(supabase)

            with col2:
                if st.button(
                    "👥 Staff",
                    use_container_width=True
                ):
                    view_staff_signup(supabase)

            with col3:
                if st.button(
                    "🔑 Reset",
                    use_container_width=True
                ):
                    forgot_password_page(supabase)

    # =====================================
    # 🔐 LOGIN LOGIC (FIXED + STABLE)
    # =====================================
    if submit:
    
        email = email.strip().lower()
        company_name = company_name.strip().lower()
    
        if not all([company_name, email, pwd]):
            st.error("All fields are required")
            return
    
        try:
    
            # =========================
            # AUTH LOGIN
            # =========================
            res = supabase.auth.sign_in_with_password({
                "email": email,
                "password": pwd
            })
    
            if not res or not res.user:
                st.error("Invalid credentials")
                return
    
            # =========================
            # PROFILE FETCH
            # =========================
            user_query = supabase.table("users") \
                .select("*, tenants(name)") \
                .eq("id", res.user.id) \
                .execute()
    
            data = getattr(user_query, "data", None)
    
            if not data:
                st.error("Profile not found")
                return
    
            user = data[0]
    
            db_company = (
                (user.get("tenants") or {}).get("name", "")
            ).lower()
    
            if db_company != company_name:
                st.error(f"Not linked to '{company_name}'")
                return
    
            # =========================
            # SESSION STATE (SOURCE OF TRUTH)
            # =========================
            st.session_state["logged_in"] = True
            st.session_state["authenticated"] = True
            st.session_state["user_id"] = user["id"]
            st.session_state["tenant_id"] = user["tenant_id"]
            st.session_state["role"] = user.get("role", "Staff")
            st.session_state["company"] = db_company
    
            # IMPORTANT: reset view so login page disappears
            st.session_state["view"] = "main"
    
            st.success("Login successful")
            st.rerun()
    
        except Exception as e:
            st.error(f"Login failed: {e}")

    # =========================================
    # 🌐 AUTH ROUTER
    # =========================================
    def run_auth_ui(supabase):
    
        if "view" not in st.session_state:
            st.session_state["view"] = "login"
    
        view = st.session_state["view"]
    
        # 🔥 LOGIN FLOW
        if view == "login":
            login_page(supabase)
    
        # 👥 STAFF SIGNUP
        elif view == "signup":
            view_staff_signup(supabase)
    
        # 🏢 COMPANY REGISTRATION
        elif view == "create_company":
            admin_company_registration(supabase)
    
        # 🚀 AFTER LOGIN SAFE STATE (prevents stuck UI)
        elif view == "main":
            st.empty()
            # Do nothing → main app will take over in router


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
                show_dashboard_view()
                
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
