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









import streamlit as st
import pandas as pd
import uuid
import plotly.express as px
from datetime import datetime

def show_expenses():
    st.markdown("<h2 style='color: #2B3F87;'>📁 Expense Management</h2>", unsafe_allow_html=True)

    current_tenant = st.session_state.get('tenant_id')
    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    def get_fy_label(date_val):
        try:
            dt = pd.to_datetime(date_val)
            return f"FY{dt.year}-{dt.year+1}" if dt.month >= 7 else f"FY{dt.year-1}-{dt.year}"
        except:
            return "Unknown FY"

    # ==============================
    # DATA
    # ==============================
    try:
        df = get_cached_data("expenses")
    except:
        df = pd.DataFrame()

    if df is not None and not df.empty:
        df.columns = df.columns.str.lower().str.strip()

        if "tenant_id" in df.columns:
            df = df[df["tenant_id"].astype(str) == str(current_tenant)].copy()

        df["id"] = df["id"].astype(str)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
        df["financial_year"] = df["payment_date"].apply(get_fy_label)
    else:
        df = pd.DataFrame(columns=[
            "id","category","amount","date","description",
            "payment_date","receipt_no","tenant_id","financial_year"
        ])

    EXPENSE_CATS = ["Rent","Insurance","Utilities","Salaries","Licence Expenses","Marketing","Office Expenses","Operating Expenses","Fuel and Motor Vehicle","Taxes","Corporate Social Responsibilities","Other"]

    tab_add, tab_view, tab_manage = st.tabs([
        "➕ Record Expense","📊 Spending Analysis","⚙️ Manage Records"
    ])

    # ==============================
    # ADD
    # ==============================
    with tab_add:
        with st.form("add_expense_form", clear_on_submit=True):
            st.write("### Log Operational Cost")
            c1, c2 = st.columns(2)

            category = c1.selectbox("category", EXPENSE_CATS)
            amount = c2.number_input("amount (UGX)", min_value=0, step=1000)
            desc = st.text_input("Description")

            c3, c4 = st.columns(2)
            p_date = c3.date_input("Payment date", value=datetime.now())
            receipt_no = c4.text_input("Receipt #")

            if st.form_submit_button("🚀 Save Expense", use_container_width=True):
                if amount > 0 and desc:
                    try:
                        d = p_date.strftime("%Y-%m-%d")

                        new = pd.DataFrame([{
                            "id": str(uuid.uuid4()),
                            "category": category,
                            "amount": float(amount),
                            "date": d,
                            "description": desc,
                            "payment_date": d,
                            "receipt_no": receipt_no,
                            "tenant_id": str(current_tenant)
                        }])

                        save_df = pd.concat([df, new], ignore_index=True)\
                            .drop(columns=["financial_year"], errors="ignore")

                        if save_data("expenses", save_df):
                            st.success(f"✅ Expense saved for {d}")
                            st.cache_data.clear()
                            st.rerun()

                    except Exception as e:
                        st.error(f"🚨 {e}")
                else:
                    st.warning("⚠️ Fill all required fields")

    # ==============================
    # VIEW
    # ==============================
    with tab_view:
        if df.empty:
            st.info("💡 No expenses recorded yet.")
        else:
            fys = sorted(df["financial_year"].unique(), reverse=True)
            fy = st.selectbox("📅 Financial Year", ["All Time"] + fys)

            view_df = df if fy == "All Time" else df[df["financial_year"] == fy]

            total = view_df["amount"].sum()

            # 🎨 COLORED CARD
            st.markdown(f"""
                <div style="background-color:#fff;padding:20px;border-radius:15px;
                border-left:6px solid #FF4B4B;box-shadow:2px 2px 10px rgba(0,0,0,0.05);">
                    <p style="margin:0;font-size:12px;color:#666;font-weight:bold;">
                        TOTAL OUTFLOW ({fy})
                    </p>
                    <h2 style="margin:0;color:#FF4B4B;">
                        UGX {total:,.0f}
                    </h2>
                </div><br>
            """, unsafe_allow_html=True)

            col1, col2 = st.columns([2,1])

            with col1:
                fig = px.pie(
                    view_df.groupby("category")["amount"].sum().reset_index(),
                    names="category", values="amount",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.write("#### FY Summary")
                st.dataframe(
                    df.groupby("financial_year")["amount"].sum().reset_index(),
                    hide_index=True
                )

            # 🎨 STYLED LEDGER

            # 4. Detailed Ledger
            st.markdown("### 📋 Expense Ledger")
            
            ledger_df = view_df.sort_values("payment_date", ascending=False).copy()
            
            try:
                # --- Clean data ---
                ledger_df["payment_date"] = pd.to_datetime(ledger_df["payment_date"], errors="coerce")
                ledger_df["amount"] = pd.to_numeric(ledger_df["amount"], errors="coerce").fillna(0)
            
                display_ledger = ledger_df[[
                    "payment_date",
                    "category",
                    "description",
                    "amount",
                    "receipt_no"
                ]].copy()
            
                display_ledger.columns = [
                    "date",
                    "category",
                    "Description",
                    "amount (UGX)",
                    "Ref #"
                ]
            
                # --- Format date ---
                display_ledger["date"] = display_ledger["date"].dt.strftime("%Y-%m-%d")
            
                # --- FORMAT commas (IMPORTANT) ---
                display_ledger["amount (UGX)"] = display_ledger["amount (UGX)"].apply(
                    lambda x: f"{x:,.0f}"
                )
            
                # --- Filters ---
                col1, col2 = st.columns(2)
            
                categories = ["All"] + sorted(display_ledger["category"].dropna().unique().tolist())
                selected_cat = col1.selectbox("Filter category", categories)
            
                # convert back to numeric for filtering (because we formatted strings)
                ledger_df["amount_num"] = pd.to_numeric(ledger_df["amount"], errors="coerce").fillna(0)
            
                min_amt, max_amt = col2.slider(
                    "amount Range",
                    float(ledger_df["amount_num"].min()),
                    float(ledger_df["amount_num"].max()),
                    (float(ledger_df["amount_num"].min()), float(ledger_df["amount_num"].max()))
                )
            
                # --- Apply filters ---
                if selected_cat != "All":
                    filtered = ledger_df[ledger_df["category"].fillna("General") == selected_cat]
                else:
                    filtered = ledger_df.copy()
                
                # --- amount filter ---
                filtered = filtered[
                    (filtered["amount_num"] >= min_amt) &
                    (filtered["amount_num"] <= max_amt)
                ]
            
                # --- Rebuild display after filtering ---
                final_display_df = filtered[[
                    "payment_date",
                    "category",
                    "description",
                    "amount",
                    "receipt_no"
                ]].copy()
                
                final_display_df.columns = [
                    "date",
                    "category",
                    "Description",
                    "amount (UGX)",
                    "Ref #"
                ]
                
                final_display_df["date"] = pd.to_datetime(
                    final_display_df["date"],
                    errors="coerce"
                ).dt.strftime("%Y-%m-%d")
                
                final_display_df["amount (UGX)"] = final_display_df["amount (UGX)"].apply(
                    lambda x: f"{float(x):,.0f}"
                )
                
                def color_amount(val):
                    return "color: #D32F2F; font-weight: 700;"
                
                styled = final_display_df.style.map(
                    color_amount,
                    subset=["amount (UGX)"]
                )
                
                st.dataframe(
                    styled,
                    use_container_width=True,
                    hide_index=True
                )
            
            except Exception as e:
                st.error(f"Ledger error: {e}")
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
                    new_amt = st.number_input("Update amount (UGX)", value=float(target_record['amount']))
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
                        full_df = get_cached_data("expenses")  # reload FULL dataset
                    
                        full_df["id"] = full_df["id"].astype(str)
                    
                        updated_df = full_df[full_df["id"] != str(target_record["id"])]
                    
                        if save_data("expenses", updated_df):
                            st.warning("🗑️ Record deleted.")
                            st.cache_data.clear()
                            st.rerun()
# ==============================
# 21. MASTER LEDGER 
# ==============================
def generate_pdf_statement(client_name, loans_df, payments_df):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    styles = getSampleStyleSheet()
    elements = []
    elements.append(Paragraph(f"<b>{st.session_state.get('company_name', 'ZOE CONSULTS').upper()}</b>", styles["Title"]))
    elements.append(Paragraph(f"Client: {client_name}", styles["Normal"]))
    elements.append(Paragraph(f"Statement date: {datetime.now().strftime('%d %b %Y')}", styles["Normal"]))
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
        data = [["date", "Description", "Debit", "Credit", "balance"]]
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
def show_ledger():
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

    loans_df.columns = loans_df.columns.str.strip().str.lower().str.replace(" ", "_")
    if payments_df is not None and not payments_df.empty:
        payments_df.columns = payments_df.columns.str.strip().str.lower().str.replace(" ", "_")
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
    
    m1, m2, m3, m4 = st.columns(4)
    
    # 🟦 Principal (neutral blue feel via delta)
    m1.metric(
        "Principal",
        f"UGX {p:,.0f}",
        delta="Base loan",
        delta_color="off"
    )
    
    # 🟨 Interest (warning style via delta)
    m2.metric(
        "Total Interest",
        f"UGX {i:,.0f}",
        delta=f"{(i/total_due*100):.1f}% of total" if total_due > 0 else None,
        delta_color="normal"
    )
    
    # 🟢 Paid (good = positive green)
    m3.metric(
        "Total Paid",
        f"UGX {paid:,.0f}",
        delta=f"{paid/total_due:.1%}" if total_due > 0 else None,
        delta_color="normal"
    )
    
    # 🔴 Balance (inverse makes it red when high)
    m4.metric(
        "Current Balance",
        f"UGX {bal:,.0f}",
        delta=f"-{bal:,.0f}",
        delta_color="inverse"
    )
    # ==============================
    # 📜 TRANSACTION HISTORY (LEDGER)
    # ==============================
    ledger_data = []
    running_bal = p + i

    # Entry 1: Disbursement
    ledger_data.append({
        "date": str(loan_info.get("start_date", "-"))[:10],
        "Description": "🏦 Loan Disbursement",
        "Debit (Due)": p,
        "Credit (Paid)": 0,
        "balance": running_bal
    })

    # Entry 2: interest Charge
    if i > 0:
        ledger_data.append({
            "date": str(loan_info.get("start_date", "-"))[:10],
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
                    "date": str(p_row.get("date", p_row.get("payment_date", "-")))[:10],
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
            "date": st.column_config.TextColumn("date"),
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
