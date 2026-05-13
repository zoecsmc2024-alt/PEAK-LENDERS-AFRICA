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
# ⚡ CORE DATA ENGINE
# ==============================
@st.cache_data(ttl=600, show_spinner=False)
def get_cached_data(table_name):
    try:
        if supabase is None:
            return pd.DataFrame()

        require_tenant()
        tenant_id = get_tenant_id()

        res = supabase.table(table_name)\
            .select("*")\
            .eq("tenant_id", tenant_id)\
            .execute()

        if res.data:
            df = pd.DataFrame(res.data)
            df.columns = df.columns.str.strip().str.lower()
            return df

        return pd.DataFrame()

    except Exception as e:
        st.error(f"Database Fetch Error [{table_name}]: {e}")
        return pd.DataFrame()

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

# ==============================
# 🔌 2. SUPABASE INIT (SAFE GLOBAL)
# ==============================

# FIND THIS SECTION IN YOUR database.py
@st.cache_resource
def init_supabase():
    try:
        url = st.secrets.get("SUPABASE_URL")
        key = st.secrets.get("SUPABASE_KEY")
        if not url or not key:
            return None
        return create_client(url, key)
    except Exception:
        return None

# THIS LINE IS THE KEY: It makes 'supabase' available to other files
supabase = init_supabase()

# Global Instance
supabase = init_supabase()

if supabase is None:
    st.warning("⚠️ Supabase not connected (some features may not work)")


# ==============================
# 💾 3. DATA PERSISTENCE
# ==============================

def save_data(table_name, dataframe):
    try:
        if supabase is None:
            st.error("❌ Database not connected")
            return False

        require_tenant()

        if dataframe is None or dataframe.empty:
            st.error("No Data")
            return False

        df_to_save = dataframe.copy()
        df_to_save["tenant_id"] = get_tenant_id()

        records = df_to_save.replace({np.nan: None}).to_dict("records")

        response = supabase.table(table_name).upsert(records).execute()

        if response.data:
            st.success(f"Saved {len(response.data)} record(s)")
            return True

        return False

    except Exception as e:
        st.error(f"DB Error [{table_name}]: {e}")
        return False
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
            "Overdue Tracker": "🚨",
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
import plotly.express as px

# ==========================================
# 1. CORE PAGE FUNCTIONS (BRANDING & WIDE LAYOUT)
# ==========================================

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

def show_dashboard_view():
    brand_color = get_Active_color()

    try:
        # --- SLEEK METRIC CARD STYLE ---
        st.markdown(f"""
        <style>
        .metric-card {{
            background: rgba(255,255,255,0.92);
            padding: 10px 14px;
            border-radius: 12px;
            border: 1px solid rgba(0,0,0,0.05);
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
            backdrop-filter: blur(6px);
            transition: all 0.2s ease;
            max-width: 170px;
            margin: auto;
        }}
        
        .metric-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 14px rgba(0,0,0,0.08);
            border-color: rgba(0,0,0,0.08);
        }}
        
        .metric-card h3 {{
            font-size: 0.9rem;
            margin-bottom: 4px;
            color: #666;
            font-weight: 500;
        }}
        
        .metric-card h1 {{
            font-size: 1.4rem;
            margin: 0;
            font-weight: 700;
            color: #111;
        }}
        
        @media (max-width:768px) {{
            .metric-card {{
                padding: 8px 12px;
                max-width: 145px;
            }}
        
            .metric-card h1 {{
                font-size: 1.2rem;
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

        # =========================================================
        # 🛡️ SMART STATUS LOGIC (CRITICAL FIX FOR DASHBOARD SYNC)
        # =========================================================
        # Ensure SN and Balance are ready for processing
        if "sn" in loans_df.columns:
            loans_df["sn"] = loans_df["sn"].astype(str).str.strip().str.upper()
            
            # Use the most accurate balance column available
            bal_col = first_existing(loans_df, ["balance", "total_repayable"])
            loans_df["tmp_bal"] = pd.to_numeric(loans_df[bal_col], errors="coerce").fillna(0)
            
            # Sort for chronological sequence
            loans_df = loans_df.sort_values(by=["sn", "cycle_no", "start_date"])

            for _, grp in loans_df.groupby("sn"):
                indices = grp.index.tolist()
                
                # Mark all previous cycles as BCF
                if len(indices) > 1:
                    loans_df.loc[indices[:-1], "status"] = "BCF"
                
                # Check the latest cycle for clearance
                latest_idx = indices[-1]
                if abs(loans_df.at[latest_idx, "tmp_bal"]) < 1.0:
                    loans_df.at[latest_idx, "status"] = "CLEARED"
            
            # Global Override: If balance is zero and not BCF, it's CLEARED
            loans_df.loc[(loans_df["tmp_bal"] <= 0) & (loans_df["status"] != "BCF"), "status"] = "CLEARED"
            loans_df.drop(columns=["tmp_bal"], inplace=True)
        
        # ==============================
        # ENSURE REQUIRED COLUMNS EXIST
        # ==============================
        required_loan_cols = [
            "status", "principal", "amount", "interest", 
            "interest_amount", "balance", "total_repayable", 
            "amount_paid", "paid", "end_date", "due_date"
        ]
        
        for col in required_loan_cols:
            if col not in loans_df.columns:
                loans_df[col] = 0
        
        # ==============================
        # SAFE NUMERIC ENGINE
        # ==============================
        def get_numeric(df, cols):
            for c in cols:
                if c in df.columns:
                    return pd.to_numeric(df[c], errors="coerce").fillna(0)
            return pd.Series([0] * len(df), index=df.index)
        
        # ==============================
        # SAFE DATE ENGINE
        # ==============================
        def get_dates(df, cols):
            for c in cols:
                if c in df.columns:
                    return pd.to_datetime(df[c], errors="coerce")
            return pd.Series([pd.NaT] * len(df), index=df.index)
        
        # ==============================
        # ENGINE: UNIFIED CALCULATIONS
        # ==============================
        loans_df["principal_n"] = pd.to_numeric(
            loans_df.get("principal", 0),
            errors="coerce" 
        ).fillna(0)
        
        loans_df["interest_n"] = get_numeric(
            loans_df,
            ["interest", "interest_amount"]
        )
        
        total_repayable = get_numeric(
            loans_df,
            ["balance", "total_repayable"]
        )
        
        amount_paid = get_numeric(
            loans_df,
            ["amount_paid", "paid"]
        )
        
        # Safer balance calc
        loans_df["balance_n"] = (total_repayable - amount_paid).clip(lower=0)
        
        # ==============================
        # EXPENSES
        # ==============================
        if expenses_df is None or expenses_df.empty:
            total_expenses = 0
        else:
            if "amount" not in expenses_df.columns:
                expenses_df["amount"] = 0
        
            expenses_df["amount"] = pd.to_numeric(
                expenses_df["amount"],
                errors="coerce"
            ).fillna(0)
        
            total_expenses = float(expenses_df["amount"].sum())
        
        # ==============================
        # OVERDUE ENGINE
        # ==============================
        today = pd.Timestamp.now().normalize()
        
        loans_df["due_date_dt"] = get_dates(
            loans_df,
            ["end_date", "due_date"]
        )
        
        loans_df["status"] = (
            loans_df["status"]
            .astype(str)
            .str.upper()
        )
        
        # Now overdue count correctly excludes CLEARED and BCF loans
        overdue_mask = (
            loans_df["due_date_dt"].notna()
            & (loans_df["due_date_dt"] < today)
            & (~loans_df["status"].isin(["CLEARED", "BCF", "CLOSED"]))
        )
        
        overdue_count = int(overdue_mask.sum())
        
        # ==============================
        # TOTALS (FIXED)
        # ==============================
        if "cycle_no" not in loans_df.columns:
            loans_df["cycle_no"] = 1
        
        loans_df["cycle_no"] = pd.to_numeric(
            loans_df["cycle_no"],
            errors="coerce"
        ).fillna(1)
        
        # ONLY original loans for exposure calculations
        original_loans = loans_df[loans_df["cycle_no"] == 1].copy()
        
        total_principal = float(original_loans["principal_n"].sum())
        total_interest = float(loans_df["interest_n"].sum())
        
        # ==============================
        # SMART ALERTS
        # ==============================
        if overdue_count >= 5:
            st.warning(f"⚠️ {overdue_count} overdue loans need urgent attention.")

    # ==============================
    # SLEEK + MUTED METRIC CARD STYLES
    # ==============================
    st.markdown("""
    <style>
    .metric-box {
        padding: 16px;
        border-radius: 16px;
        color: #F9FAFB;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.08);
        box-shadow: 0 4px 14px rgba(0,0,0,0.06);
        margin-bottom: 10px;
        transition: all 0.22s ease;
        overflow: hidden;
        position: relative;
    }
    
    .metric-box:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 18px rgba(0,0,0,0.10);
    }
    
    .metric-title {
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1.1px;
        opacity: 0.78;
        margin-bottom: 6px;
    }
    
    .metric-value {
        font-size: 26px;
        font-weight: 700;
        line-height: 1.1;
        margin-bottom: 4px;
    }
    
    .metric-sub {
        font-size: 11px;
        font-weight: 500;
        opacity: 0.72;
    }
    
    .blue-card {
        background: linear-gradient(135deg, #5B6B8C, #3E4C68);
    }
    
    .green-card {
        background: linear-gradient(135deg, #5E7C6B, #3E5A4C);
    }
    
    .red-card {
        background: linear-gradient(135deg, #8B5E5E, #5E3E3E);
    }
    
    .orange-card {
        background: linear-gradient(135deg, #9A7B5F, #6B523E);
    }
    
    @media (max-width:768px) {
        .metric-box {
            padding: 14px;
            border-radius: 14px;
        }
    
        .metric-value {
            font-size: 22px;
        }
    
        .metric-title,
        .metric-sub {
            font-size: 10px;
        }
    }
    </style>
    """, unsafe_allow_html=True)
            
    # ==============================
    # CARD HELPER
    # ==============================
    def render_metric_card(container, title, value, subtitle, css_class):
        with container:
            st.markdown(
                f"""
                <div class="metric-box {css_class}">
                    <div class="metric-title">{title}</div>
                    <div class="metric-value">{value}</div>
                    <div class="metric-sub">{subtitle}</div>
                </div>
                """,
                unsafe_allow_html=True
            )
    
    # ==============================
    # TOP METRICS
    # ==============================
    try:
        m1, m2, m3, m4 = st.columns(4)
        
        render_metric_card(
            m1,
            "Active Principal",
            f"UGX {total_principal:,.0f}",
            "Portfolio Value",
            "blue-card"
        )
        
        render_metric_card(
            m2,
            "Interest Income",
            f"UGX {total_interest:,.0f}",
            "Expected Earnings",
            "green-card"
        )
        
        render_metric_card(
            m3,
            "Operational Costs",
            f"UGX {total_expenses:,.0f}",
            "Total Expenses",
            "red-card"
        )
        
        render_metric_card(
            m4,
            "Critical Alerts",
            str(overdue_count),
            "Overdue Loans",
            "orange-card"
        )
    except NameError:
        pass # Handle if calculation failed earlier
        
    st.write("")
    # --- 4. DATA VISUALIZATION SECTION ---
    col_l, col_r = st.columns([2, 1])

    with col_l:
        st.markdown("#### 📈 Revenue Trend vs Expenses")
        try:
            if not payments_df.empty:
                # REVENUE
                pay_date_col = first_existing(payments_df, ["date", "payment_date", "created_at"])
                pay_amt_col = first_existing(payments_df, ["amount", "paid", "payment"])

                if pay_date_col and pay_amt_col:
                    rev_df = payments_df.copy()
                    rev_df["date_dt"] = pd.to_datetime(rev_df[pay_date_col], errors="coerce")
                    rev_df["amount_n"] = pd.to_numeric(rev_df[pay_amt_col], errors="coerce").fillna(0)
                    rev_df = rev_df.dropna(subset=["date_dt"])
                    rev_df["month"] = rev_df["date_dt"].dt.to_period("M").dt.to_timestamp()
                    
                    monthly_rev = rev_df.groupby("month", as_index=False)["amount_n"].sum()
                    monthly_rev.rename(columns={"amount_n": "Revenue"}, inplace=True)
            
                    # EXPENSES
                    if not expenses_df.empty:
                        exp_df = expenses_df.copy()
                        exp_date_col = first_existing(exp_df, ["payment_date", "date", "created_at"])
                        exp_df["date_dt"] = pd.to_datetime(exp_df[exp_date_col], errors="coerce")
                        exp_df["amount_n"] = pd.to_numeric(exp_df["amount"], errors="coerce").fillna(0)
                        exp_df = exp_df.dropna(subset=["date_dt"])
                        exp_df["month"] = exp_df["date_dt"].dt.to_period("M").dt.to_timestamp()
                        
                        monthly_exp = exp_df.groupby("month", as_index=False)["amount_n"].sum()
                        monthly_exp.rename(columns={"amount_n": "Expenses"}, inplace=True)
                    else:
                        monthly_exp = pd.DataFrame(columns=["month", "Expenses"])
            
                    # MERGE
                    trend_df = pd.merge(monthly_rev, monthly_exp, on="month", how="outer").fillna(0)
                    trend_df = trend_df.sort_values("month")
            
                    # PLOT
                    fig = px.line(
                        trend_df,
                        x="month",
                        y=["Revenue", "Expenses"],
                        template="plotly_white",
                        color_discrete_map={"Revenue": "#10B981", "Expenses": "#EF4444"}
                    )
                    fig.update_traces(mode="lines+markers")
                    fig.update_layout(
                        height=320,
                        margin=dict(l=0, r=0, t=20, b=0),
                        legend_title="",
                        xaxis_title="",
                        yaxis_title="amount (UGX)",
                        hovermode="x unified"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Payment columns missing.")
            else:
                st.info("Insufficient payment history for trend analysis.")
        except Exception as e:
            st.warning(f"Revenue chart temporarily unavailable: {e}")

    with col_r:
        st.markdown("#### 🎯 Portfolio Health")
        try:
            if not loans_df.empty and "status" in loans_df.columns:
                clean_status = loans_df["status"].astype(str).str.strip().str.upper()
                clean_status = clean_status.replace({
                    "CURRENT": "ACTIVE", "ONGOING": "ACTIVE",
                    "COMPLETE": "PAID", "CLOSED": "PAID",
                    "LATE PAYMENT": "LATE", "DEFAULTED": "DEFAULT"
                })
                status_data = clean_status.value_counts().reset_index()
                status_data.columns = ["status", "count"]
                total_loans = status_data["count"].sum()
        
                color_map = {"ACTIVE": "#10B981", "PAID": "#3B82F6", "LATE": "#F59E0B", "DEFAULT": "#EF4444"}
        
                fig_pie = px.pie(
                    status_data, names="status", values="count",
                    hole=0.65, color="status", color_discrete_map=color_map
                )
                fig_pie.update_traces(
                    textposition="inside", textinfo="percent+label",
                    hovertemplate="<b>%{label}</b><br>Loans: %{value}<br>Share: %{percent}<extra></extra>"
                )
                fig_pie.update_layout(
                    height=320, margin=dict(l=10, r=10, t=20, b=10),
                    annotations=[dict(text=f"{total_loans}<br>Total", x=0.5, y=0.5, font_size=18, showarrow=False)]
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No portfolio data available.")
        except Exception as e:
            st.info(f"Portfolio chart unavailable: {e}")

    # --- 5. ACTIVITY FEEDS ---
    st.write("---")
    t1, t2 = st.columns(2)

    with t1:
        st.markdown("#### 📊 Monthly Lending vs Interest")
        try:
            graph_df = loans_df.copy()
            graph_df["date_dt"] = safe_date(graph_df, ["start_date", "created_at"])
            graph_df = graph_df.dropna(subset=["date_dt"])
    
            if not graph_df.empty:
                graph_df["month"] = graph_df["date_dt"].dt.to_period("M").dt.to_timestamp()
                timeline_df = graph_df.groupby("month")[["principal_n", "interest_n"]].sum().reset_index().sort_values("month")
                timeline_df.rename(columns={"principal_n": "Loans Issued", "interest_n": "Interest Expected"}, inplace=True)
                
                fig_portfolio = px.line(
                    timeline_df, x="month", y=["Loans Issued", "Interest Expected"],
                    template="plotly_white", markers=True,
                    color_discrete_map={"Loans Issued": brand_color, "Interest Expected": "#10B981"}
                )
                fig_portfolio.update_layout(height=350, hovermode="x unified", xaxis_title="", yaxis_title="amount (UGX)", legend_title="")
                st.plotly_chart(fig_portfolio, use_container_width=True)
            else:
                st.info("Not enough dated records to generate a trend.")
        except Exception as e:
            st.info(f"Growth chart unavailable: {e}")

    with t2:
        st.markdown("### 💸 Latest Expenses")
        try:
            if not expenses_df.empty:
                df = expenses_df.copy()
                df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df = df.sort_values("date", ascending=False)
                latest = df.head(5)
                
                total = latest["amount"].sum()
                avg = latest["amount"].mean()
                count = len(latest)
        
                k1, k2, k3 = st.columns(3)
                k1.markdown(f"""<div style="background:#FEE2E2; padding:16px; border-radius:12px;"><div style="font-size:12px; color:#991B1B;">Total (Top 5)</div><div style="font-size:22px; font-weight:700; color:#B91C1C;">UGX {total:,.0f}</div></div>""", unsafe_allow_html=True)
                k2.markdown(f"""<div style="background:#E0F2FE; padding:16px; border-radius:12px;"><div style="font-size:12px; color:#075985;">Average</div><div style="font-size:22px; font-weight:700; color:#0369A1;">UGX {avg:,.0f}</div></div>""", unsafe_allow_html=True)
                k3.markdown(f"""<div style="background:#ECFDF5; padding:16px; border-radius:12px;"><div style="font-size:12px; color:#065F46;">Entries</div><div style="font-size:22px; font-weight:700; color:#047857;">{count}</div></div>""", unsafe_allow_html=True)
        
                st.divider()
                display_df = latest.copy()
                display_df["category"] = display_df["category"].fillna("General")
                display_df["date"] = display_df["date"].dt.strftime("%Y-%m-%d")
                final_df = display_df[["category", "amount", "date"]]
        
                def style_amount(val): return "color: #EF4444; font-weight: 600;"
                styled_df = final_df.style.format({"amount": "UGX {:,.0f}"}).map(style_amount, subset=["amount"])
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
            else:
                st.info("No recorded expenses.")
        except Exception as e:
            st.error(f"Expenses feed error: {e}")

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
                label="⬇️ Export Expenses CSV",
                data=csv2,
                file_name="expenses_report.csv",
                mime="text/csv",
                use_container_width=True
            )


# ==============================
# 🚀 borrowers ENGINE (PRODUCTION)
# ==============================

def show_borrowers():

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
    borrowers_df = safe_df(get_cached_data("borrowers"))
    loans_df = safe_df(get_cached_data("loans"))
    # Force lowercase column names for consistency
    for df in [borrowers_df, loans_df]:
        if not df.empty:
            df.columns = df.columns.str.strip().str.lower()

    # Apply Tenant Filters
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
                        "status": "Active", "tenant_id": str(st.session_state.get("tenant_id"))
                    }])
                
                    if save_data_saas("borrowers", new_entry):
                        st.write("Saving to tenant:", st.session_state.get("tenant_id"))
                        st.success(f"✅ {name} registered successfully!")
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.error("⚠️ Full name and Phone Number are required.")

    with tab_view:

        st.markdown("### 👥 Borrowers Registry")
    
        search = st.text_input("🔍 Search by name or phone...").lower()
    
        if not borrowers_df.empty:
    
            df = borrowers_df.copy()
    
            # --- Clean types ---
            for col in ["name", "phone", "national_id", "next_of_kin", "status"]:
                df[col] = df[col].astype(str)
    
            # --- Attach risk info safely ---
            def get_risk_label(b_id):
                r = risk_map.get(str(b_id), {})
                return r.get("risk_label", "🟢 Healthy")
    
            df["Risk Status"] = df["id"].apply(get_risk_label)
    
            # --- Search filter ---
            df_filtered = df[
                df["name"].str.lower().str.contains(search, na=False) |
                df["phone"].str.contains(search, na=False)
            ]
    
            if not df_filtered.empty:
    
                # --- Color mapping (no HTML needed) ---
                def style_risk(val):
                    if "🔴" in val:
                        return "color: #EF4444; font-weight:700;"
                    elif "🟠" in val:
                        return "color: #F97316; font-weight:700;"
                    elif "🟡" in val:
                        return "color: #F59E0B; font-weight:700;"
                    else:
                        return "color: #10B981; font-weight:700;"
    
                # --- Display table ---
                display_df = df_filtered[[
                    "name",
                    "phone",
                    "national_id",
                    "next_of_kin",
                    "Risk Status",
                    "status"
                ]].copy()
    
                display_df.columns = [
                    "Borrower Name",
                    "Phone",
                    "National ID",
                    "Next of Kin",
                    "Risk Status",
                    "Status"
                ]
    
                # --- Make status uppercase ---
                display_df["Status"] = display_df["Status"].str.upper()
    
                # --- Interactive table ---
                styled_df = display_df.style.map(
                    style_risk,
                    subset=["Risk Status"]
                )
    
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    hide_index=True
                )
    
                # --- Select borrower (kept your logic) ---
                st.markdown("### 🎯 Management Actions")
    
                selected_name = st.selectbox(
                    "Select borrower:",
                    ["-- Choose borrower --"] + df_filtered["name"].tolist()
                )
    
                if selected_name != "-- Choose borrower --":
                    sel_id = df_filtered[df_filtered["name"] == selected_name]["id"].values[0]
                    st.session_state["selected_borrower"] = sel_id
    
                    st.success(f"Selected: {selected_name}")
    
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
                            "start_date": st.column_config.dateColumn("date Issued"),
                            "end_date": st.column_config.dateColumn("Due date"),
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
    df = get_cached_data(table_name)

    if df is None:
        return pd.DataFrame()

    if df.empty:
        return df

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    if "tenant_id" in df.columns:
        df["tenant_id"] = df["tenant_id"].astype(str).str.strip()

        df = df[df["tenant_id"] == tenant_id].copy()

    return df.reset_index(drop=True)

def save_data_saas(table_name, df):
    tenant_id = get_current_tenant()

    if tenant_id:
        df["tenant_id"] = str(tenant_id)

    return save_data(table_name, df)


# ==============================
# 13. LOANS MANAGEMENT PAGE
# ==============================
def show_loans():

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
            "id",
            "sn",
            "loan_id_label",
            "parent_loan_id",
            "borrower_id",
            "borrower",
            "loan_type",
            "principal",
            "interest",
            "total_repayable",
            "amount_paid",
            "balance",
            "status",
            "start_date",
            "end_date",
            "cycle_no",
            "tenant_id"
        ])

    if borrowers_df is None:
        borrowers_df = pd.DataFrame()

    if payments_df is None:
        payments_df = pd.DataFrame()

    # ------------------------------
    # REQUIRED DEFAULTS
    # ------------------------------
    required_defaults = {
        "id": "",
        "sn": "",
        "loan_id_label": "",
        "parent_loan_id": "",
        "borrower_id": "",
        "borrower": "",
        "loan_type": "",
        "principal": 0.0,
        "interest": 0.0,
        "total_repayable": 0.0,
        "amount_paid": 0.0,
        "balance": 0.0,
        "status": "ACTIVE",
        "start_date": "",
        "end_date": "",
        "cycle_no": 1,
        "tenant_id": ""
    }

    for col, val in required_defaults.items():
        if col not in loans_df.columns:
            loans_df[col] = val

    loans_df = loans_df.copy()
    payments_df = payments_df.copy()

    # ------------------------------
    # type CLEANUP
    # ------------------------------
    loans_df["id"] = loans_df["id"].astype(str)
    loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)
    loans_df["parent_loan_id"] = loans_df["parent_loan_id"].fillna("").astype(str)

    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].astype(str)

    # ------------------------------
    # NUMERIC CLEANUP
    # ------------------------------
    for col in [
        "principal",
        "interest",
        "total_repayable",
        "amount_paid",
        "balance"
    ]:
        loans_df[col] = pd.to_numeric(loans_df[col], errors="coerce").fillna(0)

    if not payments_df.empty and "amount" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(
            payments_df["amount"], errors="coerce"
        ).fillna(0)

    # ------------------------------
    # date CLEANUP
    # ------------------------------
    for col in ["start_date", "end_date"]:
        loans_df[col] = pd.to_datetime(loans_df[col], errors="coerce")

    # ------------------------------
    # PAYMENT SYNC
    # ------------------------------
    loans_df["amount_paid"] = 0  # ✅ ensure column always exists

    if not payments_df.empty and "loan_id" in payments_df.columns:
        pay_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(pay_sums).fillna(0)

    loans_df["balance"] = (
        loans_df["total_repayable"] - loans_df["amount_paid"]
    ).clip(lower=0)

    # ==============================
    # SERIAL ENGINE (MOVE UP)
    # ==============================
    existing_nums = []
    
    # ------------------------------
    # 🔒 NORMALIZE EXISTING SNs
    # ------------------------------
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip()
    
    # Map existing SNs (immutability protection)
    existing_sn_map = dict(zip(loans_df["id"], loans_df["sn"]))
    
    for val in loans_df["sn"]:
        val = val.strip()
        if val.startswith("LN-"):
            try:
                existing_nums.append(int(val.replace("LN-", "")))
            except:
                pass
    
    next_sn_val = max(existing_nums, default=0)
    
    # ------------------------------
    # 🔁 SAFE SN ASSIGNMENT
    # ------------------------------
    for i in loans_df.index:
    
        current_id = loans_df.at[i, "id"]
    
        # 🔒 NEVER touch already valid SN
        existing_sn = str(existing_sn_map.get(current_id, "")).strip()
        if existing_sn.startswith("LN-"):
            continue
    
        parent_id = str(loans_df.at[i, "parent_loan_id"]).strip()
    
        inherited_sn = ""
    
        # 🔗 WALK FULL LINEAGE (not just direct parent)
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
    
        # ✅ APPLY INHERITED SN
        if inherited_sn:
            loans_df.at[i, "sn"] = inherited_sn
    
        # 🆕 CREATE NEW SN ONLY IF STILL MISSING
        if not str(loans_df.at[i, "sn"]).startswith("LN-"):
            next_sn_val += 1
            loans_df.at[i, "sn"] = f"LN-{next_sn_val:04d}"
    
    # ✅ SORT BEFORE ASSIGNING CYCLES (Ensures Parent is Cycle 1)
    loans_df = loans_df.sort_values(by=["sn", "start_date", "id"])
    
    loans_df["cycle_no"] = (
        loans_df.groupby("sn").cumcount() + 1
    )
    
    # ------------------------------
    # REVISED SMART STATUS LOGIC (V2)
    # ------------------------------
    
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip().str.upper()
    
    # 1. Sort to ensure chronological order
    loans_df = loans_df.sort_values(by=["sn", "cycle_no", "start_date"])
    
    # 2. Process each loan family
    for sn_val, grp in loans_df.groupby("sn"):
        
        indices = grp.index.tolist()
        latest_idx = indices[-1]
        
        # 3. Mark all rows EXCEPT the last one as BCF
        # (Because a newer cycle exists, these are inherently "Brought Forward")
        if len(indices) > 1:
            loans_df.loc[indices[:-1], "status"] = "BCF"
        
        # 4. Handle the Latest Cycle
        latest_row = loans_df.loc[latest_idx]
        
        # Check if balance is effectively zero (handling float rounding)
        if abs(latest_row["balance"]) < 1.0: 
            loans_df.at[latest_idx, "status"] = "CLEARED"
        else:
            # If there's a balance, determine if it's a fresh loan or a rollover
            if int(latest_row["cycle_no"]) == 1:
                loans_df.at[latest_idx, "status"] = "ACTIVE"
            else:
                loans_df.at[latest_idx, "status"] = "PENDING"
    
    # ------------------------------
    # FINAL SAFETY OVERRIDE
    # ------------------------------
    # If ANY row (even a middle one) has 0 balance, it cannot be PENDING or ACTIVE.
    # It's either BCF (if a newer cycle exists) or CLEARED.
    # This rule forces any 0 balance "Pending" rows to "Cleared".
    
    mask_zero = (loans_df["balance"] <= 0) & (loans_df["status"] != "BCF")
    loans_df.loc[mask_zero, "status"] = "CLEARED"
    
    # ------------------------------
    # FINAL RULE: FORCE CLEARED STATE
    # ------------------------------
    # Any loan with balance = 0 is ALWAYS CLEARED
    loans_df.loc[
        loans_df["balance"] <= 0,
        "status"
    ] = "CLEARED"
    
    # ------------------------------
    # FINAL SORT
    # ------------------------------
    loans_df = loans_df.sort_values(
        by=["sn", "cycle_no"],
        ascending=[True, True]
    ).reset_index(drop=True)
    
    # ------------------------------
    # LABELS
    # ------------------------------
    loans_df["loan_id_label"] = (
        loans_df["sn"]
        .str.replace("LN-", "", regex=False)
        .str.zfill(4)
    )
    

    # ==============================
    # 🔄 DATABASE SYNC ENGINE (OPTIMIZED)
    # ==============================
    # 1. Fetch the raw data from cache to see what's actually in the DB right now
    raw_db_df = get_cached_data("loans")
    
    # 2. Identify only rows where our calculated SN/Cycle differs from the DB
    def needs_update(row):
        db_match = raw_db_df[raw_db_df["id"] == row["id"]]
        if db_match.empty:
            return True
        db_row = db_match.iloc[0]
        # Only sync if SN, Label, or Cycle has changed or is missing in DB
        return (str(db_row.get("sn", "")) != str(row["sn"]) or 
                str(db_row.get("loan_id_label", "")) != str(row["loan_id_label"]) or
                int(db_row.get("cycle_no", 0)) != int(row["cycle_no"]))

    # 3. Filter to_sync to only include actual changes
    to_sync = loans_df[loans_df.apply(needs_update, axis=1)]
    
    if not to_sync.empty:
        # Use st.status for a cleaner look than st.spinner
        with st.status("🔄 Syncing Serial Numbers to Database...", expanded=False) as status:
            for _, row in to_sync.iterrows():
                sync_data = {
                    "sn": row["sn"],
                    "loan_id_label": row["loan_id_label"],
                    "cycle_no": int(row["cycle_no"])
                }
                try:
                    supabase.table("loans").update(sync_data).eq("id", row["id"]).execute()
                except Exception as e:
                    st.error(f"Error syncing row {row['id']}: {e}")
            
            # Clear cache so the next run sees the updated data
            st.cache_data.clear()
            status.update(label="✅ Database Serial Numbers Synced!", state="complete", expanded=False)
            st.rerun()
    # ------------------------------
    # borrower MAP
    # ------------------------------
    if not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(
            borrowers_df["id"],
            borrowers_df["name"]
        ))
        mapped_names = loans_df["borrower_id"].map(bor_map)

        loans_df["borrower"] = mapped_names.fillna(loans_df["borrower"]).fillna("Unknown")

    # ------------------------------
    # ACTIVE borrowerS
    # ------------------------------
    if not borrowers_df.empty and "status" in borrowers_df.columns:
        Active_borrowers = borrowers_df[
            borrowers_df["status"]
            .astype(str)
            .str.upper() == "ACTIVE"
        ]
    else:
        Active_borrowers = pd.DataFrame(columns=["id", "name"])

    # ==============================
    # TABS
    # ==============================
    tab_view, tab_add, tab_manage, tab_actions = st.tabs([
        "📑 Portfolio View",
        "➕ New Loan",
        "🛠️ Manage/Edit",
        "⚙️ Actions"
    ])

    # ==============================
    # TAB VIEW
    # ==============================
    with tab_view:
        search_query = st.text_input(
            "🔍 Search Loan / borrower",
            key="loan_search_main"
        )

        # Create a local copy for filtering
        filtered_loans = loans_df.copy() if not loans_df.empty else pd.DataFrame()

        if not filtered_loans.empty and search_query:
            filtered_loans = filtered_loans[
                filtered_loans.apply(
                    lambda r: search_query.lower() in str(r).lower(),
                    axis=1
                )
            ]
        # ------------------------------
        # 📊 PORTFOLIO METRICS
        # ------------------------------
        if not filtered_loans.empty:

            total_loans = filtered_loans["sn"].nunique()
            original_loans = filtered_loans[filtered_loans["cycle_no"] == 1]  
            total_principal = original_loans["principal"].sum()
            total_repayable = filtered_loans["total_repayable"].sum()
            total_paid = filtered_loans["amount_paid"].sum()
            
            # --- NEW CALCULATION ---
            total_pending = filtered_loans[filtered_loans["status"] == "PENDING"]["total_repayable"].sum()

            col1, col2, col3, col4 = st.columns(4)

            col1.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #3b82f6, #1e3a8a);
                padding:15px;
                border-radius:10px;
                color:white;
                text-align:center;">
                <div style="font-size:14px;">📄 Total Loans</div>
                <div style="font-size:22px;font-weight:bold;">{total_loans}</div>
            </div>
            """, unsafe_allow_html=True)
            
            col2.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #10b981, #065f46);
                padding:15px;
                border-radius:10px;
                color:white;
                text-align:center;">
                <div style="font-size:14px;">💰 Principal</div>
                <div style="font-size:22px;font-weight:bold;">{total_principal:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)
            
            col3.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #f59e0b, #92400e);
                padding:15px;
                border-radius:10px;
                color:white;
                text-align:center;">
                <div style="font-size:14px;">💳 Paid</div>
                <div style="font-size:22px;font-weight:bold;">{total_paid:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)

            # --- NEW METRIC CARD ---
            col4.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #ef4444, #991b1b);
                padding:15px;
                border-radius:10px;
                color:white;
                text-align:center;">
                <div style="font-size:14px;">⏳ Total Pending</div>
                <div style="font-size:22px;font-weight:bold;">{total_pending:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("---")
        # ------------------------------
        # 📋 LOAN DATA TABLE
        # ------------------------------
        if filtered_loans.empty:
            st.warning("No matching loans found.")
        else:
            # ------------------------------
            # 🎯 Fiscal Year Engine (July–June FIXED)
            # ------------------------------
            if "fiscal_year" not in filtered_loans.columns:
                # Convert start_date to datetime
                start_dt = pd.to_datetime(filtered_loans.get("start_date"), errors="coerce")
                
                # Fill missing with created_at or today
                start_dt = start_dt.fillna(pd.to_datetime(filtered_loans.get("created_at", pd.Timestamp.today())))
                
                # Compute fiscal year (July–June)
                fiscal_years_list = []
                for dt in start_dt:
                    if dt.month >= 7:
                        fiscal_years_list.append(f"{dt.year}/{dt.year + 1}")
                    else:
                        fiscal_years_list.append(f"{dt.year - 1}/{dt.year}")
                
                filtered_loans["fiscal_year"] = fiscal_years_list
            
            # Build dropdown
            fy_unique = sorted(filtered_loans["fiscal_year"].dropna().unique().tolist())
            fy_selected = st.selectbox("Filter by Fiscal Year", ["All"] + fy_unique)
            
            # Filter loans if a specific FY is selected
            if fy_selected != "All":
                filtered_loans = filtered_loans[filtered_loans["fiscal_year"] == fy_selected]
        
            # Calculation update (Ensuring amount_paid reduces total_repayable)
            filtered_loans["balance"] = filtered_loans["total_repayable"] - filtered_loans["amount_paid"]
        
            show_cols = [
                "sn",
                "loan_id_label",
                "borrower",
                "cycle_no",
                "principal",
                "total_repayable",
                "amount_paid",
                "balance",
                "start_date",
                "end_date",
                "status"
            ]
            def style_entire_row(row):
                val = str(row["status"]).upper().strip()
                color_map = {
                    "ACTIVE": "background-color:#dbeafe;color:#1e40af;font-weight:bold;",
                    "PENDING": "background-color:#fee2e2;color:#991b1b;font-weight:bold;",
                    "CLEARED": "background-color:#d1fae5;color:#065f46;",
                    "BCF": "background-color:#ffedd5;color:#9a3412;",
                    "CLOSED": "background-color:#f3f4f6;color:#374151;"
                }
                style = color_map.get(val, "")
                return [style] * len(row)
        
            # Apply styling and currency formatting
            styled_df = (
                filtered_loans[show_cols].style
                .apply(style_entire_row, axis=1)
                .format({
                    "principal": "{:,.0f}",
                    "amount_paid": "{:,.0f}",
                    "total_repayable": "{:,.0f}",
                    "balance": "{:,.0f}"
                })
            )
        
            st.dataframe(
                styled_df,
                column_order=show_cols,
                use_container_width=True,
                hide_index=True
            )
            
    # ==============================       
    # TAB ADD LOAN
    # ==============================
    with tab_add:

        if Active_borrowers.empty:

            st.info("💡 Tip: Activate borrower first.")

        else:

            with st.form("loan_issue_form_v2"):

                st.markdown(
                    "<h4 style='color:#0A192F;'>📝 Create New Loan Agreement</h4>",
                    unsafe_allow_html=True
                )

                col1, col2 = st.columns(2)

                borrower_map = dict(
                    zip(
                        Active_borrowers["name"],
                        Active_borrowers["id"]
                    )
                )

                selected_name = col1.selectbox(
                    "Select borrower",
                    list(borrower_map.keys())
                )

                selected_id = str(
                    borrower_map[selected_name]
                ).strip()

                amount = col1.number_input(
                    "Principal amount (UGX)",
                    min_value=0,
                    step=50000
                )

                date_issued = col1.date_input(
                    "Start date",
                    value=datetime.now()
                )

                loan_type = col2.selectbox(
                    "Loan type",
                    ["Business", "Personal", "Emergency", "Other"]
                )

                interest_rate = col2.number_input(
                    "Monthly Interest Rate (%)",
                    min_value=0.0,
                    step=0.5
                )

                date_due = col2.date_input(
                    "Due date",
                    value=date_issued + timedelta(days=30)
                )

                total_due = amount + (
                    amount * interest_rate / 100
                )

                st.info(
                    f"Preview: Total Repayable {total_due:,.0f} UGX"
                )

                submit = st.form_submit_button(
                    "🚀 Confirm & Issue Loan"
                )

                if submit:

                    tenant_id = get_current_tenant()

                    if not tenant_id:
                        st.error("Tenant session missing.")
                        st.stop()

                    if selected_id == "":
                        st.error("borrower ID missing.")
                        st.stop()

                    loan_data = {
                        "id": str(uuid.uuid4()),
                        "sn": "",
                        "loan_id_label": "",
                        "parent_loan_id": None,
                        "borrower_id": selected_id,
                        "borrower": selected_name,
                        "loan_type": loan_type,
                        "principal": float(amount),
                        "interest": float(
                            amount * interest_rate / 100
                        ),
                        "total_repayable": float(total_due),
                        "amount_paid": 0.0,
                        "balance": float(total_due),
                        "status": "ACTIVE",
                        "start_date": str(date_issued),
                        "end_date": str(date_due),
                        "cycle_no": 1,
                        "tenant_id": tenant_id
                    }

                    if save_data(
                        "loans",
                        pd.DataFrame([loan_data])
                    ):
                        st.success("✅ Loan issued.")
                        st.cache_data.clear()
                        st.session_state.pop("loans", None)
                        st.rerun()
    # ==============================
    # TAB ACTIONS
    # ==============================
    with tab_actions:
    
        st.markdown(
            "<h4 style='color: #0A192F;'>🔄 Multi-Stage Loan Rollover</h4>",
            unsafe_allow_html=True
        )
    
        eligible_loans = loans_df[
            (~loans_df["status"].isin(["CLEARED"])) &
            (loans_df["balance"] > 0)
        ]
    
        if eligible_loans.empty:
            st.success("All loans brought up to date! ✨")
    
        else:
            roll_map = {
                f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']} • Bal {row['balance']:,.0f}":
                row["id"]
                for _, row in eligible_loans.iterrows()
            }
    
            roll_sel = st.selectbox(
                "Select Loan to Roll Forward",
                list(roll_map.keys())
            )
    
            parent_id = roll_map[roll_sel]
    
            loan_to_roll = eligible_loans[
                eligible_loans["id"] == parent_id
            ].iloc[0]
    
            new_interest_rate = st.number_input(
                "New Monthly Interest (%)",
                value=3.0,
                step=0.5
            )
    
            if st.button(
                "🔥 Execute Next Rollover",
                use_container_width=True
            ):
    
                old_due = pd.to_datetime(
                    loan_to_roll["end_date"],
                    errors="coerce"
                )
    
                if pd.isna(old_due):
                    old_due = datetime.now()
    
                new_start = old_due
                new_due = old_due + timedelta(days=30)
    
                # --- Corrected Indentation for Status Check ---
                current_status = str(
                    loan_to_roll["status"]
                ).strip().upper()
    
                # Only pending loans become BCF when pushed forward
                if current_status == "PENDING":
                    loans_df.loc[
                        loans_df["id"] == parent_id,
                        "status"
                    ] = "BCF"
    
                save_data_saas("loans", loans_df)
                # ----------------------------------------------
    
                # This 'unpaid' value is (Old Total Repayable - Old amount Paid)
                unpaid = float(
                    loan_to_roll["balance"]
                )
    
                new_interest = unpaid * (
                    new_interest_rate / 100
                )
    
                new_row = {
                    "id": str(uuid.uuid4()),
                    "sn": "",  # Handled by your serial engine
                    "loan_id_label": "",
                    "parent_loan_id": parent_id,
                    "borrower_id": loan_to_roll["borrower_id"],
                    "loan_type": loan_to_roll["loan_type"],
                    "principal": unpaid, # ✅ New Principal is Old Principal + Old Interest - Payments
                    "interest": new_interest,
                    "total_repayable": unpaid + new_interest,
                    "amount_paid": 0.0,
                    "balance": unpaid + new_interest,
                    "status": "PENDING",
                    "start_date": str(new_start.date()),
                    "end_date": str(new_due.date()),
                    "cycle_no": int(
                        loan_to_roll["cycle_no"]
                    ) + 1,
                    "tenant_id": get_current_tenant()
                }
    
                if save_data(
                    "loans",
                    pd.DataFrame([new_row])
                ):
                    st.success("✅ Loan rolled forward.")
                    st.cache_data.clear()
                    st.rerun()

    # ==============================
    # TAB MANAGE
    # ==============================
    with tab_manage:

        if not loans_df.empty:

            edit_map = {
                f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']}":
                row["id"]
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

            with st.form(f"edit_form_{target_id}"):

                e_princ = st.number_input(
                    "Principal",
                    value=float(
                        loan_to_edit["principal"]
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
                ).upper()

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

                if st.form_submit_button(
                    "💾 Save Changes"
                ):

                    supabase.table("loans").update({
                        "principal": e_princ,
                        "status": e_stat
                    }).eq(
                        "id",
                        target_id
                    ).execute()

                    st.success("✅ Updated!")
                    st.cache_data.clear()
                    st.rerun()

            if st.button(
                "🗑️ Delete Loan Permanently",
                use_container_width=True
            ):

                supabase.table("loans").delete().eq(
                    "id",
                    target_id
                ).execute()

                st.warning("Loan Deleted.")
                st.cache_data.clear()
                st.rerun()


# ==============================
# 🧾 RECEIPT GENERATION
# ==============================
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from io import BytesIO
from datetime import datetime
def generate_receipt_pdf(data, filename):
    doc = SimpleDocTemplate(filename)
    styles = getSampleStyleSheet()
    content = []
    content.append(Paragraph("<b>PAYMENT RECEIPT</b>", styles["Title"]))
    content.append(Spacer(1, 12))
    for k, v in data.items():
        content.append(Paragraph(f"<b>{k}:</b> {v}", styles["Normal"]))
        content.append(Spacer(1, 8))
    doc.build(content)

# ✅ SINGLE SOURCE OF TRUTH (RPC)
def generate_receipt_no(supabase, tenant_id):
    try:
        res = supabase.rpc("get_next_receipt", {"p_tenant": tenant_id}).execute()
        return res.data
    except Exception as e:
        st.error(f"Receipt generation failed: {e}")
        return f"RCPT-{datetime.now().strftime('%Y%m%d%H%M%S')}"

# ==============================
# 💵 PAYMENTS MODULE (CYCLE-AWARE)
# ==============================
def show_payments():
    st.markdown("## 💵 Payments Management")

    try:
        loans_raw = get_cached_data("loans")
        payments_raw = get_cached_data("payments")
        borrowers_raw = get_cached_data("borrowers")
    except Exception as e:
        st.error(f"❌ Data load error: {e}")
        return

    loans_df = pd.DataFrame(loans_raw) if loans_raw is not None else pd.DataFrame()
    payments_df = pd.DataFrame(payments_raw) if payments_raw is not None else pd.DataFrame()
    borrowers_df = pd.DataFrame(borrowers_raw) if borrowers_raw is not None else pd.DataFrame()

    if loans_df.empty:
        st.info("ℹ️ No loans available.")
        return

    # Normalize
    for df in [loans_df, payments_df, borrowers_df]:
        if not df.empty:
            df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

    # Ensure IDs
    for df, col in [(borrowers_df, "id"), (loans_df, "borrower_id"), (loans_df, "id"), (payments_df, "loan_id")]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # borrower mapping
    if not borrowers_df.empty and "name" in borrowers_df.columns:
        borrower_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].map(borrower_map).fillna("Unknown")
    else:
        loans_df["borrower"] = "Unknown"

    # Numeric fields
    for col in ["total_repayable", "amount_paid", "balance", "principal", "interest"]:
        if col in loans_df.columns:
            loans_df[col] = pd.to_numeric(loans_df[col].fillna(0), errors="coerce")

    if not payments_df.empty and "amount" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(payments_df.get("amount", 0), errors="coerce").fillna(0)

    # Map total payments to loans
    if not payments_df.empty:
        payment_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(payment_sums).fillna(0)
    else:
        loans_df["amount_paid"] = 0

    # ------------------------------
    # CYCLE-AWARE CASCADE FUNCTION
    # ------------------------------
    def cascade_payment(loans_df, sn, changed_cycle_no):
        # Get all cycles of this SN sorted by cycle_no
        cycles = loans_df[loans_df["sn"] == sn].sort_values("cycle_no").reset_index()
        for idx, row in cycles.iterrows():
            # Skip cycles before the changed cycle
            if row["cycle_no"] <= changed_cycle_no:
                continue
            prev_balance = cycles.loc[idx - 1, "balance"]
            prev_interest = row["interest"]
            # New principal = previous balance
            loans_df.loc[row["index"], "principal"] = prev_balance
            # Total repayable = principal + interest
            loans_df.loc[row["index"], "total_repayable"] = prev_balance + prev_interest
            # Recalculate balance = total repayable - amount_paid
            loans_df.loc[row["index"], "balance"] = loans_df.loc[row["index"], "total_repayable"] - loans_df.loc[row["index"], "amount_paid"]

    # ==============================
    # 🔥 GET ACTIVE LOAN
    # ==============================
    def get_active_loan(loans_df, loan_row):
        current = loan_row
        visited = set()
        while True:
            if current["id"] in visited:
                break
            visited.add(current["id"])
            child = loans_df[loans_df["parent_loan_id"] == current["id"]]
            if child.empty:
                return current
            current = child.iloc[0]
        return current

    # ==============================
    # 📑 TABS
    # ==============================
    tab1, tab2 = st.tabs(["➕ Record Payment", "📜 History"])

    # ==============================
    # ➕ RECORD PAYMENT
    # ==============================
    with tab1:
        active_loans = loans_df.copy()

        def format_loan(row):
            balance = row["total_repayable"] - row["amount_paid"]
            sn = row.get("loan_id_label") or row.get("sn") or "N/A"
            return f"{row['borrower']} | SN: {sn} | BAL: UGX {balance:,.0f}"

        active_loans["label"] = active_loans.apply(format_loan, axis=1)

        selected_index = st.selectbox(
            "Select Loan",
            active_loans.index,
            format_func=lambda i: active_loans.loc[i, "label"]
        )

        loan = active_loans.loc[selected_index]

        # 🔥 CRITICAL: Get active cycle for this loan
        active_loan = get_active_loan(loans_df, loan)
        loan_id = active_loan["id"]
        balance = active_loan["total_repayable"] - active_loan["amount_paid"]

        st.info(f"Active Loan Used: {active_loan['borrower']} (ID: {loan_id[:6]})")
        st.metric("Balance", f"UGX {balance:,.0f}")

        with st.form("payment_form"):
            amount = st.number_input("amount", min_value=0.0, step=1000.0)
            method = st.selectbox("Method", ["Cash", "Mobile Money", "Bank"])
            date = st.date_input("date", datetime.now())
            submit = st.form_submit_button("Post Payment")

        if submit:
            if amount <= 0:
                st.warning("Enter valid amount")
                return

            try:
                tenant_id = st.session_state.get("tenant_id")

                # ✅ SINGLE SOURCE OF TRUTH
                receipt_no = generate_receipt_no(supabase, tenant_id)

                # 1️⃣ Insert payment
                supabase.table("payments").insert({
                    "receipt_no": receipt_no,
                    "loan_id": loan_id,
                    "borrower": active_loan["borrower"],
                    "amount": float(amount),
                    "date": date.strftime("%Y-%m-%d"),
                    "method": method,
                    "tenant_id": tenant_id
                }).execute()

                # 2️⃣ Update this cycle locally
                loans_df.loc[loans_df["id"] == loan_id, "amount_paid"] += amount
                loans_df.loc[loans_df["id"] == loan_id, "balance"] = loans_df.loc[loans_df["id"] == loan_id, "total_repayable"] - loans_df.loc[loans_df["id"] == loan_id, "amount_paid"]

                # 3️⃣ Cascade to subsequent cycles
                sn = active_loan["sn"]
                changed_cycle_no = int(active_loan["cycle_no"])
                cascade_payment(loans_df, sn, changed_cycle_no)

                # 4️⃣ Save updated loan table
                save_data_saas("loans", loans_df)

                # 5️⃣ Generate receipt PDF
                file_path = f"/tmp/{receipt_no}.pdf"
                generate_receipt_pdf({
                    "Receipt No": receipt_no,
                    "borrower": active_loan["borrower"],
                    "amount": f"UGX {amount:,.0f}",
                    "Method": method,
                    "date": date.strftime("%Y-%m-%d"),
                }, file_path)

                with open(file_path, "rb") as f:
                    st.download_button("📥 Download Receipt", f, file_name=f"{receipt_no}.pdf")

                st.success(f"✅ Payment posted. New Balance: UGX {loans_df.loc[loans_df['id'] == loan_id, 'balance'].values[0]:,.0f}")

                st.cache_data.clear()
                st.rerun()

            except Exception as e:
                st.error(f"❌ Error: {e}")

    # ==============================
    # 📜 HISTORY
    # ==============================
    with tab2:
        if payments_df.empty:
            st.info("No payment history")
        else:
            payments_df["amount_display"] = payments_df["amount"].apply(lambda x: f"UGX {x:,.0f}")
            payments_df["id"] = payments_df["id"].astype(str)
            payments_df["receipt_no"] = payments_df["receipt_no"].fillna("No Receipt")
            display_cols = ["date", "borrower", "amount_display", "method", "receipt_no"]
            st.dataframe(payments_df[display_cols], use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown("### ⚙️ Payment Maintenance")

            pay_map = {
                f"{row['receipt_no']} | {row['borrower']} | {row['amount_display']}": row['id']
                for _, row in payments_df.iterrows()
            }

            selected_pay_label = st.selectbox("Choose Payment to Modify", list(pay_map.keys()))
            target_pay_id = pay_map[selected_pay_label]
            target_pay = payments_df[payments_df['id'] == target_pay_id].iloc[0]

            p_col1, p_col2 = st.columns(2)

            if p_col1.button("🗑️ Delete Payment", use_container_width=True):
                try:
                    supabase.table("payments").delete().eq("id", target_pay_id).execute()
                    # Recompute cascade if needed
                    loan_id = target_pay["loan_id"]
                    affected_loan = loans_df[loans_df["id"] == loan_id].iloc[0]
                    cascade_payment(loans_df, affected_loan["sn"], int(affected_loan["cycle_no"]))
                    save_data_saas("loans", loans_df)
                    st.cache_data.clear()
                    st.warning(f"Payment {target_pay['receipt_no']} removed and cascade updated.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")

            if p_col2.button("📝 Edit Payment", use_container_width=True):
                st.session_state["edit_pay_mode"] = True

            if st.session_state.get("edit_pay_mode"):
                with st.form("edit_payment_form"):
                    st.info(f"Modifying: {target_pay['receipt_no']}")
                    new_amt = st.number_input("Revised amount", value=float(target_pay['amount']))
                    current_method = target_pay['method']
                    method_options = ["Cash", "Mobile Money", "Bank"]
                    method_idx = method_options.index(current_method) if current_method in method_options else 0
                    new_method = st.selectbox("Revised Method", method_options, index=method_idx)
                    eb1, eb2 = st.columns(2)

                    if eb1.form_submit_button("💾 Save Changes"):
                        try:
                            supabase.table("payments").update({
                                "amount": new_amt,
                                "method": new_method
                            }).eq("id", target_pay_id).execute()
                            # Recompute cascade after edit
                            loan_id = target_pay["loan_id"]
                            affected_loan = loans_df[loans_df["id"] == loan_id].iloc[0]
                            cascade_payment(loans_df, affected_loan["sn"], int(affected_loan["cycle_no"]))
                            save_data_saas("loans", loans_df)
                            st.session_state["edit_pay_mode"] = False
                            st.cache_data.clear()
                            st.success("Payment updated successfully and cascade applied!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Update failed: {e}")

                    if eb2.form_submit_button("❌ Cancel"):
                        st.session_state["edit_pay_mode"] = False
                        st.rerun()


def format_with_commas(df):
    if df.empty:
        return df

    df = df.copy()
    numeric_cols = df.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        df[col] = df[col].apply(lambda x: f"{int(x):,}" if pd.notnull(x) else "")
    return df
# =================================
# 🏢 Enterprise Payroll Engine (Clean + Excel Export)
# =================================

import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from io import BytesIO
def delete_data_saas(table_name, filters):
    """
    Deletes a record from Supabase based on a filter (e.g., payroll_id).
    """
    try:
        # Assuming 'supabase' is your initialized client in database.py
        response = supabase.table(table_name).delete().match(filters).execute()
        return True
    except Exception as e:
        st.error(f"Database Error: {e}")
        return False
def export_styled_excel(df, company="ZOE CONSULTS SMC LTD"):
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter
    from datetime import datetime
    from io import BytesIO

    wb = Workbook()
    ws = wb.active
    ws.title = "Payroll"

    # -----------------------------
    # Styles
    # -----------------------------
    blue = PatternFill("solid", fgColor="4A90E2")
    white_font = Font(color="FFFFFF", bold=True)
    bold = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    right = Alignment(horizontal="right")

    thin = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )

    # -----------------------------
    # Title
    # -----------------------------
    ws.merge_cells("A1:W1")
    ws["A1"] = f"{datetime.now().strftime('%B %Y').upper()} PAYROLL ({company})"
    ws["A1"].font = Font(bold=True, size=14)
    ws["A1"].alignment = center

    # -----------------------------
    # Header Row 1
    # -----------------------------
    ws.append([
        "S/NO","Employee Name","TIN","Designation","Mob No",
        "Account No","NSSF No",
        "Salary","Basic",
        "NO PAY","",
        "LST",
        "Gross Salary",
        "Deductions",
        "If yes calculate","","",
        "Other",
        "Total Deductions",
        "Nett Pay",
        "Total Tax",
        "10% NSSF",
        "15% NSSF"
    ])

    ws.merge_cells("J2:K2")
    ws.merge_cells("O2:Q2")

    # -----------------------------
    # Header Row 2
    # -----------------------------
    ws.append([
        "No","","","","","","",
        "ARREARS","Salary",
        "DAYS","Absenteeism",
        "Deductions",
        "",
        "P.A.Y.E","N.S.S.F","S.DRS/ADV",
        "Deduction",
        "",
        "",
        "",
        "",
        ""
    ])

    # Style headers
    for row in ws.iter_rows(min_row=2, max_row=3, min_col=1, max_col=23):
        for cell in row:
            cell.fill = blue
            cell.font = white_font
            cell.alignment = center
            cell.border = thin

    # -----------------------------
    # Data
    # -----------------------------
    for i, r in df.iterrows():
        ws.append([
            i+1,
            r["employee"],
            r["tin"],
            r["designation"],
            r["mob_no"],
            r["account_no"],
            r["nssf_no"],
            r["arrears"],
            r["basic_salary"],
            0,
            r["absent_deduction"],
            r["lst"],
            r["gross_salary"],
            r["paye"],
            r["paye"],
            r["nssf_5"],
            r["advance_drs"],
            r["other_deductions"],
            r["paye"] + r["nssf_5"] + r["advance_drs"] + r["other_deductions"],
            r["net_pay"],
            r["paye"],
            r["nssf_10"],
            r["nssf_15"]
        ])

    # -----------------------------
    # Number formatting (DATA)
    # -----------------------------
    for row in ws.iter_rows(min_row=4, max_row=ws.max_row, min_col=8, max_col=23):
        for cell in row:
            cell.number_format = '#,##0'
            cell.alignment = right
            cell.border = thin

    # -----------------------------
    # Totals
    # -----------------------------
    total_row = ws.max_row + 1
    ws[f"A{total_row}"] = "TOTAL"

    for col in range(8, 24):
        letter = get_column_letter(col)
        ws[f"{letter}{total_row}"] = f"=SUM({letter}4:{letter}{total_row-1})"

    # Style totals row
    for col in range(1, 24):
        cell = ws[f"{get_column_letter(col)}{total_row}"]
        cell.font = bold
        cell.fill = blue
        cell.border = thin

    for col in range(8, 24):
        cell = ws[f"{get_column_letter(col)}{total_row}"]
        cell.number_format = '#,##0'
        cell.alignment = right

    # -----------------------------
    # Column widths
    # -----------------------------
    widths = [6,22,15,25,15,20,20] + [12]*16
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w

    # -----------------------------
    # Freeze header
    # -----------------------------
    ws.freeze_panes = "A4"

    # -----------------------------
    # Save to memory
    # -----------------------------
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    return buffer
# ---------------------------------
# Payroll Calculation
# ---------------------------------
def compute_payroll(basic, arrears, absent, advance, other, apply_lst=True):
    gross = round(float(basic) + float(arrears) - float(absent))

    # -----------------------------
    # NSSF
    # -----------------------------
    nssf_5 = round(gross * 0.05)   # Employee deduction
    nssf_10 = round(gross * 0.10)  # Employer (NOT deducted)
    nssf_15 = nssf_5 + nssf_10

    # -----------------------------
    # TAXABLE INCOME
    # -----------------------------
    taxable_income = gross - nssf_5

    # -----------------------------
    # PAYE (Uganda)
    # -----------------------------
    paye = 0
    if taxable_income <= 235000:
        paye = 0
    elif taxable_income <= 335000:
        paye = (taxable_income - 235000) * 0.10
    elif taxable_income <= 410000:
        paye = 10000 + (taxable_income - 335000) * 0.20
    else:
        paye = 25000 + (taxable_income - 410000) * 0.30

    paye = round(paye)

    # -----------------------------
    # LST (Optional Toggle)
    # -----------------------------
    # Only calculate if the toggle is ON and they meet the salary threshold
    lst = 0
    if apply_lst and (gross * 12 > 1200000):
        lst = round(100000 / 12)

    # -----------------------------
    # TOTAL DEDUCTIONS
    # -----------------------------
    total_deductions = paye + nssf_5 + advance + other + lst

    # -----------------------------
    # NET PAY
    # -----------------------------
    net = gross - total_deductions

    return {
        "gross": gross,
        "lst": lst,
        "n5": nssf_5,
        "n10": nssf_10,
        "n15": nssf_15,
        "paye": paye,
        "net": round(net)
    }

# ---------------------------------
# Format Display Table
# ---------------------------------
def format_payroll_display(df):
    if df.empty:
        return df

    df = df.copy()

    df["NO"] = range(1, len(df) + 1)
    df["Salary"] = df["gross_salary"]
    df["Basic"] = df["basic_salary"]
    df["NO PAY DAYS"] = 0
    df["Absenteeism"] = df["absent_deduction"]

    df["Gross Salary"] = df["gross_salary"]
    df["Deductions"] = df["paye"]

    df["P.A.Y.E"] = df["paye"]
    df["N.S.S.F"] = df["nssf_5"]
    df["S.DRS/ADV"] = df["advance_drs"]
    df["Other"] = df["other_deductions"]

    df["Total Deductions"] = (
        df["paye"] + df["nssf_5"] + df["advance_drs"] + df["other_deductions"]
    )

    df["Nett Pay"] = df["net_pay"]
    df["Total Tax on Salary"] = df["paye"]
    df["10% NSSF"] = df["nssf_10"]
    df["15% NSSF"] = df["nssf_15"]

    return df[
        [
            "NO",
            "employee",
            "tin",
            "designation",
            "mob_no",
            "account_no",
            "nssf_no",
            "Salary",
            "Basic",
            "NO PAY DAYS",
            "Absenteeism",
            "Gross Salary",
            "Deductions",
            "P.A.Y.E",
            "N.S.S.F",
            "S.DRS/ADV",
            "Other",
            "Total Deductions",
            "Nett Pay",
            "Total Tax on Salary",
            "10% NSSF",
            "15% NSSF",
        ]
    ]

# ---------------------------------
# MAIN FUNCTION
# ---------------------------------
def show_payroll():

    tenant = st.session_state.get("tenant_id")
    role = st.session_state.get("role")

    if not tenant or role != "Admin":
        st.error("🔒 Restricted: Only Admins only")
        return

    st.markdown("<h2 style='color:#4A90E2;'>🧾 Payroll</h2>", unsafe_allow_html=True)

    # Load Data
    payroll_df = get_cached_data("payroll")

    if payroll_df is not None and not payroll_df.empty:
        payroll_df.columns = payroll_df.columns.astype(str).str.strip().str.replace(" ", "_")
        payroll_df = payroll_df[payroll_df["tenant_id"].astype(str) == str(tenant)]
    else:
        payroll_df = pd.DataFrame()

    # Employee List
    employee_list = []
    if not payroll_df.empty and "employee" in payroll_df.columns:
        employee_list = sorted(payroll_df["employee"].dropna().astype(str).unique())

    # Tabs
    tab_process, tab_history = st.tabs(["💳 Process Payroll", "📜 Payroll History"])

    # =================================
    # PROCESS TAB
    # =================================
    with tab_process:
        with st.form("payroll_form", clear_on_submit=True):
    
            st.subheader("👤 Employee Info")
    
            selected_emp = st.selectbox("Select Employee", employee_list) if employee_list else None
            new_emp = st.text_input("Or Enter New Employee")
    
            employee_name = new_emp if new_emp else selected_emp
    
            c1, c2, c3 = st.columns(3)
            f_tin = c1.text_input("TIN")
            f_desig = c2.text_input("Designation")
            f_mob = c3.text_input("Mobile")
    
            c4, c5 = st.columns(2)
            f_acc = c4.text_input("Account No")
            f_nssf = c5.text_input("NSSF No")
    
            st.subheader("💰 Earnings & Deductions")
    
            # Added the LST toggle as requested to make it optional
            f_apply_lst = st.checkbox("Deduct Local Service Tax (LST)?", value=True)
    
            c6, c7, c8 = st.columns(3)
            f_arrears = c6.number_input("Arrears", min_value=0.0)
            f_basic = c7.number_input("Basic Salary", min_value=0.0)
            f_absent = c8.number_input("Absent Deduction", min_value=0.0)
    
            c9, c10 = st.columns(2)
            f_adv = c9.number_input("Advance", min_value=0.0)
            f_other = c10.number_input("Other Deductions", min_value=0.0)
    
            if st.form_submit_button("💳 Save Payroll"):
    
                if not employee_name or f_basic <= 0:
                    st.error("Enter valid employee & salary")
                    return
    
                month_str = datetime.now().strftime("%Y-%m")
    
                if not payroll_df.empty:
                    duplicate = payroll_df[
                        (payroll_df["employee"] == employee_name) &
                        (payroll_df["month"] == month_str)
                    ]
                    if not duplicate.empty:
                        st.warning("Payroll already exists for this month")
                        return
    
                # Updated to pass the f_apply_lst toggle to your compute_payroll function
                calc = compute_payroll(f_basic, f_arrears, f_absent, f_adv, f_other, apply_lst=f_apply_lst)
    
                new_row = pd.DataFrame([{
                    "payroll_id": str(uuid.uuid4()),
                    "employee": employee_name,
                    "tin": f_tin,
                    "designation": f_desig,
                    "mob_no": f_mob,
                    "account_no": f_acc,
                    "nssf_no": f_nssf,
                    "arrears": f_arrears,
                    "basic_salary": f_basic,
                    "absent_deduction": f_absent,
                    "gross_salary": calc["gross"],
                    "lst": calc["lst"],
                    "paye": calc["paye"],
                    "nssf_5": calc["n5"],
                    "nssf_10": calc["n10"],
                    "nssf_15": calc["n15"],
                    "advance_drs": f_adv,
                    "other_deductions": f_other,
                    "net_pay": calc["net"],
                    "date": datetime.utcnow().isoformat(),
                    "month": month_str,
                    "tenant_id": str(tenant)
                }])
    
                if save_data_saas("payroll", new_row):
                    get_cached_data.clear()
                    st.success(f"✅ Saved for {employee_name}")
                    st.rerun()
    # =================================
    # HISTORY TAB
    # =================================
    with tab_history:

        if payroll_df.empty:
            st.info("No payroll records")
            return

        st.markdown("### 📊 Payroll Sheet")

        # Display Dataframe
        display_df = format_payroll_display(payroll_df)
        try:
            formatted_df = format_with_commas(display_df)
        except NameError:
            formatted_df = display_df 

        st.dataframe(formatted_df, use_container_width=True)

        # -----------------------------
        # 🛠️ EDIT / DELETE SECTION
        # -----------------------------
        st.markdown("---")
        st.subheader("🛠️ Manage Records")
        
        # Select record by Employee and date for clarity
        record_options = payroll_df.apply(lambda x: f"{x['employee']} ({x['month']}) | ID: {x['payroll_id'][:8]}", axis=1).tolist()
        selected_record_str = st.selectbox("Select a record to Edit or Delete", options=record_options)

        if selected_record_str:
            # Extract the actual payroll_id from the selection string
            sel_id = selected_record_str.split("| ID: ")[1].strip()
            # Match back to the full ID in the dataframe
            full_record = payroll_df[payroll_df['payroll_id'].str.contains(sel_id)].iloc[0]

            col_edit, col_del = st.columns(2)

            with col_edit:
                if st.button("📝 Edit Selected Record"):
                    st.warning("To edit: Adjust details in the 'Process Payroll' tab with the same name and month to overwrite, or use the database editor.")
            
            with col_del:
                if st.button("🗑️ Delete Record", type="primary"):
                    # This will now find the function in your imported modules
                    if delete_data_saas("payroll", {"payroll_id": full_record['payroll_id']}):
                        get_cached_data.clear()
                        st.success(f"Deleted payroll for {full_record['employee']}")
                        st.rerun()

        st.markdown("---")
        # -----------------------------
        # 📄 DOWNLOADS
        # -----------------------------
        c1, c2 = st.columns(2)
        with c1:
            csv = payroll_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "📄 Download CSV",
                data=csv,
                file_name=f"Payroll_{datetime.now().strftime('%Y%m%d')}.csv",
                use_container_width=True
            )

        with c2:
            excel_file = export_styled_excel(payroll_df)
            st.download_button(
                "📥 Download Styled Excel",
                data=excel_file,
                file_name=f"Payroll_Styled_{datetime.now().strftime('%B_%Y')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )

# ==========================================
# 🚀 BALLISTIC FINTECH REPORTS ENGINE (PRODUCTION READY)
# ==========================================

def show_reports():
    """
    Advanced financial reporting with multi-tenant isolation 
    and investor-grade intelligence metrics.
    """

    # ==============================
    # 🎨 HEADER (EXECUTIVE UI)
    # ==============================
    st.markdown("""
    <div style='background: linear-gradient(90deg,#1E3A8A,#2B3F87); padding:20px; border-radius:15px; margin-bottom:25px;'>
        <h2 style='margin:0; color:white; font-size:24px;'>📊 Financial Intelligence Dashboard</h2>
        <p style='margin:0; color:#DBEAFE; font-size:13px;'>Real-time P&L, Balance Sheet, and Portfolio Yield Analysis</p>
    </div>
    """, unsafe_allow_html=True)

    tenant = st.session_state.get("tenant_id")
    if not tenant:
        st.error("Session Expired.")
        return

    # ==============================
    # 🛡️ DATA FETCH & TENANT SAFETY
    # ==============================
    def safe_tenant_filter(df_name):
        try:
            df = get_cached_data(df_name)
            if df is None or df.empty:
                return pd.DataFrame()
            if "tenant_id" in df.columns:
                return df[df["tenant_id"].astype(str) == str(tenant)].copy()
            return df
        except Exception:
            return pd.DataFrame()

    loans = safe_tenant_filter("loans")
    payments = safe_tenant_filter("payments")
    expenses = safe_tenant_filter("expenses")
    payroll = safe_tenant_filter("payroll")
    petty = safe_tenant_filter("petty_cash")
    borrowers = safe_tenant_filter("borrowers")

    if loans.empty:
        st.info("💡 No loan data found. Financial reports will populate once loans are issued.")
        return

    # ==============================
    # 🧰 REUSABLE HELPERS
    # ==============================
    def col_sum(df, col):
        if df.empty or col not in df.columns: return 0.0
        return pd.to_numeric(df[col], errors="coerce").fillna(0).sum()

    def to_numeric_series(df, col):
        if df.empty or col not in df.columns: return pd.Series(dtype=float)
        return pd.to_numeric(df[col], errors="coerce").fillna(0)

    def attach_borrower_names(loans_df, borrowers_df):
        if borrowers_df.empty or "id" not in borrowers_df.columns or "name" not in borrowers_df.columns:
            loans_df["borrower"] = loans_df.get("borrower", "Unknown")
            return loans_df
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        mapped = loans_df["borrower_id"].map(bor_map)
        loans_df["borrower"] = mapped.fillna(loans_df.get("borrower")).fillna("Unknown")
        return loans_df

    loans = attach_borrower_names(loans, borrowers)

    # ==============================
    # 🔢 CORE FINANCIAL ACCOUNTING
    # ==============================
    
    # Ensure numeric safety
    for col in ["principal", "interest", "cycle_no"]:
        if col in loans.columns:
            loans[col] = pd.to_numeric(loans[col], errors="coerce").fillna(0)
    
    # Normalize status
    loans["status"] = loans["status"].astype(str).str.upper()
    
    # ==============================
    # 🧠 ACTIVE CAPITAL (Cycle 1 ONLY, Active/Pending Loans)
    # ==============================
    active_capital_loans = loans[
        (loans["cycle_no"] == 1) &
        (loans["status"].isin(["ACTIVE", "PENDING"]))
    ]
    
    total_capital_out = active_capital_loans["principal"].sum()
    
    # ==============================
    # 💰 INT. REVENUE (ONLY CLEARED LOANS, ALL CYCLES INCLUDED)
    # ==============================
    cleared_loans = loans[loans["status"] == "CLEARED"]
    
    projected_interest = cleared_loans["interest"].sum()
    
    # ==============================
    # 💵 COLLECTIONS (UNCHANGED)
    # ==============================
    actual_collected = col_sum(payments, "amount")
    
    # ==============================
    # 💸 OPEX (UNCHANGED)
    # ==============================
    direct_expenses = col_sum(expenses, "amount")
    nssf_tax = col_sum(payroll, "nssf_5") + col_sum(payroll, "nssf_10")
    paye_tax = col_sum(payroll, "paye")
    salary_net = col_sum(payroll, "net_pay")
    
    petty_out = 0
    if not petty.empty and "type" in petty.columns:
        petty_out = col_sum(petty[petty["type"] == "Out"], "amount")
    
    total_opex = direct_expenses + petty_out + nssf_tax + paye_tax + salary_net
    
    # ==============================
    # 📊 NET CASHFLOW (UNCHANGED)
    # ==============================
    cash_profit = actual_collected - total_opex
    # ==============================
    # 💎 KPI TILES
    # ==============================
    def render_kpi(title, value, color, icon="💰"):
        st.markdown(f"""
            <div style="padding:16px; border-radius:12px; background:white; border:1px solid #E5E7EB; box-shadow:0 2px 4px rgba(0,0,0,0.02); margin-bottom:10px;">
                <p style="font-size:11px; color:#6B7280; margin:0; font-weight:600;">{icon} {title}</p>
                <h3 style="margin:0; color:{color}; font-size:20px;">UGX {value:,.0f}</h3>
            </div>
        """, unsafe_allow_html=True)

    k1, k2, k3, k4 = st.columns(4)
    with k1: render_kpi("ACTIVE CAPITAL", total_capital_out, "#1E3A8A")
    with k2: render_kpi("INT. REVENUE", projected_interest, "#059669")
    with k3: render_kpi("COLLECTIONS", actual_collected, "#7C3AED")
    with k4: render_kpi("NET CASHFLOW", cash_profit, "#059669" if cash_profit >= 0 else "#DC2626")

    # ==============================
    # 📈 TREND ANALYSIS
    # ==============================
    st.markdown("### 📈 Monthly Profit & Loss Trend", unsafe_allow_html=True)

    payments["date"] = pd.to_datetime(payments.get("date"), errors="coerce")
    expenses["date"] = pd.to_datetime(expenses.get("date"), errors="coerce")

    if not payments.empty:
        inc_m = payments.set_index("date").resample("ME")["amount"].sum()
        inc_m.name = "Income"
    else:
        inc_m = pd.Series(dtype=float, name="Income")

    if not expenses.empty:
        exp_m = expenses.set_index("date").resample("ME")["amount"].sum()
        exp_m.name = "Expenses"
    else:
        exp_m = pd.Series(dtype=float, name="Expenses")
    pl_combined = pd.concat([inc_m, exp_m], axis=1).fillna(0)
    pl_combined["Net"] = pl_combined["Income"] - pl_combined["Expenses"]

    if not pl_combined.empty:
        fig_trend = px.area(
            pl_combined,
            color_discrete_map={"Income": "#059669", "Expenses": "#EF4444", "Net": "#1E3A8A"},
            line_shape="spline",
            labels={"index":"Month", "value":"UGX"}
        )
        fig_trend.update_layout(
            hovermode="x unified",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("📉 No trend data available yet.")

    # ==============================
    # 🧠 INVESTOR INTELLIGENCE METRICS
    # ==============================
    overdue_loans = loans[loans["status"].astype(str).str.upper().str.contains("OVERDUE", na=False)]
    par_value = col_sum(overdue_loans, "balance")
    par_ratio = (par_value / total_capital_out * 100) if total_capital_out > 0 else 0
    yield_pct = (projected_interest / total_capital_out * 100) if total_capital_out > 0 else 0
    coll_eff = (actual_collected / (total_capital_out + projected_interest) * 100) if (total_capital_out + projected_interest) > 0 else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Portfolio Yield", f"{yield_pct:.1f}%")
    m2.metric("Collection Eff.", f"{coll_eff:.1f}%")
    m3.metric("PAR Ratio", f"{par_ratio:.1f}%", delta=f"{par_ratio:.1f}%", delta_color="inverse")
    m4.metric("OpEx Ratio", f"{(total_opex/actual_collected*100 if actual_collected > 0 else 0):.1f}%")

    # ==============================
    # 🧾 FINANCIAL STATEMENTS (CYCLE-AWARE)
    # ==============================
    s1, s2 = st.columns(2)
    
    # ------------------------------
    # Compute fiscal year: July–June
    # ------------------------------
    def fiscal_year(dt):
        if pd.isna(dt):
            return "Unknown"
        return f"{dt.year}-{dt.year+1}" if dt.month >= 7 else f"{dt.year-1}-{dt.year}"
    
    loans["start_date"] = pd.to_datetime(loans.get("start_date"), errors="coerce")
    payments["date"] = pd.to_datetime(payments.get("date"), errors="coerce")
    expenses["date"] = pd.to_datetime(expenses.get("date"), errors="coerce")
    
    loans["fiscal_year"] = loans["start_date"].apply(fiscal_year)
    payments["fiscal_year"] = payments["date"].apply(fiscal_year)
    expenses["fiscal_year"] = expenses["date"].apply(fiscal_year)
    
    fiscal_years = sorted(loans["fiscal_year"].dropna().unique())
    
    # ==============================
    # 💰 INCOME STATEMENT & BALANCE SHEET (FY-AWARE)
    # ==============================
    
    # ------------------------------
    # Fiscal Year Selector
    # ------------------------------
    fiscal_years = sorted(loans["fiscal_year"].dropna().unique())
    selected_fy = st.selectbox("Select Financial Year", fiscal_years)
    
    fy_loans = loans[loans["fiscal_year"] == selected_fy]
    fy_expenses = expenses[expenses["fiscal_year"] == selected_fy]
    fy_payments = payments[payments["fiscal_year"] == selected_fy]
    
    # ------------------------------
    # 💰 INCOME STATEMENT
    # ------------------------------
    with s1:
        st.subheader(f"💰 Income Statement (Profit & Loss) — FY {selected_fy}")
    
        # Active Capital → Cycle 1 PENDING/ACTIVE only
        active_capital = fy_loans[
            (fy_loans["cycle_no"] == 1) &
            (fy_loans["status"].str.upper().isin(["ACTIVE", "PENDING"]))
        ]["principal"].sum()
    
        # Interest Revenue → CLEARED loans (all cycles)
        int_revenue = fy_loans[
            fy_loans["status"].str.upper() == "CLEARED"
        ]["interest"].sum()
    
        # OPEX → Direct expenses only (salaries/taxes already included)
        total_opex = col_sum(fy_expenses, "amount")
    
        # Net Profit
        net_profit = int_revenue - total_opex
    
        st.dataframe(pd.DataFrame({
            "Description": [
                "Active Capital (Cycle 1 ACTIVE/PENDING)",
                "Interest Revenue (CLEARED Loans)",
                "Total Operating Expenses (OPEX)",
                "Net Profit"
            ],
            "amount (UGX)": [
                f"{active_capital:,.0f}",
                f"{int_revenue:,.0f}",
                f"{total_opex:,.0f}",
                f"{net_profit:,.0f}"
            ]
        }), use_container_width=True)
    
    # ------------------------------
    # 🏦 BALANCE SHEET SNAPSHOT
    # ------------------------------
    with s2:
        st.subheader(f"🏦 Balance Sheet — FY {selected_fy}")
    
        # Loan Book → all cycles outstanding balances
        loan_book_value = fy_loans["balance"].sum()
    
        # Cash Position → payments minus expenses
        cash_position = col_sum(fy_payments, "amount") - col_sum(fy_expenses, "amount")
    
        # Total Assets = Active Capital + Loan Book + Cash Position
        total_assets = active_capital + loan_book_value + cash_position
    
        st.dataframe(pd.DataFrame({
            "Description": [
                "Active Capital (Cycle 1 ACTIVE/PENDING)",
                "Loan Book (All Outstanding Cycles)",
                "Cash Position",
                "Total Assets"
            ],
            "amount (UGX)": [
                f"{active_capital:,.0f}",
                f"{loan_book_value:,.0f}",
                f"{cash_position:,.0f}",
                f"{total_assets:,.0f}"
            ]
        }), use_container_width=True)
    
    # ------------------------------
    # 📤 EXPORT
    # ------------------------------
    with st.expander(f"📥 Export Executive Report — FY {selected_fy}"):
    
        export_rows = [{
            "Fiscal Year": selected_fy,
            "Active Capital": active_capital,
            "Interest Revenue": int_revenue,
            "Total OPEX": total_opex,
            "Net Profit": net_profit,
            "Cash Position": cash_position,
            "Loan Book Value": loan_book_value,
            "Total Assets": total_assets
        }]
    
        export_df = pd.DataFrame(export_rows)
        st.dataframe(export_df)
    
        csv = export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download Full Executive Report (CSV)",
            data=csv,
            file_name=f"FinReport_{selected_fy}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )


            
# ==============================
# 🚨 20.OVERDUE TRACKER 
# ==============================
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



def show_calendar():
    st.markdown("<h2 style='color: #2B3F87;'>📅 Activity Calendar</h2>", unsafe_allow_html=True)

    # 1. FETCH DATA (SAFE ADAPTERS)
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    # --- 🛡️ SMART STATUS LOGIC (FIX FOR CLEARED LOANS) ---
    # We process the statuses BEFORE filtering active_loans to ensure 0 balances are removed
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip().str.upper()
    loans_df["balance"] = pd.to_numeric(loans_df["balance"], errors="coerce").fillna(0)
    
    # Sort chronologically to identify previous cycles
    loans_df = loans_df.sort_values(by=["sn", "cycle_no", "start_date"])

    for sn_val, grp in loans_df.groupby("sn"):
        indices = grp.index.tolist()
        
        # Mark all but the latest entry as BCF
        if len(indices) > 1:
            loans_df.loc[indices[:-1], "status"] = "BCF"
        
        # Check the terminal (latest) row
        latest_idx = indices[-1]
        if abs(loans_df.at[latest_idx, "balance"]) < 1.0:
            loans_df.at[latest_idx, "status"] = "CLEARED"
        # Otherwise, it maintains its existing PENDING or ACTIVE status

    # Global safety: Any row with 0 balance that isn't BCF must be CLEARED
    loans_df.loc[(loans_df["balance"] <= 0) & (loans_df["status"] != "BCF"), "status"] = "CLEARED"

    # --- 👤 INJECT BORROWER NAMES (MAPPING) ---
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
    
    # Filter for active loans (Excluding CLEARED and BCF)
    active_loans = loans_df[~loans_df["status"].astype(str).str.upper().isin(["CLEARED", "BCF", "CLOSED"])].copy()

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
        st.markdown(f"""<div style="border:2px solid #2B3F87;border-radius:10px;overflow:hidden;"><table style="width:100%;border-collapse:collapse;font-size:12px;"><tr style="background:#2B3F87;color:white;"><th style="padding:10px;">Loan ID</th><th style="padding:10px;">borrower</th><th style="padding:10px;text-align:right;">amount</th><th style="padding:10px;text-align:center;">Action</th></tr>{today_rows}</table></div>""", unsafe_allow_html=True)

    # 5. 🔴 OVERDUE FOLLOW-UP
    st.markdown("<br><h4 style='color: #FF4B4B;'>🔴 Overdue Follow-up</h4>", unsafe_allow_html=True)
    try:
        # Filter for overdue: end_date passed AND status is not CLEARED/BCF
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
# 🛡️ 15. COLLATERAL MANAGEMENT
# ==============================

def show_collateral():
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
                    "Asset type",
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
    # 📋 TAB 2: INVENTORY & STATUS (INTERACTIVE)
    # ==============================
    with tab_view:
    
        if collateral_df is None or collateral_df.empty:
            st.info("💡 No assets currently in the registry.")
    
        else:
    
            # ==============================
            # 📊 METRIC DASHBOARD
            # ==============================
            collateral_df["value"] = pd.to_numeric(collateral_df["value"], errors="coerce").fillna(0)
    
            total_value = collateral_df["value"].sum()
            held_count = len(collateral_df[collateral_df["status"] == "In Custody"])
    
            m1, m2 = st.columns(2)
            m1.metric("Total Asset Value (Security)", f"UGX {total_value:,.0f}")
            m2.metric("Items in Custody", held_count)
    
            st.divider()
    
            # ==============================
            # 🔍 FILTERS (NEW INTERACTIVE LAYER)
            # ==============================
            col1, col2 = st.columns(2)
    
            status_filter = col1.selectbox(
                "Filter by Status",
                ["All"] + sorted(collateral_df["status"].dropna().unique().tolist())
            )
    
            borrower_filter = col2.text_input("Search borrower / description").lower()
    
            df = collateral_df.copy()
    
            # Apply filters
            if status_filter != "All":
                df = df[df["status"] == status_filter]
    
            if borrower_filter:
                df = df[
                    df["borrower"].str.lower().str.contains(borrower_filter, na=False) |
                    df["description"].str.lower().str.contains(borrower_filter, na=False)
                ]
    
            # ==============================
            # 📊 INTERACTIVE TABLE (NO HTML)
            # ==============================
            st.markdown("### Asset Ledger")
    
            display_df = df.copy()
    
            display_df["Value (UGX)"] = display_df["value"].apply(lambda x: f"{x:,.0f}")
            display_df = display_df.rename(columns={
                "date_added": "date Registered",
                "borrower": "Borrower",
                "type": "type",
                "description": "Description",
                "status": "Status"
            })
    
            table_df = display_df[[
                "date Registered",
                "Borrower",
                "type",
                "Description",
                "Value (UGX)",
                "Status"
            ]]
    
            st.dataframe(
                table_df,
                use_container_width=True,
                hide_index=True
            )
    
            st.divider()
    
            # ==============================
            # ⚙️ ASSET MANAGEMENT & PHOTO VIEW
            # ==============================
            st.markdown("### 🛠️ View Details & Manage Lifecycle")
    
            manageable = df.copy()
    
            if manageable.empty:
                st.warning("No assets match your filters.")
            else:
    
                # Better labels (keeps your logic but cleaner UX)
                manageable["label"] = manageable.apply(
                    lambda x: f"{x['borrower']} — {x['description']}", axis=1
                )
    
                selected_label = st.selectbox(
                    "Select Asset",
                    manageable["label"].tolist()
                )
    
                selected_row = manageable[manageable["label"] == selected_label].iloc[0]
                asset_id = selected_row["id"]
    
                # ==============================
                # 📸 PHOTO EVIDENCE
                # ==============================
                st.markdown("#### 📸 Photo Evidence")
    
                asset_photo = selected_row.get("photo", selected_row.get("image_url", None))
    
                if asset_photo:
                    st.image(asset_photo, caption=selected_row["description"], use_container_width=True)
                else:
                    st.info("No photo uploaded for this asset.")
    
                st.divider()
    
                # ==============================
                # 🔄 STATUS UPdate (INTERACTIVE IMPROVED)
                # ==============================
                st.markdown("#### 🔄 Update Status")
    
                status_options = ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"]
    
                col_stat, col_btn = st.columns([3, 1])
    
                new_status = col_stat.selectbox(
                    "Change Status",
                    status_options,
                    index=status_options.index(selected_row["status"])
                    if selected_row["status"] in status_options else 0
                )
    
                if col_btn.button("Update Status", use_container_width=True):
    
                    update_row = pd.DataFrame([{
                        "id": asset_id,
                        "status": new_status,
                        "tenant_id": str(current_tenant)
                    }])
    
                    if save_data_saas("collateral", update_row):
                        st.success("✅ Asset status updated successfully!")
                        st.cache_data.clear()
                        st.rerun()
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
        run_auth_ui(supabase)
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
                
            elif page == "Overdue Tracker":
                show_overdue_tracker()
                
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
