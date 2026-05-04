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

st.set_page_config(
    page_title="Lending Manager Pro",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
SESSION_TIMEOUT = 30

# ==============================
# 🔒 INITIALIZE SUPABASE
# ==============================
if "supabase" not in st.session_state:
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        st.session_state.supabase = create_client(url, key)
    except Exception as e:
        st.error("Failed to connect to Supabase. Check your secrets.")
        st.session_state.supabase = None

supabase = st.session_state.supabase

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
    user_id = cookie_manager.get("user_id")
    if user_id:
        st.session_state["authenticated"] = True
        st.session_state["user_id"] = user_id
        st.session_state["tenant_id"] = cookie_manager.get("tenant_id")

# ==============================
# ⚡ CORE DATA ENGINE
# ==============================
@st.cache_data(show_spinner=False)
def get_cached_data(table_name, version):
    """Fetches data from Supabase with caching."""
    try:
        if supabase is None:
            return pd.DataFrame()

        # require_tenant() 
        tenant_id = st.session_state.get("tenant_id")

        if not tenant_id:
            return pd.DataFrame()

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

    /* 🔥 ACTIVE ITEM (FULL BRIGHT + GLOW) */
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

    /* SIDEBAR BACKGROUND (GRADIENT) */
    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {brand_color} 0%, #0F172A 100%) !important;
    }}

    /* REMOVE DEFAULT PADDING */
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

    /* LOGO CIRCLE EFFECT */
    .logo-container img {{
        border-radius: 50%;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    }}
    </style>
    """, unsafe_allow_html=True)


# ==============================
# 🔌 2. SUPABASE INIT (SAFE GLOBAL)
# ==============================

@st.cache_resource
def init_supabase():
    """Initializes the Supabase client once and caches it."""
    try:
        url = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
        key = st.secrets.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")

        if not url or not key:
            return None

        return create_client(url, key)
    except Exception:
        return None

# Global Instance
supabase = init_supabase()

if supabase is None:
    st.warning("⚠️ Supabase not connected (some features may not work)")


# ==============================
# 💾 3. DATA PERSISTENCE
# ==============================

def save_data(table_name, dataframe):
    """Upserts data to Supabase ensuring tenant isolation."""
    try:
        if supabase is None:
            st.error("❌ Database not connected")
            return False
        # require_tenant() 
        tenant_id = st.session_state.get("tenant_id")

        if dataframe is None or dataframe.empty:
            st.error(" No Data")
            return False

        # Prepare for Supabase (handle NaNs for JSON compatibility)
        df_to_save = dataframe.copy()
        df_to_save["tenant_id"] = tenant_id
        records = df_to_save.replace({np.nan: None}).to_dict("records")

        response = supabase.table(table_name).upsert(records).execute()

        if hasattr(response, "data") and response.data:
            st.success(f"✅ Saved {len(response.data)} record(s)")
            # Optional: increment data version to refresh caches
            st.session_state["data_version"] += 1
            return True
        else:
            st.warning("⚠️ Save completed but returned no data confirmation")
            return True

    except Exception as e:
        st.error(f"🔥 DATABASE SAVE ERROR [{table_name}]: {e}")
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

@st.cache_data(ttl=600)
def get_cached_data(table_name):
    """Fetches tenant-specific data with a 10-minute cache."""
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
            # Standardize column naming convention
            df.columns = df.columns.str.strip().str.lower()
            return df
        return pd.DataFrame()
        
    except Exception as e:
        st.error(f"Database Fetch Error [{table_name}]: {e}")
        return pd.DataFrame()

def save_data(table_name, dataframe):
    """Upserts records and provides detailed status feedback."""
    try:
        if supabase is None:
            st.error("❌ Database not connected")
            return False

        require_tenant()

        if dataframe is None or dataframe.empty:
            st.error(" No Data")
            return False

        # Ensure every row is tagged with the correct tenant
        df_to_save = dataframe.copy()
        df_to_save["tenant_id"] = get_tenant_id()

        # Handle NaNs (SQL doesn't like Python's NaN, prefers None/null)
        records = df_to_save.replace({np.nan: None}).to_dict("records")

        # Execute upsert (Insert or Update based on Primary Key)
        response = supabase.table(table_name).upsert(records).execute()

        # Validation logic
        if hasattr(response, "data") and response.data:
            st.success(f"✅ Successfully saved {len(response.data)} record(s).")
            return True
        else:
            st.warning("⚠️ Save submitted but no confirmation data returned.")
            return False

    except Exception as e:
        st.error(f"🔥 Database Save Error [{table_name}]: {e}")
        return False

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
# 👥 EMPLOYEE MANAGEMENT IN LOGIN FLOW
# =========================================
def admin_company_registration(supabase):
    st.markdown("## 🏢 Register Your Company", unsafe_allow_html=True)
    with st.form("company_reg_form"):
        st.text_input("Organization name", key="company_name")
        st.text_input("Admin Full name", key="admin_name")
        st.text_input("Business Email", key="admin_email")
        st.text_input("Password", type="password", key="admin_pwd")
        submit = st.form_submit_button("Create Organization", use_container_width=True)

    if submit:
        company_name = st.session_state.get("company_name", "").strip()
        admin_name = st.session_state.get("admin_name", "").strip()
        email = st.session_state.get("admin_email", "").strip().lower()
        pwd = st.session_state.get("admin_pwd", "").strip()

        if not all([company_name, admin_name, email, pwd]):
            st.error("All fields are required")
            return

        # 1. Sign up user
        try:
            res = supabase.auth.sign_up({"email": email, "password": pwd})
            if res.user:
                tenant_id = str(uuid.uuid4())
                company_code = f"{company_name[:3].upper()}{uuid.uuid4().int % 999}"
                supabase.table("tenants").insert({
                    "id": tenant_id,
                    "name": company_name,
                    "company_code": company_code
                }).execute()

                supabase.table("users").insert({
                    "id": res.user.id,
                    "name": admin_name,
                    "email": email,
                    "tenant_id": tenant_id,
                    "role": "Admin"
                }).execute()

                # Audit log
                supabase.table("audit_logs").insert({
                    "user_id": res.user.id,
                    "action": "CREATE_COMPANY",
                    "tenant_id": tenant_id,
                    "timestamp": datetime.now()
                }).execute()

                st.success(f"✅ {company_name} registered! Company Code: {company_code}")
                st.session_state["view"] = "login"
                st.rerun()
        except Exception as e:
            st.error(f"Registration failed: {e}")

# =========================================
# 🔑 LOGIN PAGE WITH MODERN UI
# =========================================
def login_page(supabase):
    st.markdown("""
        <div style="background: linear-gradient(90deg,#1E3A8A,#2B3F87); padding:20px; border-radius:15px;">
            <h2 style="color:white;">💰 PEAK-LENDERS AFRICA</h2>
            <p style="color:#DBEAFE;">Secure Login Portal</p>
        </div>
        """, unsafe_allow_html=True)

    with st.form("login_form"):
        st.text_input("Business name", key="login_company")
        st.text_input("Email", key="login_email")
        st.text_input("Password", type="password", key="login_pwd")
        submit = st.form_submit_button("Access Dashboard", use_container_width=True)

    if submit:
        email = st.session_state.get("login_email", "").strip().lower()
        company_name = st.session_state.get("login_company", "").strip().lower()
        pwd = st.session_state.get("login_pwd", "")

        # 1. Rate limit
        if not check_rate_limit(email):
            st.error(f"Too many failed attempts. Wait {LOCKOUT_MINUTES} minutes.")
            return

        try:
            res = supabase.auth.sign_in_with_password({"email": email, "password": pwd})
            if not res.user:
                record_failed_attempt(email)
                st.error("Login failed. Please check credentials.")
                return

            # 2. Tenant validation
            user_query = supabase.table("users")\
                .select("*, tenants(name)")\
                .eq("id", res.user.id)\
                .execute()
            if not user_query.data:
                st.error("Profile not found.")
                return

            user = user_query.data[0]
            db_company = user.get("tenants", {}).get("name", "").lower()
            if db_company != company_name:
                st.error(f"Membership Error: Not linked to '{company_name}'")
                return

            # 3. Create session
            create_session({
                "user_id": user["id"],
                "tenant_id": user["tenant_id"],
                "role": user.get("role", "Staff"),
                "company": user.get("tenants", {}).get("name")
            })
            
            # Audit log
            supabase.table("audit_logs").insert({
                "user_id": user["id"],
                "action": "LOGIN",
                "tenant_id": user["tenant_id"],
                "timestamp": datetime.now()
            }).execute()

        except Exception as e:
            st.error(f"Login failed: {e}")

# =========================================
# 🌐 AUTH ROUTER
# =========================================
def run_auth_ui(supabase):
    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    # Modern back button
    if st.session_state["view"] in ["signup", "create_company"]:
        if st.button("⬅️ Back to Login"):
            st.session_state["view"] = "login"
            st.rerun()

    view = st.session_state["view"]
    if view == "login":
        login_page(supabase)
    elif view == "signup":
        view_staff_signup(supabase)
    elif view == "create_company":
        admin_company_registration(supabase)




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
# 📁 18. EXPENSE MANAGEMENT PAGE (SAAS + ENTERPRISE UPGRADE)
# ==============================
import plotly.express as px
import uuid
import pandas as pd
import streamlit as st
from datetime import datetime

def show_expenses():
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
# 💵 19. PETTY CASH MANAGEMENT PAGE
# ==============================
import pandas as pd
import streamlit as st
from datetime import datetime

def show_petty_cash():
    """
    Manages daily office cash transactions with a modern Banking UI.
    Tracks inflows/outflows for specific tenants with real-time balance alerts.
    """
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    # ==============================
    # 🎨 BANKING UI SYSTEM (ENHANCED)
    # ==============================
    st.markdown(f"""
    <style>
    .block-container {{ padding-top: 1.2rem; }}
    
    /* Glassmorphism Cards */
    .glass-card {{
        backdrop-filter: blur(10px);
        background: linear-gradient(145deg, rgba(255,255,255,0.9), rgba(240,244,255,0.7));
        border-radius: 16px;
        padding: 20px;
        border: 1px solid rgba(43,63,135,0.1);
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
        transition: transform 0.2s ease;
    }}
    .glass-card:hover {{ transform: translateY(-3px); box-shadow: 0 8px 25px rgba(0,0,0,0.08); }}

    .metric-title {{ font-size: 11px; color: #6b7280; font-weight: 600; letter-spacing: 0.8px; text-transform: uppercase; }}
    .metric-value {{ font-size: 24px; font-weight: 700; margin-top: 4px; }}
    
    /* status Badges */
    .status-badge {{ font-size: 10px; padding: 3px 10px; border-radius: 12px; font-weight: 700; float: right; }}
    .badge-safe {{ background: #E1F9F0; color: #10B981; }}
    .badge-low {{ background: #FFEBEB; color: #FF4B4B; }}
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f"<h2 style='color:{brand_color};'>💵 Petty Cash Management</h2>", unsafe_allow_html=True)

    # ==============================
    # 📦 1. DATA ADAPTER & ISOLATION
    # ==============================
    df = get_cached_data("petty_cash")

    if df is None or df.empty:
        df = pd.DataFrame(columns=["id", "type", "amount", "date", "description", "tenant_id"])
    else:
        # Enforce multi-tenancy
        df = df[df["tenant_id"].astype(str) == str(current_tenant)].copy()
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    # ==============================
    # 📈 2. LIQUIDITY CALCULATIONS
    # ==============================
    inflow = df[df["type"] == "In"]["amount"].sum()
    outflow = df[df["type"] == "Out"]["amount"].sum()
    balance = inflow - outflow

    # Threshold for "Low balance" warning
    LOW_CASH_THRESHOLD = 50000
    bal_status = "SAFE" if balance >= LOW_CASH_THRESHOLD else "LOW"
    status_class = "badge-safe" if balance >= LOW_CASH_THRESHOLD else "badge-low"
    bal_color = "#10B981" if balance >= LOW_CASH_THRESHOLD else "#FF4B4B"

    # ==============================
    # 💎 KPI DASHBOARD
    # ==============================
    c1, c2, c3 = st.columns(3)

    c1.markdown(f"""<div class="glass-card"><div class="metric-title">Total Cash In</div>
        <div class="metric-value" style="color:#10B981;">{inflow:,.0f} <span style="font-size:12px;">UGX</span></div></div>""", unsafe_allow_html=True)

    c2.markdown(f"""<div class="glass-card"><div class="metric-title">Total Cash Out</div>
        <div class="metric-value" style="color:#FF4B4B;">{outflow:,.0f} <span style="font-size:12px;">UGX</span></div></div>""", unsafe_allow_html=True)

    c3.markdown(f"""<div class="glass-card">
        <div class="metric-title">Current balance <span class="status-badge {status_class}">{bal_status}</span></div>
        <div class="metric-value" style="color:{bal_color};">{balance:,.0f} <span style="font-size:12px;">UGX</span></div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ==============================
    # 📋 TABS: ACTION & LOG
    # ==============================
    tab_record, tab_history = st.tabs(["➕ Record Transaction", "📜 Digital Cashbook"])

    # --- TAB 1: RECORD ENTRY ---
    with tab_record:
        with st.form("petty_cash_form", clear_on_submit=True):
            st.write("### Log Cash Movement")
            col_a, col_b = st.columns(2)
            ttype = col_a.selectbox("Transaction type", ["Out", "In"], help="'In' for top-ups, 'Out' for expenses")
            t_amount = col_b.number_input("Amount (UGX)", min_value=0, step=500)
            desc = st.text_input("Purpose / Description", placeholder="e.g., Office Internet bundle, Cleaning supplies")

            if st.form_submit_button("💾 Commit to Cashbook", use_container_width=True):
                if t_amount > 0 and desc:
                    new_row = pd.DataFrame([{
                        "id": str(uuid.uuid4()) if 'uuid' in globals() else datetime.now().strftime("%Y%m%d%H%M%S"),
                        "type": ttype,
                        "amount": float(t_amount),
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "description": desc,
                        "tenant_id": str(current_tenant)
                    }])
                    
                    # Merge with existing for the save_data function
                    if save_data("petty_cash", pd.concat([df, new_row], ignore_index=True)):
                        st.success(f"✅ Recorded {t_amount:,.0f} UGX {ttype}flow")
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.error("⚠️ Please provide a valid amount and description.")

    # --- TAB 2: TRANSACTION HISTORY ---
    with tab_history:
        if df.empty:
            st.info("ℹ️ No cash transactions recorded yet.")
        else:
            st.markdown("### 📜 Transaction Log")
            
            # Format the dataframe for professional display
            display_df = df.sort_values("date", ascending=False).copy()
            
            # Stylized display using st.dataframe
            st.dataframe(
                display_df[["date", "type", "description", "amount"]].rename(
                    columns={"date": "Date", "type": "type", "description": "Details", "amount": "Amount (UGX)"}
                ),
                use_container_width=True,
                hide_index=True
            )

            # ==============================
            # ⚙️ ADVANCED MANAGEMENT (CRUD)
            # ==============================
            with st.expander("🛠️ Correct or Remove Entry"):
                # Use a specific list for the selectbox to prevent index errors
                entry_list = display_df.apply(lambda r: f"{r['date']} | {r['type']} | {r['description'][:20]}... | {r['amount']:,.0f}", axis=1).tolist()
                selected_label = st.selectbox("Select Entry to Modify", options=entry_list)
                
                # Get the original record
                selected_idx = entry_list.index(selected_label)
                original_record = display_df.iloc[selected_idx]
                entry_id = original_record["id"]

                c_edit, c_del = st.columns(2)
                
                # We use a sub-form for the edit to keep state clean
                with st.popover("📝 Edit Record Details"):
                    new_desc = st.text_input("Edit Description", value=original_record["description"])
                    new_amt = st.number_input("Edit Amount", value=float(original_record["amount"]))
                    if st.button("Save Changes"):
                        df.loc[df["id"] == entry_id, ["description", "amount"]] = [new_desc, new_amt]
                        if save_data("petty_cash", df):
                            st.success("Entry Updated")
                            st.cache_data.clear()
                            st.rerun()

                if c_del.button("🗑️ Delete Permanently", use_container_width=True, type="secondary"):
                    # Filter out the deleted ID
                    df_filtered = df[df["id"] != entry_id]
                    if save_data("petty_cash", df_filtered):
                        st.warning("Entry removed from digital cashbook.")
                        st.cache_data.clear()
                        st.rerun()
                

# ==============================
# 🚨 20. AI OVERDUE TRACKER (BULLETPROOF)
# ==============================
import pandas as pd
import streamlit as st
from datetime import datetime

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

# =================================
# 🏢 Enterprise Payroll Engine (Fixed Truth-Value Errors)
# =================================
import streamlit as st
import pandas as pd
from datetime import datetime
import uuid

def show_payroll_enterprise():
    tenant = st.session_state.get("tenant_id")
    role = st.session_state.get("role")
    if not tenant or role != "Admin":
        st.error("🔒 Restricted: Only Admins with valid session can access payroll")
        return

    st.markdown("<h2 style='color:#4A90E2;'>🧾 Enterprise Payroll</h2>", unsafe_allow_html=True)

    # -----------------------------
    # 1. Load Payroll & Employees
    # -----------------------------
    payroll_df = get_cached_data("payroll")
    if payroll_df is None:
        payroll_df = pd.DataFrame()
    emp_df = get_cached_data("employees")
    if emp_df is None:
        emp_df = pd.DataFrame()

    # Standardize column names
    payroll_df.columns = payroll_df.columns.astype(str).str.strip().str.replace(" ", "_")
    emp_df.columns = emp_df.columns.astype(str).str.strip().str.replace(" ", "_")
    # Filter by tenant
    payroll_df = payroll_df[payroll_df.get("tenant_id", "") == str(tenant)].copy() if not payroll_df.empty else pd.DataFrame()
    emp_df = emp_df[emp_df.get("tenant_id", "") == str(tenant)].copy() if not emp_df.empty else pd.DataFrame()

    # -----------------------------
    # 2. Payroll Calculations
    # -----------------------------
    def calculate_paye(gross):
        if gross <= 235000: return 0
        elif gross <= 335000: return (gross - 235000) * 0.10
        elif gross <= 410000: return 10000 + (gross - 335000) * 0.20
        else: return 25000 + (gross - 410000) * 0.30

    def calculate_nssf(gross):
        n5 = gross * 0.05
        n10 = gross * 0.10
        return round(n5), round(n10), round(n5+n10)

    def calculate_lst(gross):
        return round(100000/12) if gross*12 > 1200000 else 0

    def compute_payroll(basic, arrears, absent, advance, other):
        gross = round(float(basic) + float(arrears) - float(absent))
        lst = calculate_lst(gross)
        n5, n10, n15 = calculate_nssf(gross)
        paye = round(calculate_paye(gross))
        net = gross - (paye + lst + n5 + advance + other)
        return {"gross": gross, "lst": lst, "n5": n5, "n10": n10, "n15": n15, "paye": paye, "net": net}

    # -----------------------------
    # 3. Process Payroll Form
    # -----------------------------
    tab_employees, tab_process, tab_history = st.tabs([
        "🧑‍💼 Employees",
        "💳 Process Payroll",
        "📜 Payroll History"
    ])

    # -----------------------------
    # Tab 1: Employees
    # -----------------------------
    with tab_employees:
        st.subheader("Manage Employees")
        
        # Add new employee form
        with st.form("add_employee_form"):
            name = st.text_input("Employee Name")
            tin = st.text_input("TIN")
            desig = st.text_input("Designation")
            mob = st.text_input("Mobile No.")
            acc = st.text_input("Account No.")
            nssf = st.text_input("NSSF No.")
            
            if st.form_submit_button("Add Employee"):
                if not name:
                    st.error("Employee name is required")
                else:
                    emp_row = pd.DataFrame([{
                        "employee_id": str(uuid.uuid4()),
                        "employee_name": name,
                        "tin": tin,
                        "designation": desig,
                        "mob_no": mob,
                        "account_no": acc,
                        "nssf_no": nssf,
                        "tenant_id": str(st.session_state.get("tenant_id"))
                    }])
                    save_data_saas("employees", emp_row)
                    st.success(f"✅ Employee {name} added")
        
        # Show existing employees
        emp_df = get_data("employees")
        if not emp_df.empty:
            st.dataframe(emp_df[["employee_name", "designation", "tin", "mob_no"]])
        else:
            st.info("No employees added yet.")

    with tab_process:
        with st.form("payroll_form", clear_on_submit=True):
            st.subheader("👤 Employee Info")
            emp_options = emp_df.get("employee_name", []).tolist() if not emp_df.empty else []
            selected_emp = st.selectbox("Select Employee", emp_options)
            emp_record = {}
            if not emp_df.empty:
                temp = emp_df[emp_df["employee_name"] == selected_emp]
                if not temp.empty:
                    emp_record = temp.iloc[0].to_dict()

            c1, c2, c3 = st.columns(3)
            f_tin = c1.text_input("TIN", emp_record.get("tin",""))
            f_desig = c2.text_input("Designation", emp_record.get("designation",""))
            f_mob = c3.text_input("Mobile No.", emp_record.get("mob_no",""))

            c4, c5 = st.columns(2)
            f_acc = c4.text_input("Account No.", emp_record.get("account_no",""))
            f_nssf_no = c5.text_input("NSSF No.", emp_record.get("nssf_no",""))

            st.subheader("💰 Earnings & Deductions")
            c6, c7, c8 = st.columns(3)
            f_arrears = c6.number_input("Arrears", min_value=0.0)
            f_basic = c7.number_input("Basic Salary", min_value=0.0)
            f_absent = c8.number_input("Absent Deduction", min_value=0.0)

            c9, c10 = st.columns(2)
            f_adv = c9.number_input("Advance / DRS", min_value=0.0)
            f_other = c10.number_input("Other Deductions", min_value=0.0)

            if st.form_submit_button("💳 Preview & Save"):
                if not selected_emp or f_basic <= 0:
                    st.error("Enter employee and valid salary.")
                else:
                    # Check duplicate payroll for same month
                    month_str = datetime.now().strftime("%Y-%m")
                    duplicate_check = payroll_df[
                        (payroll_df.get("employee","") == selected_emp) &
                        (payroll_df.get("month","") == month_str)
                    ] if not payroll_df.empty else pd.DataFrame()

                    if not duplicate_check.empty:
                        st.warning("⚠️ Payroll for this employee already exists this month.")
                    else:
                        calc = compute_payroll(f_basic, f_arrears, f_absent, f_adv, f_other)
                        new_row = pd.DataFrame([{
                            "payroll_id": str(uuid.uuid4()),
                            "employee": selected_emp,
                            "tin": f_tin,
                            "designation": f_desig,
                            "mob_no": f_mob,
                            "account_no": f_acc,
                            "nssf_no": f_nssf_no,
                            "arrears": f_arrears,
                            "basic_salary": f_basic,
                            "absent_deduction": f_absent,
                            "gross_salary": calc['gross'],
                            "lst": calc['lst'],
                            "paye": calc['paye'],
                            "nssf_5": calc['n5'],
                            "nssf_10": calc['n10'],
                            "nssf_15": calc['n15'],
                            "advance_drs": f_adv,
                            "other_deductions": f_other,
                            "net_pay": calc['net'],
                            "date": datetime.now(),
                            "month": month_str,
                            "tenant_id": str(tenant)
                        }])
                        if save_data_saas("payroll", new_row):
                            st.success(f"✅ Payroll for {selected_emp} saved successfully!")

    # -----------------------------
    # 4. Payroll History & Reporting
    # -----------------------------
    with tab_history:
        if payroll_df.empty:
            st.info("No payroll records found.")
        else:
            # Sort latest first
            payroll_df = payroll_df.sort_values(by=["date"], ascending=False)
            st.dataframe(payroll_df, use_container_width=True)

            # Export & Printable
            csv = payroll_df.to_csv(index=False).encode("utf-8")
            st.download_button("📄 Download CSV", data=csv, file_name=f"Payroll_{datetime.now().strftime('%Y%m')}.csv")
# ==========================================
# 🚀 BALLISTIC FINTECH REPORTS ENGINE (PRODUCTION READY)
# ==========================================
import plotly.express as px
import pandas as pd
import streamlit as st
from datetime import datetime

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
    total_capital_out = col_sum(loans, "principal")
    projected_interest = col_sum(loans, "interest")
    actual_collected = col_sum(payments, "amount")

    direct_expenses = col_sum(expenses, "amount")
    nssf_tax = col_sum(payroll, "nssf_5") + col_sum(payroll, "nssf_10")
    paye_tax = col_sum(payroll, "paye")
    salary_net = col_sum(payroll, "net_pay")
    petty_out = col_sum(petty[petty.get("type") == "Out"], "amount") if not petty.empty else 0

    total_opex = direct_expenses + petty_out + nssf_tax + paye_tax + salary_net

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
    # 🧾 STATEMENTS
    # ==============================
    s1, s2 = st.columns(2)

    with s1:
        st.markdown("#### 💰 Income Statement (OpEx)")
        st.markdown(f"""
        <div style="background:#F9FAFB; padding:20px; border-radius:12px; border:1px solid #E5E7EB">
            <small>REVENUE (Projected interest)</small><br><b>UGX {projected_interest:,.0f}</b><hr>
            <small>OPERATIONAL COSTS</small><br><b>UGX {total_opex:,.0f}</b><br>
            <p style="font-size:12px; color:#666;">Includes Salaries, Taxes, Petty Cash & Admin Expenses</p>
            <h4 style="color:#1E3A8A; margin-top:10px;">TRUE NET: UGX {(projected_interest - total_opex):,.0f}</h4>
        </div>
        """, unsafe_allow_html=True)

    with s2:
        st.markdown("#### 🧾 Balance Sheet Position")
        loan_book_value = col_sum(loans, "balance")
        total_assets = cash_profit + loan_book_value
        st.markdown(f"""
        <div style="background:#F9FAFB; padding:20px; border-radius:12px; border:1px solid #E5E7EB">
            <small>CASH AT HAND</small><br><b>UGX {cash_profit:,.0f}</b><hr>
            <small>LOAN BOOK (Active Receivables)</small><br><b>UGX {loan_book_value:,.0f}</b><br>
            <p style="font-size:12px; color:#666;">Current value of all outstanding principal + interest</p>
            <h4 style="color:#059669; margin-top:10px;">TOTAL ASSETS: UGX {total_assets:,.0f}</h4>
        </div>
        """, unsafe_allow_html=True)

    # ==============================
    # 📤 DATA EXPORT
    # ==============================
    with st.expander("📥 Export Financial Data for Auditors"):
        report_data = {
            "Metric": ["Capital Out", "Interest Revenue", "Total OpEx", "Cash Profit", "Portfolio Yield %", "PAR %"],
            "Value": [total_capital_out, projected_interest, total_opex, cash_profit, f"{yield_pct:.2f}%", f"{par_ratio:.2f}%"]
        }
        export_df = pd.DataFrame(report_data)
        st.table(export_df)

        csv = export_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="⬇️ Download Full Executive Report",
            data=csv,
            file_name=f"FinReport_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
# ==============================
# 21. MASTER LEDGER (SAAS + ENTERPRISE)
# ==============================

import pandas as pd
import streamlit as st
from datetime import datetime
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from io import BytesIO


# ==============================
# PDF GENERATION BACKEND
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
# ==============================
# 22. SETTINGS & BRANDING (SAAS CONTROL CENTER)
# ==============================

import streamlit as st
import time

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
# 1. CORE PAGE FUNCTIONS (BRANDING & WIDE LAYOUT)
# ==========================================

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

# =========================================================
# FULLY RECREATED / CRASH-PROOF / PERFORMANCE VERSION
# Keeps every line, layout, feature & structure
# =========================================================

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

        # --- GLOBAL CSS UPGRADE ---
        st.markdown(f"""
        <style>
        .metric-card {{
            background:white;
            padding:20px;
            border-radius:15px;
            box-shadow:0 4px 10px rgba(0,0,0,0.05);
            transition:0.25s ease;
        }}

        .metric-card:hover {{
            transform:translateY(-3px);
            box-shadow:0 8px 16px rgba(0,0,0,0.08);
        }}

        @media (max-width:768px) {{
            .metric-card {{
                padding:14px;
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

        if "status" not in loans_df.columns:
            loans_df["status"] = "ACTIVE"

        # --- 2. ENGINE: UNIFIED CALCULATIONS ---
        loans_df["principal_n"] = safe_numeric(loans_df, ["principal", "amount"])
        loans_df["interest_n"] = safe_numeric(loans_df, ["interest", "interest_amount"])

        loans_df["balance_n"] = (
            safe_numeric(loans_df, ["balance", "total_repayable"])
            - safe_numeric(loans_df, ["amount_paid", "paid"])
        )

        total_expenses = safe_numeric(expenses_df, ["amount"]).sum()

        today = pd.Timestamp.now().normalize()

        loans_df["due_date_dt"] = safe_date(loans_df, ["end_date", "due_date"])

        overdue_mask = (
            (loans_df["due_date_dt"] < today)
            &
            (loans_df["status"].astype(str).str.upper() != "CLEARED")
        )

        overdue_count = int(overdue_mask.sum())

        total_principal = loans_df["principal_n"].sum()
        total_interest = loans_df["interest_n"].sum()

        # --- SMART ALERTS ---
        if overdue_count >= 5:
            st.warning(f"⚠️ {overdue_count} overdue loans need urgent attention.")

        # --- 3. TOP LEVEL METRIC CARDS ---
        m1, m2, m3, m4 = st.columns(4)

        def metric_card(title, value, subtitle, color, is_money=True):

            try:
                fmt = f"{float(value):,.0f} UGX" if is_money else f"{int(value)}"
            except:
                fmt = "0"

            return f"""
            <div class='metric-card' style='border-bottom:5px solid {color};'>
                <p style="color:#64748B; font-size:12px; font-weight:bold; text-transform:uppercase; margin-bottom:5px;">{title}</p>
                <h2 style="color:#1E293B; margin:0; font-size:22px;">{fmt}</h2>
                <p style="color:{color}; font-size:11px; margin-top:5px; font-weight:bold;">{subtitle}</p>
            </div>
            """

        m1.markdown(metric_card("Active principal", total_principal, "Portfolio Value", brand_color), unsafe_allow_html=True)
        m2.markdown(metric_card("interest Income", total_interest, "Expected Earnings", "#10B981"), unsafe_allow_html=True)
        m3.markdown(metric_card("Operational Costs", total_expenses, "Total Expenses", "#EF4444"), unsafe_allow_html=True)
        m4.markdown(metric_card("Critical Alerts", overdue_count, "Overdue loans", "#F59E0B", False), unsafe_allow_html=True)

        st.write("##")

        # --- 4. DATA VISUALIZATION SECTION ---
        col_l, col_r = st.columns([2, 1])

        with col_l:

            st.markdown("#### 📈 Revenue Trend vs Expenses")

            try:

                if not payments_df.empty:

                    date_col = first_existing(payments_df, ["date", "payment_date", "created_at"])
                    amt_col = first_existing(payments_df, ["amount", "paid", "payment"])

                    if date_col and amt_col:

                        payments_df["date_dt"] = pd.to_datetime(payments_df[date_col], errors="coerce")
                        payments_df["amount_n"] = pd.to_numeric(payments_df[amt_col], errors="coerce").fillna(0)

                        temp = payments_df.dropna(subset=["date_dt"]).copy()

                        temp["month"] = temp["date_dt"].dt.to_period("M").astype(str)

                        monthly_rev = temp.groupby("month", as_index=False)["amount_n"].sum()

                        fig = px.area(
                            monthly_rev,
                            x="month",
                            y="amount_n",
                            template="plotly_white",
                            color_discrete_sequence=[brand_color]
                        )

                        fig.update_layout(
                            height=320,
                            margin=dict(l=0, r=0, t=20, b=0)
                        )

                        st.plotly_chart(fig, use_container_width=True)

                    else:
                        st.info("Payment columns missing.")

                else:
                    st.info("Insufficient payment history for trend analysis.")

            except:
                st.warning("Revenue chart temporarily unavailable.")

        with col_r:

            st.markdown("#### 🎯 Portfolio Health")

            try:

                status_data = (
                    loans_df["status"]
                    .astype(str)
                    .str.upper()
                    .value_counts()
                    .reset_index()
                )

                status_data.columns = ["status", "count"]

                fig_pie = px.pie(
                    status_data,
                    names="status",
                    values="count",
                    hole=0.72,
                    color_discrete_sequence=[
                        "#10B981",
                        "#F59E0B",
                        "#EF4444",
                        brand_color
                    ]
                )

                fig_pie.update_layout(
                    height=320,
                    showlegend=False
                )

                st.plotly_chart(fig_pie, use_container_width=True)

            except:
                st.info("Portfolio chart unavailable.")

        # --- 5. ACTIVITY FEEDS ---
        st.write("---")

        t1, t2 = st.columns(2)

        with t1:

            st.markdown("#### 📊 Portfolio Growth vs. interest")

            try:

                graph_df = loans_df.copy()

                graph_df["date_dt"] = safe_date(graph_df, ["start_date", "created_at"])

                graph_df = graph_df.dropna(subset=["date_dt"])

                if not graph_df.empty:

                    timeline_df = (
                        graph_df
                        .groupby("date_dt")[["principal_n", "interest_n"]]
                        .sum()
                        .sort_index()
                        .cumsum()
                        .reset_index()
                    )

                    fig_portfolio = px.line(
                        timeline_df,
                        x="date_dt",
                        y=["principal_n", "interest_n"],
                        template="plotly_white",
                        color_discrete_map={
                            "principal_n": brand_color,
                            "interest_n": "#10B981"
                        }
                    )

                    fig_portfolio.update_layout(
                        height=350,
                        hovermode="x unified"
                    )

                    st.plotly_chart(fig_portfolio, use_container_width=True)

                else:
                    st.info("Not enough dated records to generate a trend.")

            except:
                st.info("Growth chart unavailable.")

        with t2:

            st.markdown("#### 💸 Latest Expenses")

            try:

                if not expenses_df.empty:

                    display_exp = expenses_df.head(5)

                    vals = safe_numeric(display_exp, ["amount"]).tolist()

                    rows = ""

                    for i, (_, r) in enumerate(display_exp.iterrows()):

                        amount_val = vals[i] if i < len(vals) else 0

                        rows += f"""
                        <tr style='border-bottom:1px solid #f0f0f0;'>
                            <td style='padding:12px 5px;'>{r.get('category','General')}</td>
                            <td style='padding:12px 5px; text-align:right; color:#EF4444;'>-{amount_val:,.0f}</td>
                            <td style='padding:12px 5px; text-align:right; color:#64748B;'>{r.get('date','-')}</td>
                        </tr>
                        """

                    st.markdown(
                        f"<table style='width:100%; font-size:13px;'>{rows}</table>",
                        unsafe_allow_html=True
                    )

                else:
                    st.info("No recorded expenses.")

            except:
                st.info("Expenses feed unavailable.")

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
                "⬇️ Export Expenses CSV",
                csv2,
                file_name="expenses_report.csv",
                mime="text/csv",
                use_container_width=True
            )

    except Exception as e:

        st.error(f"Dashboard recovered from an internal issue: {str(e)}")
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
                
            elif page == "Petty Cash":
                show_petty_cash()
                
            elif page == "Overdue Tracker":
                show_overdue_tracker()
                
            elif page == "Payroll":
                show_payroll_enterprise()
                
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
