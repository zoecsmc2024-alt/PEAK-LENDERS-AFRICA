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
