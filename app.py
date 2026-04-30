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

# ============================================================
# ⚙️ CONFIGURATION (Must be the very first Streamlit command)
# ============================================================
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
# Ensure these environment variables are set in your secrets
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
    # Note: Ensure 'cookie_manager' is initialized before calling this
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

        # These functions must be defined in your logic
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
    # Fallback color if "theme_color" isn't set yet
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
        # Check secrets first, then environment variables
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

        # ensure require_tenant() and get_tenant_id() are defined in your helper section
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

# ============================================================
# 🚀 6. SESSION CREATION & SECURITY
# ============================================================
def create_session(user_data, remember_me=False):
    """Initializes the session state variables upon successful login."""
    st.session_state.update({
        "logged_in": True,
        "authenticated": True,
        "user_id": user_data["user_id"],
        "tenant_id": user_data["tenant_id"],
        "role": user_data["role"],
        "company": user_data["company"],
        "last_activity": datetime.now(),
        "view": "dashboard"
    })

    if remember_me:
        st.session_state["remember"] = True

    st.success(f"Welcome back to {user_data['company']}!")
    time.sleep(1) # Brief pause for UX
    st.rerun()

def check_session_timeout():
    """Monitor inactivity and logs out user after SESSION_TIMEOUT minutes."""
    if not st.session_state.get("logged_in"):
        return

    last_activity = st.session_state.get("last_activity", datetime.now())
    idle_time = (datetime.now() - last_activity).total_seconds() / 60

    if idle_time > SESSION_TIMEOUT:
        # ✅ Selective cleanup (keep non-sensitive UI states, clear auth)
        auth_keys = ["logged_in", "authenticated", "user_id", "tenant_id", "company", "auth_session"]
        for key in auth_keys:
            st.session_state.pop(key, None)

        st.warning("Session timed out due to inactivity. Please log in again.")
        st.stop()

    # Update timestamp for current interaction
    st.session_state["last_activity"] = datetime.now()

# ============================================================
# 🛡️ 7. RATE LIMITING (Brute Force Protection)
# ============================================================
MAX_ATTEMPTS = 5
LOCKOUT_MINUTES = 10

def check_rate_limit(email):
    """Checks if the user is currently locked out."""
    attempts = st.session_state.get("login_attempts", {})
    if email in attempts:
        count, last_time = attempts[email]
        if count >= MAX_ATTEMPTS and (datetime.now() - last_time) < timedelta(minutes=LOCKOUT_MINUTES):
            return False
    return True

def record_failed_attempt(email):
    """Increments the failed login counter for an email."""
    attempts = st.session_state.setdefault("login_attempts", {})
    count, _ = attempts.get(email, (0, datetime.now()))
    attempts[email] = (count + 1, datetime.now())

# ============================================================
# 🏢 10. TENANT UTILITIES
# ============================================================
def tenant_filter(df):
    """Filters any dataframe to only show records for the active tenant."""
    if df is None or df.empty:
        return df
    if "tenant_id" not in df.columns:
        return df
    
    current_tenant = st.session_state.get("tenant_id")
    return df[df["tenant_id"] == current_tenant].copy()

# ============================================================
# 🏭 ADMIN COMPANY REGISTRATION
# ============================================================
def admin_company_registration(supabase):
    st.header("🏢 Register Your Company")
    
    with st.form("company_reg_form", clear_on_submit=False):
        company_name = st.text_input("Organization name")
        admin_name = st.text_input("Admin Full name")
        email = st.text_input("Business Email").strip().lower()
        password = st.text_input("Password", type="password", help="Minimum 6 characters")
        submit = st.form_submit_button("Create Organization", use_container_width=True)

    if submit:
        if not all([company_name, email, password, admin_name]):
            st.error("Please fill in all fields.")
            return

        try:
            # 1. Sign up the user in Supabase Auth
            res = supabase.auth.sign_up({"email": email, "password": password})
            
            if res.user:
                tenant_id = str(uuid.uuid4())
                # 2. Create the Tenant entry
                # We use a random code for the "Company Code" security check
                comp_code = f"{company_name[:3].upper()}{random.randint(100, 999)}"
                
                supabase.table("tenants").insert({
                    "id": tenant_id,
                    "name": company_name,
                    "company_code": comp_code
                }).execute()

                # 3. Create the User profile linked to Tenant
                supabase.table("users").insert({
                    "id": res.user.id,
                    "name": admin_name,
                    "email": email,
                    "tenant_id": tenant_id,
                    "role": "Admin"
                }).execute()

                st.success(f"✅ {company_name} registered! Your Company Code is: **{comp_code}**")
                st.info("Please login to continue.")
                st.session_state["view"] = "login"
                st.rerun()
        except Exception as e:
            st.error(f"Registration Error: {e}")

# ============================================================
# 👥 STAFF SIGNUP
# ============================================================
def view_staff_signup(supabase):
    st.header("🆕 Join Organization")
    st.info("Ask your Admin for the exact Business name registered.")
    
    with st.form("staff_form"):
        company_input = st.text_input("Company name to Join").strip()
        name = st.text_input("Your Full name")
        email = st.text_input("Email").strip().lower()
        pwd = st.text_input("Password", type="password")
        submit = st.form_submit_button("Request Access", use_container_width=True)

    if submit:
        try:
            # 1. Find the company (Case-insensitive)
            tenant_query = supabase.table("tenants").select("id").ilike("name", company_input).execute()

            if not tenant_query.data:
                st.error("Company not found. Please verify the name with your administrator.")
                return

            t_id = tenant_query.data[0]["id"]

            # 2. Create Auth Account
            res = supabase.auth.sign_up({"email": email, "password": pwd})

            if res.user:
                # 3. Create Profile linked to found Tenant
                supabase.table("users").insert({
                    "id": res.user.id,
                    "name": name,
                    "email": email,
                    "tenant_id": t_id,
                    "role": "Staff"
                }).execute()

                st.success("Account created! Please log in.")
                st.session_state["view"] = "login"
                st.rerun()
        except Exception as e:
            st.error(f"Signup Error: {e}")

# ============================================================
# 🔑 LOGIN PAGE
# ============================================================
def login_page(supabase):
    st.markdown("## PEAK-LENDERS AFRICA 💰")
    
    with st.container():
        company_name = st.text_input("Business name").strip()
        email = st.text_input("Email").strip().lower()
        pwd = st.text_input("Password", type="password")
        
        if st.button("Access Dashboard", use_container_width=True, type="primary"):
            if not all([company_name, email, pwd]):
                st.error("Please fill in all fields.")
            else:
                try:
                    # 1. Identity Check
                    auth_res = supabase.auth.sign_in_with_password({"email": email, "password": pwd})
                    
                    if auth_res.user:
                        st.session_state["auth_session"] = auth_res.session

                        # 2. Profile & Tenant Validation
                        user_query = supabase.table("users")\
                            .select("*, tenants(name)")\
                            .eq("id", auth_res.user.id)\
                            .execute()

                        if user_query.data:
                            user = user_query.data[0]
                            db_company = user.get("tenants", {}).get("name", "").strip().lower()

                            # Security: Ensure input matches DB record
                            if db_company != company_name.lower():
                                st.error(f"Membership Error: User not linked to '{company_name}'")
                                return

                            # 3. Set Session
                            st.session_state.update({
                                "authenticated": True,
                                "logged_in": True,
                                "user_id": user["id"],
                                "tenant_id": user["tenant_id"],
                                "company": user.get("tenants", {}).get("name"),
                                "role": user.get("role", "Staff"),
                                "last_activity": datetime.now(),
                                "current_page": "Overview" 
                            })
                            st.success("Access Granted!")
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("Profile not found. Please contact your administrator.")
                except Exception as e:
                    st.error(f"Login failed: {e}")

        st.markdown("---")
        
        # Navigation to Registration
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🏢 Register Company", use_container_width=True):
                st.session_state["view"] = "create_company"
                st.rerun()
        with col2:
            if st.button("🆕 Staff Signup", use_container_width=True):
                st.session_state["view"] = "signup"
                st.rerun()

# ============================================================
# 🚦 AUTH ROUTER
# ============================================================
def run_auth_ui(supabase):
    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    # Show 'Back' button only on registration pages
    if st.session_state["view"] != "login":
        if st.button("⬅️ Back to Login"):
            st.session_state["view"] = "login"
            st.rerun()

    # View Switching Logic
    current_view = st.session_state["view"]
    if current_view == "login":
        login_page(supabase)
    elif current_view == "signup":
        view_staff_signup(supabase)
    elif current_view == "create_company":
        admin_company_registration(supabase)




def render_sidebar():
    """Renders the business selector, branding, and main navigation."""
    # ==============================
    # 1. FETCH TENANTS (CENTRALIZED)
    # ==============================
    try:
        # We fetch all tenants so the user can see their available businesses
        tenants_res = supabase.table("tenants")\
            .select("id, name, brand_color, logo_url")\
            .execute()

        tenant_map = {
            row['name']: row for row in tenants_res.data
        } if tenants_res.data else {}

    except Exception as e:
        st.sidebar.error(f"Error fetching tenants: {e}")
        tenant_map = {}

    current_tenant_id = st.session_state.get('tenant_id')

    # ==============================
    # 2. SIDEBAR UI & SELECTOR
    # ==============================
    with st.sidebar:
        st.markdown('<div style="padding-top:10px;"></div>', unsafe_allow_html=True)

        if tenant_map:
            options = list(tenant_map.keys())
            
            # Find the index of the currently active tenant
            default_index = 0
            if current_tenant_id:
                for i, name in enumerate(options):
                    if str(tenant_map[name]['id']) == str(current_tenant_id):
                        default_index = i
                        break

            active_company_name = st.selectbox(
                "🏢 Business Portal",
                options,
                index=default_index,
                key="sidebar_portal_select"
            )

            active_company = tenant_map.get(active_company_name)

            # 🔁 TENANT SYNC: If user switches business in the dropdown
            if active_company:
                if str(st.session_state.get('tenant_id')) != str(active_company['id']):
                    st.session_state.update({
                        'tenant_id': active_company['id'],
                        'theme_color': active_company.get('brand_color', '#1E3A8A'),
                        'company': active_company.get('name')
                    })
                    # Clear cache to ensure new business data is loaded
                    st.cache_data.clear()
                    st.rerun()
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
import streamlit as st
import pandas as pd
import numpy as np
import uuid
from datetime import datetime

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
    # DATE CLEANUP
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
    # SMART STATUS LOGIC (NOW CORRECT)
    # ------------------------------
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip().str.upper()
    loans_df["status"] = "CLEARED"
    loans_df.loc[loans_df["balance"] > 0, "status"] = "ACTIVE"
    
    # Process each SN family separately
    for sn_val, grp in loans_df.groupby("sn"):
        # Only open loans within this specific family
        open_grp = grp[grp["balance"] > 0].copy()
    
        if open_grp.empty:
            continue
    
        # Sort within family to find the absolute latest row
        open_grp = open_grp.sort_values(
            by=["cycle_no", "start_date", "id"]
        )
    
        latest_row = open_grp.iloc[-1]
        latest_id = latest_row["id"]
        latest_cycle = int(latest_row["cycle_no"])
    
        older_ids = open_grp.iloc[:-1]["id"].tolist()
    
        # Mark all unpaid historical links as BCF
        if older_ids:
            loans_df.loc[
                loans_df["id"].isin(older_ids),
                "status"
            ] = "BCF"
    
        # Mark the current active link
        if latest_cycle == 1:
            loans_df.loc[
                loans_df["id"] == latest_id,
                "status"
            ] = "ACTIVE"
        else:
            # Multi-cycle loans (Cycle 2+) are PENDING until processed
            loans_df.loc[
                loans_df["id"] == latest_id,
                "status"
            ] = "PENDING"
    
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
    # ------------------------------
    # BORROWER MAP
    # ------------------------------
    if not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(
            borrowers_df["id"],
            borrowers_df["name"]
        ))
        loans_df["borrower"] = loans_df["borrower_id"].map(
            bor_map
        ).fillna("Unknown")

    # ------------------------------
    # ACTIVE BORROWERS
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
            "🔍 Search Loan / Borrower",
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

            st.markdown("---")

        # ------------------------------
        # 📋 LOAN DATA TABLE
        # ------------------------------
        if filtered_loans.empty:
            st.warning("No matching loans found.")
        else:
            show_cols = [
                "sn",
                "loan_id_label",
                "borrower",
                "cycle_no",
                "principal",
                "total_repayable",
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
                    "Select Borrower",
                    list(borrower_map.keys())
                )

                selected_id = str(
                    borrower_map[selected_name]
                ).strip()

                amount = col1.number_input(
                    "Principal Amount (UGX)",
                    min_value=0,
                    step=50000
                )

                date_issued = col1.date_input(
                    "Start Date",
                    value=datetime.now()
                )

                loan_type = col2.selectbox(
                    "Loan Type",
                    ["Business", "Personal", "Emergency", "Other"]
                )

                interest_rate = col2.number_input(
                    "Monthly Interest Rate (%)",
                    min_value=0.0,
                    step=0.5
                )

                date_due = col2.date_input(
                    "Due Date",
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
                        st.error("Borrower ID missing.")
                        st.stop()

                    loan_data = {
                        "id": str(uuid.uuid4()),
                        "sn": "",
                        "loan_id_label": "",
                        "parent_loan_id": None,
                        "borrower_id": selected_id,
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
                    "principal": unpaid,
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
      
    
import pandas as pd
from datetime import datetime
import uuid
import streamlit as st
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

# ==============================
# 🧾 RECEIPT GENERATION
# ==============================
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
# 💵 PAYMENTS MODULE
# ==============================
def show_payments():
    st.markdown("## 💵 Payments Management")

    # ==============================
    # 📦 LOAD DATA (SAFE)
    # ==============================
    try:
        # Utilizing your global get_data/get_cached_data helpers
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
    loans_df["id"] = loans_df["id"].astype(str)
    borrowers_df["id"] = borrowers_df["id"].astype(str)

    if "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].astype(str)
    # ==============================
    # 🛡️ NORMALIZATION
    # ==============================
    for df in [loans_df, payments_df, borrowers_df]:
        if not df.empty:
            df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

    # Ensure IDs are strings for mapping
    for df, col in [(borrowers_df, "id"), (loans_df, "borrower_id"), (loans_df, "id"), (payments_df, "loan_id")]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # ==============================
    # 👤 borrower name RESOLUTION
    # ==============================
    if not borrowers_df.empty and "id" in borrowers_df.columns and "name" in borrowers_df.columns:
        borrower_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].map(borrower_map).fillna("Unknown")
    else:
        if "borrower" not in loans_df.columns:
            loans_df["borrower"] = "Unknown"

    # ==============================
    # 📊 FIELD PREP
    # ==============================
    if "status" not in loans_df.columns:
        loans_df["status"] = "ACTIVE"
    loans_df["status"] = loans_df["status"].astype(str).str.upper()

    loans_df["amount_paid"] = pd.to_numeric(loans_df.get("amount_paid", 0), errors="coerce").fillna(0)
    loans_df["total_repayable"] = pd.to_numeric(loans_df.get("total_repayable", 0), errors="coerce").fillna(0)

    # ==============================
    # 🔥 CRITICAL FIX: SYNC PAYMENTS → loans
    # ==============================
    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(payments_df.get("amount", 0), errors="coerce").fillna(0)
        payment_sums = payments_df.groupby("loan_id")["amount"].sum().to_dict()
        # Recalculate to ensure absolute accuracy after any Edits/Deletes
        loans_df["amount_paid"] = loans_df["id"].map(payment_sums).fillna(0)

    # ==============================
    # 📑 TABS
    # ==============================
    tab1, tab2 = st.tabs(["➕ Record Payment", "📜 History"])

    # --- TAB 1: RECORD PAYMENT ---
    with tab1:
        active_loans = loans_df[loans_df["status"].str.upper().isin(["ACTIVE", "BCF", "PENDING", "OVERDUE"])].copy()

        if active_loans.empty:
            st.success("🎉 No active loans requiring payment.")
        else:
            search = st.text_input("🔍 Search borrower or loan")
            if search:
                search = search.lower()
                active_loans = active_loans[
                    active_loans.apply(lambda r: search in str(r.get("borrower", "")).lower() 
                    or search in str(r.get("sn", "")).lower(), axis=1) 
                ]

            if active_loans.empty:
                st.warning("No matching loans found.")
            else:
                # ------------------------------
                # 🧾 LOAN CARD SELECTOR (SAFE DROP-IN)
                # ------------------------------
                active_loans = active_loans.sort_values(
                    by=["sn", "cycle_no", "end_date"]
                )

                st.markdown("### 📌 Select Loan to Pay")

                for _, row in active_loans.iterrows():
                    total = float(row.get("total_repayable", 0))
                    paid = float(row.get("amount_paid", 0))
                    balance = total - paid

                    # ------------------------------
                    # ⏰ OVERDUE CHECK
                    # ------------------------------
                    try:
                        due = pd.to_datetime(row.get("end_date"))
                        overdue = due < datetime.now() and balance > 0
                    except:
                        overdue = False

                    progress = 0 if total == 0 else min(int((paid / total) * 100), 100)

                    # ------------------------------
                    # 🎨 STYLE
                    # ------------------------------
                    color = "#10b981" # Green
                    if overdue:
                        color = "#ef4444" # Red
                    elif balance <= 0:
                        color = "#3b82f6" # Blue
                    elif row["status"] == "PENDING":
                        color = "#f59e0b" # Orange

                    # ------------------------------
                    # 🧾 CARD BUTTON
                    # ------------------------------
                    # Using columns to put button and visual card together
                    clicked = st.button(
                        f"Select: {row['borrower']} (SN:{row['sn']})",
                        key=f"loan_btn_{row['id']}",
                        use_container_width=True
                    )

                    st.markdown(
                        f"""
                        <div style="
                            border:2px solid {color};
                            border-radius:10px;
                            padding:10px;
                            margin-bottom:15px;
                            background:#0f172a;
                            color:white;
                        ">
                            <b style="font-size:16px;">{row['borrower']}</b> • SN {row['sn']}<br>
                            <span style="color:#94a3b8; font-size:12px;">Cycle {row.get('cycle_no',1)}</span><br>
                            <div style="margin-top:8px;">
                                💰 {paid:,.0f} / {total:,.0f} UGX<br>
                                ⚖️ Balance: <b>{balance:,.0f}</b><br>
                                📅 Due: {str(row.get('end_date',''))[:10]} {'⚠️ <span style="color:#ef4444;font-weight:bold;">OVERDUE</span>' if overdue else ''}
                            </div>

                            <!-- Progress Bar Container with overflow:hidden to stop leaks -->
                            <div style="background:#1e293b; border-radius:8px; margin-top:10px; height:8px; overflow:hidden;">
                                <div style="
                                    width:{progress}%;
                                    background:{color};
                                    height:8px;
                                    transition: width 0.5s ease-in-out;
                                "></div>
                            </div>
                            <small style="color:#94a3b8;">{progress}% repaid</small>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    if clicked:
                        st.session_state["selected_loan_id"] = row["id"]

                # ------------------------------
                # 🎯 PROCESS SELECTION
                # ------------------------------
                if "selected_loan_id" not in st.session_state:
                    st.info("👉 Click a 'Select' button above to continue")
                else:
                    loan_id = st.session_state["selected_loan_id"]
                    # Ensure the selected loan still exists in current filtered view
                    match = active_loans[active_loans["id"] == loan_id]
                    
                    if not match.empty:
                        loan = match.iloc[0]
                        st.success(f"Selected: {loan['borrower']} (ID: {loan_id})")
                        # You can place your payment input form here
                    else:
                        st.session_state.pop("selected_loan_id")
                        st.rerun()

                total = float(loan["total_repayable"])
                paid = float(loan["amount_paid"])
                balance = total - paid

                c1, c2 = st.columns(2)
                c1.metric("👤 borrower", loan["borrower"])
                c2.metric("💰 balance", f"UGX {balance:,.0f}")

                with st.form("payment_form"):
                    amount = st.number_input("Amount", min_value=0.0, step=1000.0)
                    method = st.selectbox("Method", ["Cash", "Mobile Money", "Bank"])
                    date = st.date_input("Date", datetime.now())
                    submit = st.form_submit_button("Post Payment", use_container_width=True)

                if submit:
                    if amount <= 0:
                        st.warning("Enter valid payment amount.")
                    else:
                        try:
                            tenant_id = st.session_state.get("tenant_id")
                            if not tenant_id:
                                st.error("❌ Authentication error. Please login again.")
                                st.stop()

                            receipt_no = generate_receipt_no(supabase, tenant_id)
                            current_label = str(loan.get("loan_id_label", loan["sn"]))

                            # STEP 1: INSERT PAYMENT RECORD
                            supabase.table("payments").insert({
                                "receipt_no": receipt_no,
                                "loan_id": loan_id,
                                "borrower": loan["borrower"],
                                "amount": float(amount),
                                "date": date.strftime("%Y-%m-%d"),
                                "method": method,
                                "recorded_by": st.session_state.get("user", "Staff"),
                                "tenant_id": tenant_id
                            }).execute()

                            # STEP 2: UPDATE MASTER LOAN RECORD
                            new_paid = paid + amount
                            new_status = "CLEARED" if new_paid >= total else loan["status"]

                            supabase.table("loans").update({
                                "amount_paid": float(new_paid),
                                "status": new_status,
                                "loan_id_label": current_label
                            }).eq("id", loan_id).execute()

                            # STEP 3: GENERATE PDF RECEIPT
                            file_path = f"/tmp/{receipt_no}.pdf"
                            generate_receipt_pdf({
                                "Receipt No": receipt_no,
                                "borrower": loan["borrower"],
                                "Amount": f"UGX {amount:,.0f}",
                                "Method": method,
                                "Date": date.strftime("%Y-%m-%d"),
                                "Recorded By": st.session_state.get("user", "Staff")
                            }, file_path)

                            with open(file_path, "rb") as f:
                                st.session_state["receipt_pdf"] = f.read()
                                st.session_state["show_receipt"] = True

                            st.success(f"✅ Payment Success! New balance: {max(0, total-new_paid):,.0f}")
                            st.cache_data.clear()
                            st.rerun()

                        except Exception as e:
                            st.error(f"❌ Error: {e}")

        if st.session_state.get("show_receipt"):
            st.download_button(
                "📥 Download Receipt",
                data=st.session_state["receipt_pdf"],
                file_name=f"receipt_{datetime.now().strftime('%Y%m%d')}.pdf",
                mime="application/pdf",
                use_container_width=True
            )
            if st.button("Clear Receipt View"):
                st.session_state["show_receipt"] = False
                st.rerun()

    # --- TAB 2: HISTORY & MANAGEMENT ---
    with tab2:
        if payments_df.empty:
            st.info("No payment history found.")
        else:
            df_hist = payments_df.copy()
            df_hist["amount_display"] = df_hist["amount"].apply(lambda x: f"UGX {x:,.0f}")
            if "date" in df_hist.columns:
                df_hist = df_hist.sort_values("date", ascending=False)

            display_cols = ["date", "borrower", "amount_display", "method", "receipt_no"]
            st.dataframe(df_hist[display_cols], use_container_width=True, hide_index=True)

            # --- 🛠️ EDIT/DELETE MANAGEMENT ---
            st.markdown("---")
            st.markdown("### ⚙️ Payment Maintenance")
            
            pay_map = {f"{row['receipt_no']} | {row['borrower']}": row['id'] for _, row in df_hist.iterrows()}
            selected_pay = st.selectbox("Choose Payment to Modify", list(pay_map.keys()))
            
            target_pay_id = pay_map[selected_pay]
            target_pay = df_hist[df_hist['id'] == target_pay_id].iloc[0]

            p_col1, p_col2 = st.columns(2)

            if p_col1.button("🗑️ Delete Payment", use_container_width=True):
                try:
                    supabase.table("payments").delete().eq("id", target_pay_id).execute()
                    st.cache_data.clear() # Triggers the Sync logic at top
                    st.warning(f"Payment {target_pay['receipt_no']} removed.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")

            if p_col2.button("📝 Edit Payment", use_container_width=True):
                st.session_state["edit_pay_mode"] = True

            if st.session_state.get("edit_pay_mode"):
                with st.form("edit_payment_form"):
                    st.info(f"Modifying: {target_pay['receipt_no']}")
                    new_amt = st.number_input("Revised Amount", value=float(target_pay['amount']))
                    new_method = st.selectbox("Revised Method", ["Cash", "Mobile Money", "Bank"], 
                                             index=["Cash", "Mobile Money", "Bank"].index(target_pay['method']))
                    
                    eb1, eb2 = st.columns(2)
                    if eb1.form_submit_button("💾 Save Changes"):
                        try:
                            supabase.table("payments").update({
                                "amount": new_amt,
                                "method": new_method
                            }).eq("id", target_pay_id).execute()
                            st.session_state["edit_pay_mode"] = False
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Update failed: {e}")
                    
                    if eb2.form_submit_button("❌ Cancel"):
                        st.session_state["edit_pay_mode"] = False
                        st.rerun()
# ==============================
# 🛡️ 15. COLLATERAL MANAGEMENT PAGE (SAAS + ENTERPRISE UPGRADE)
# ==============================
import mimetypes
from datetime import datetime
import pandas as pd
import streamlit as st

def show_collateral():
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    # ==============================
    # 🔐 SAFETY CHECK (HARDENED)
    # ==============================
    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    st.markdown(f"<h2 style='color: {brand_color};'>🛡️ Collateral & Security</h2>", unsafe_allow_html=True)

    # ==============================
    # 📦 FETCH DATA (SAFE ADAPTERS)
    # ==============================
    collateral_df = get_data("collateral") 
    loans_df = get_data("loans")

    # ==============================
    # 🔍 FILTER ELIGIBLE loans
    # ==============================
    if loans_df is not None and not loans_df.empty:
        # Standardize columns to avoid case-sensitivity issues
        loans_df.columns = loans_df.columns.str.lower()
        # Filter for loans that actually need collateral coverage
        active_statuses = ["active", "overdue", "pending", "bcf"]
        available_loans = loans_df[loans_df["status"].str.lower().isin(active_statuses)].copy()
    else:
        available_loans = pd.DataFrame()

    # --- TABS ---
    tab_reg, tab_view = st.tabs(["➕ Register Asset", "📋 Inventory & status"])

    # ==============================
    # ➕ TAB 1: REGISTER ASSET
    # ==============================
    with tab_reg:
        if available_loans.empty:
            st.info("ℹ️ No Active or Overdue loans found to attach collateral to.")
        else:
            with st.form("collateral_reg_form", clear_on_submit=True):
                st.write("### Link Asset to Loan")
                c1, c2 = st.columns(2)

                # Robust label generation using the standardized columns
                loan_labels = available_loans.apply(
                    lambda x: f"{x['borrower']} (ID: {x['id']})", axis=1
                ).tolist()
                
                selected_label = c1.selectbox("Select Loan/borrower", options=loan_labels)
                asset_type = c2.selectbox(
                    "Asset type",
                    ["Logbook (Car)", "Land Title", "Electronics", "House Deed", "Business Stock", "Other"]
                )

                desc = st.text_input("Detailed Asset Description (e.g. Plate No, Plot No)")
                est_value = st.number_input("Estimated Market Value (UGX)", min_value=0, step=100000)
                
                st.markdown("---")
                # Photo upload placeholder (Linked to future Supabase Storage integration)
                uploaded_photo = st.file_uploader("Upload Asset Photo (Verification)", type=["jpg", "png", "jpeg"])

                if st.form_submit_button("💾 Save & Secure Asset", use_container_width=True):
                    if not desc or est_value <= 0:
                        st.error("❌ Please provide a description and valid market value.")
                    else:
                        # Logic to extract the specific DB ID from the dropdown label
                        actual_loan_id = selected_label.split("(ID: ")[1].replace(")", "")
                        borrower_name = selected_label.split(" (ID:")[0]

                        new_asset = pd.DataFrame([{
                            "loan_id": actual_loan_id,
                            "tenant_id": str(current_tenant),
                            "borrower": borrower_name,
                            "type": asset_type,
                            "description": desc,
                            "value": float(est_value),
                            "status": "In Custody",
                            "date_added": datetime.now().strftime("%Y-%m-%d")
                        }])

                        if save_data_saas("collateral", new_asset):
                            st.success(f"✅ Asset secured for {borrower_name}!")
                            st.cache_data.clear()
                            st.rerun()

    # ==============================
    # 📋 TAB 2: INVENTORY & status
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
            
            # Formatting for display
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
            # ⚙️ ASSET MANAGEMENT (RELEASE/DISPOSE)
            # ==============================
            st.markdown("---")
            with st.expander("🛠️ Manage Asset Lifecycle"):
                # Filter for assets still under control
                manageable = collateral_df[collateral_df["status"] != "Released"].copy()
                
                if manageable.empty:
                    st.write("All collateral assets have been released.")
                else:
                    asset_labels = manageable.apply(lambda x: f"{x['borrower']} - {x['description']}", axis=1).tolist()
                    asset_to_manage = st.selectbox("Select Asset to Update", options=asset_labels)
                    
                    # Target specific ID for update
                    selected_idx = manageable.index[manageable.apply(lambda x: f"{x['borrower']} - {x['description']}", axis=1) == asset_to_manage][0]
                    asset_id = manageable.at[selected_idx, "id"]

                    col_stat, col_btn = st.columns([2,1])
                    new_stat = col_stat.selectbox(
                        "Set New status", 
                        ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"]
                    )
                    
                    if col_btn.button("Update status", use_container_width=True):
                        # Constructing the update payload
                        update_row = pd.DataFrame([{
                            "id": asset_id, 
                            "status": new_stat,
                            "tenant_id": str(current_tenant)
                        }])
                        
                        if save_data_saas("collateral", update_row):
                            st.success("✅ Asset status updated successfully!")
                            st.cache_data.clear()
                            st.rerun()
            

# ==============================
# 📅 17. ACTIVITY CALENDAR PAGE
# ==============================
from streamlit_calendar import calendar

def show_calendar():
    st.markdown("<h2 style='color: #2B3F87;'>📅 Activity Calendar</h2>", unsafe_allow_html=True)

    # 1. FETCH DATA (SAFE ADAPTERS)
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    # --- 👤 INJECT borrower nameS (MAPPING) ---
    if borrowers_df is not None and not borrowers_df.empty:
        # Standardizing ID columns for a perfect match
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
    
    # Filter for active/pending/overdue loans for the calendar view
    active_loans = loans_df[~loans_df["status"].astype(str).str.upper().isin(["CLEARED", "CLOSED"])].copy()

    # --- 🎨 VISUAL CALENDAR WIDGET ---
    calendar_events = []
    for _, r in active_loans.iterrows():
        if pd.notna(r['end_date']):
            # Color logic: Red for overdue, Blue for upcoming
            is_overdue = r['end_date'].date() < today.date()
            ev_color = "#FF4B4B" if is_overdue else "#4A90E2"
            
            # Construct rich event title
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

    # Render the interactive calendar widget
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

    # 3. 📈 REVENUE FORECAST (THIS MONTH)
    st.markdown("---")
    st.markdown("<h4 style='color: #2B3F87;'>📊 Revenue Forecast (This Month)</h4>", unsafe_allow_html=True)
    
    this_month_df = active_loans[active_loans["end_date"].dt.month == today.month]
    total_expected = this_month_df["total_repayable"].sum()
    
    f1, f2 = st.columns(2)
    f1.metric("Expected Collections", f"{total_expected:,.0f} UGX")
    f2.metric("Deadlines This Month", len(this_month_df))

    # 4. 📌 ACTION ITEMS (TODAY)
    st.markdown("<h4 style='color: #2B3F87;'>📌 Action Items for Today</h4>", unsafe_allow_html=True)
    if due_today_df.empty:
        st.success("✨ No collection deadlines for today.")
    else:
        today_rows = "".join([f"""
            <tr style="background:#F0F8FF;">
                <td style="padding:10px;"><b>#{r.get('loan_id_label', r['id'][:8])}</b></td>
                <td style="padding:10px;">{r['borrower']}</td>
                <td style="padding:10px;text-align:right;">{r['total_repayable']:,.0f}</td>
                <td style="padding:10px;text-align:center;">
                    <span style="background:#2B3F87;color:white;padding:2px 8px;border-radius:10px;font-size:10px;">💰 COLLECT NOW</span>
                </td>
            </tr>""" for _, r in due_today_df.iterrows()])
            
        st.markdown(f"""
            <div style="border:2px solid #2B3F87;border-radius:10px;overflow:hidden;">
                <table style="width:100%;border-collapse:collapse;font-size:12px;">
                    <tr style="background:#2B3F87;color:white;">
                        <th style="padding:10px;">Loan ID</th>
                        <th style="padding:10px;">borrower</th>
                        <th style="padding:10px;text-align:right;">Amount</th>
                        <th style="padding:10px;text-align:center;">Action</th>
                    </tr>
                    {today_rows}
                </table>
            </div>""", unsafe_allow_html=True)

    # 5. 🔴 OVERDUE FOLLOW-UP
    st.markdown("<br><h4 style='color: #FF4B4B;'>🔴 Overdue Follow-up</h4>", unsafe_allow_html=True)
    overdue_df = active_loans[active_loans["end_date"] < today].copy()
    if not overdue_df.empty:
        overdue_df["days_late"] = (today - overdue_df["end_date"]).dt.days
        od_rows = ""
        for _, r in overdue_df.iterrows():
            late_color = "#FF4B4B" if r['days_late'] > 7 else "#FFA500"
            od_rows += f"""
                <tr style="background:#FFF5F5;">
                    <td style="padding:10px;"><b>#{r.get('loan_id_label', r['id'][:8])}</b></td>
                    <td style="padding:10px;">{r['borrower']}</td>
                    <td style="padding:10px;color:{late_color};font-weight:bold;">{r['days_late']} Days</td>
                    <td style="padding:10px;text-align:center;">
                        <span style="background:{late_color};color:white;padding:2px 8px;border-radius:10px;font-size:10px;">{r['status']}</span>
                    </td>
                </tr>"""
                
        st.markdown(f"""
            <div style="border:2px solid #FF4B4B;border-radius:10px;overflow:hidden;">
                <table style="width:100%;border-collapse:collapse;font-size:12px;">
                    <tr style="background:#FF4B4B;color:white;">
                        <th style="padding:10px;">Loan ID</th>
                        <th style="padding:10px;">borrower</th>
                        <th style="padding:10px;text-align:center;">Late By</th>
                        <th style="padding:10px;text-align:center;">status</th>
                    </tr>
                    {od_rows}
                </table>
            </div>""", unsafe_allow_html=True)                                                                                                                                                                                                                                   
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

# ==============================
# 20. PAYROLL MANAGEMENT PAGE (REBUILT)
# ==============================

def show_payroll():
    """
    Rebuilt Payroll Management: Handles Uganda tax compliance (PAYE/LST/NSSF)
    with enhanced stability and professional printable reporting.
    """
    # Early Initialization
    df = pd.DataFrame() 
    tenant = st.session_state.get("tenant_id")
    
    # SaaS Access Control
    if st.session_state.get("role") != "Admin":
        st.error("🔒 Restricted Access: Only Administrators can process payroll.")
        return

    if not tenant:
        st.error("Session expired. Please log in.")
        st.stop()

    st.markdown("<h2 style='color: #4A90E2; font-family: sans-serif;'>🧾 Payroll Management</h2>", unsafe_allow_html=True)

    # 1. DATA SYNC & CLEANING
    df_raw = get_cached_data("payroll")
    if df_raw is None: df_raw = pd.DataFrame()
    
    # Remove duplicate columns and reset index immediately
    df_raw = df_raw.loc[:, ~df_raw.columns.duplicated()]
    df_raw = df_raw.reset_index(drop=True)

    required_columns = [
        "payroll_ID", "Employee", "TIN", "Designation", "Mob_No", "Account_No", "NSSF_No",
        "Arrears", "Basic_Salary", "Absent_Deduction", "LST", "Gross_Salary", 
        "PAYE", "NSSF_5", "Advance_DRS", "Other_Deductions", "Net_Pay", 
        "NSSF_10", "NSSF_15", "Date", "tenant_id" 
    ]
    
    if df_raw.empty:
        df_all = pd.DataFrame(columns=required_columns)
    else:
        df_all = df_raw.copy()
        df_all.columns = df_all.columns.str.strip().str.replace(" ", "_")
        df_all = df_all.loc[:, ~df_all.columns.duplicated()]
        df_all = df_all.reset_index(drop=True)

        for col in required_columns:
            if col not in df_all.columns: df_all[col] = 0
        df_all = df_all.fillna(0)

    # Filter for Tenant
    df = df_all[df_all["tenant_id"].astype(str) == str(tenant)].copy() if "tenant_id" in df_all.columns else df_all.copy()
    df = df.reset_index(drop=True)

    def run_manual_sync_calculations(basic, arrears, absent_deduct, advance, other):
        gross = (float(basic) + float(arrears)) - float(absent_deduct)
        lst = 100000 / 12 if gross > 1000000 else 0
        n5, n10 = gross * 0.05, gross * 0.10
        
        # Uganda PAYE Logic
        paye = 0
        if gross > 410000: paye = 25000 + (0.30 * (gross - 410000))
        elif gross > 235000: paye = (gross - 235000) * 0.10
            
        net = gross - (paye + lst + n5 + float(advance) + float(other))
        return {"gross": round(gross), "lst": round(lst), "n5": round(n5), "n10": round(n10), "n15": round(n5+n10), "paye": round(paye), "net": round(net)}

    tab_process, tab_logs = st.tabs(["➕ Process Salary", "📜 Payroll History"])

    with tab_process:
        with st.form("new_payroll_form", clear_on_submit=True):
            st.markdown("<h4 style='color: #2B3F87; font-family: sans-serif;'>👤 Employee Details</h4>", unsafe_allow_html=True)
            name = st.text_input("Employee name")
            c1, c2, c3 = st.columns(3)
            f_tin = c1.text_input("TIN")
            f_desig = c2.text_input("Designation")
            f_mob = c3.text_input("Mob No.")
            
            c4, c5 = st.columns(2)
            f_acc = c4.text_input("Account No.")
            f_nssf_no = c5.text_input("NSSF No.")
            
            st.write("---")
            st.markdown("<h4 style='color: #2B3F87; font-family: sans-serif;'>💰 Earnings & Deductions</h4>", unsafe_allow_html=True)
            c6, c7, c8 = st.columns(3)
            f_arrears = c6.number_input("ARREARS", min_value=0.0)
            f_basic = c7.number_input("SALARY (Basic)", min_value=0.0)
            f_absent = c8.number_input("Absenteeism Deduction", min_value=0.0)
            
            c9, c10 = st.columns(2)
            f_adv = c9.number_input("S.DRS / ADVANCE", min_value=0.0)
            f_other = c10.number_input("Other Deductions", min_value=0.0)

            if st.form_submit_button("💳 Confirm & Release Payment", use_container_width=True):
                if name and f_basic > 0:
                    calc = run_manual_sync_calculations(f_basic, f_arrears, f_absent, f_adv, f_other)
                    next_id = int(pd.to_numeric(df_all["payroll_ID"], errors="coerce").max() + 1) if not df_all.empty else 1

                    new_row = pd.DataFrame([{
                        "payroll_ID": next_id, "Employee": name, "TIN": f_tin, "Designation": f_desig, 
                        "Mob_No": f_mob, "Account_No": f_acc, "NSSF_No": f_nssf_no, "Arrears": f_arrears,
                        "Basic_Salary": f_basic, "Absent_Deduction": f_absent, "Gross_Salary": calc['gross'], 
                        "LST": calc['lst'], "PAYE": calc['paye'], "NSSF_5": calc['n5'], "NSSF_10": calc['n10'], 
                        "NSSF_15": calc['n15'], "Advance_DRS": f_adv, "Other_Deductions": f_other, 
                        "Net_Pay": calc['net'], "Date": datetime.now().strftime("%Y-%m-%d"), "tenant_id": str(tenant)
                    }])
                    
                    final_save_df = pd.concat([df_all, new_row], ignore_index=True).fillna(0)
                    final_save_df.columns = [c.replace("_", " ") for c in final_save_df.columns]
                    
                    if save_data("payroll", final_save_df):
                        st.success(f"✅ Payroll for {name} saved!")
                        st.rerun()

    with tab_logs:
        if not df.empty:
            p_col1, p_col2 = st.columns([4, 1])
            p_col1.markdown(f"<h3 style='color: #4A90E2; font-family: sans-serif;'>{datetime.now().strftime('%B %Y')} Summary</h3>", unsafe_allow_html=True)
            
            def fm(x): 
                try: return f"{int(float(x)):,}" 
                except: return "0"

            # Enhanced HTML Template with proper Headings
            header_style = "background:#2B3F87; color:white; padding:10px; border:1px solid #ddd; font-size:11px;"
            cell_style = "border:1px solid #ddd; padding:8px; font-size:11px; font-family: sans-serif;"
            
            rows_html = f"""
            <thead>
                <tr style="{header_style}">
                    <th>S/N</th><th>Employee Details</th><th>TIN/NSSF/ACC</th><th>Arrears</th><th>Basic</th>
                    <th>Gross</th><th>PAYE</th><th>NSSF(5%)</th><th>Net Pay</th><th>NSSF(10%)</th>
                </tr>
            </thead>
            <tbody>"""
            
            for i, r in df.iterrows():
                rows_html += f"""
                <tr>
                    <td style='{cell_style} text-align:center;'>{i+1}</td>
                    <td style='{cell_style}'>
                        <b>{r['Employee']}</b><br><small>{r.get('Designation', '-')}</small>
                    </td>
                    <td style='{cell_style} font-size:9px;'>
                        TIN: {r.get('TIN','-')}<br>ACC: {r.get('Account_No','-')}<br>NSSF: {r.get('NSSF_No','-')}
                    </td>
                    <td style='{cell_style} text-align:right;'>{fm(r['Arrears'])}</td>
                    <td style='{cell_style} text-align:right;'>{fm(r['Basic_Salary'])}</td>
                    <td style='{cell_style} text-align:right; font-weight:bold;'>{fm(r['Gross_Salary'])}</td>
                    <td style='{cell_style} text-align:right;'>{fm(r['PAYE'])}</td>
                    <td style='{cell_style} text-align:right;'>{fm(r['NSSF_5'])}</td>
                    <td style='{cell_style} text-align:right; background:#E3F2FD; font-weight:bold;'>{fm(r['Net_Pay'])}</td>
                    <td style='{cell_style} text-align:right; background:#FFF9C4;'>{fm(r['NSSF_10'])}</td>
                </tr>"""

            rows_html += f"""
                <tr style="background:#2B3F87; color:white; font-weight:bold; font-family: sans-serif;">
                    <td colspan="3" style="text-align:center; padding:12px;">GRAND TOTALS</td>
                    <td style='text-align:right; padding:12px;'>{fm(df['Arrears'].sum())}</td>
                    <td style='text-align:right; padding:12px;'>{fm(df['Basic_Salary'].sum())}</td>
                    <td style='text-align:right; padding:12px;'>{fm(df['Gross_Salary'].sum())}</td>
                    <td style='text-align:right; padding:12px;'>{fm(df['PAYE'].sum())}</td>
                    <td style='text-align:right; padding:12px;'>{fm(df['NSSF_5'].sum())}</td>
                    <td style='text-align:right; padding:12px;'>{fm(df['Net_Pay'].sum())}</td>
                    <td style='text-align:right; padding:12px;'>{fm(df['NSSF_10'].sum())}</td>
                </tr>
            </tbody>"""

            printable_html = f"""
            <html><head><style>body{{font-family:sans-serif;}} table{{width:100%; border-collapse:collapse;}}</style></head>
            <body><table>{rows_html}</table></body></html>"""

            if p_col2.button("📥 Print PDF", key="print_payroll_trigger"):
                st.components.v1.html(printable_html + "<script>window.print();</script>", height=0)

            st.components.v1.html(printable_html, height=600, scrolling=True)
            st.download_button("📄 Download CSV", data=df.to_csv(index=False), file_name="payroll_export.csv")
            
            st.write("---")
            with st.expander("⚙️ Modify / Delete Record"):
                pay_opts = [f"{r['Employee']} (ID: {r['payroll_ID']})" for _, r in df.iterrows()]
                if pay_opts:
                    sel_opt = st.selectbox("Select Record to Manage", pay_opts, key="payroll_edit_selectbox")
                    try:
                        sid = str(sel_opt.split("(ID: ")[1].replace(")", ""))
                        # Final protection against reindexing crash during selection
                        item = df[df['payroll_ID'].astype(str) == sid].iloc[0]
                        st.info("Direct modification locked. Delete and re-process for errors.")
                        
                        if st.button("🗑️ Delete This Record", use_container_width=True):
                            # Filter using df_all to preserve other tenants' data
                            df_new = df_all[df_all['payroll_ID'].astype(str) != sid].copy()
                            df_new.columns = [c.replace("_", " ") for c in df_new.columns]
                            
                            if save_data("payroll", df_new):
                                st.warning("payroll record deleted.")
                                st.rerun()
                    except Exception as e:
                        st.error(f"Selection error: {e}")
        else:
            st.info("No payroll records found for this period.")
# ==========================================
# 🚀 BALLISTIC FINTECH REPORTS ENGINE (FINAL)
# ==========================================
import plotly.express as px
import pandas as pd
import streamlit as st
from datetime import datetime

def show_reports():
    """
    Advanced financial reporting with multi-tenant isolation 
    and Investor-grade intelligence metrics.
    """
    # ==============================
    # 🎨 HEADER (EXECUTIVE UI)
    # ==============================
    st.markdown("""
    <div style='background: linear-gradient(90deg,#1E3A8A,#2B3F87); padding:20px; border-radius:15px; margin-bottom:25px;'>
        <h2 style='margin:0; color:white; font-size:24px;'>📊 Financial Intelligence Dashboard</h2>
        <p style='margin:0; color:#DBEAFE; font-size:13px;'>Real-time P&L, balance Sheet, and Portfolio Yield Analysis</p>
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
    # ⚙️ CALCULATION HELPERS
    # ==============================
    def col_sum(df, col):
        if df.empty or col not in df.columns: return 0.0
        return pd.to_numeric(df[col], errors="coerce").fillna(0).sum()

    def to_numeric_series(df, col):
        if df.empty or col not in df.columns: return pd.Series(dtype=float)
        return pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ==============================
    # 🔢 CORE FINANCIAL ACCOUNTING
    # ==============================
    # Capital and Revenue
    total_capital_out = col_sum(loans, "principal")
    projected_interest = col_sum(loans, "interest")
    actual_collected = col_sum(payments, "amount")
    
    # Operational Costs (OpEx)
    direct_expenses = col_sum(expenses, "amount")
    nssf_tax = col_sum(payroll, "nssf_5") + col_sum(payroll, "nssf_10")
    paye_tax = col_sum(payroll, "paye")
    salary_net = col_sum(payroll, "net_pay")
    petty_out = col_sum(petty[petty["type"] == "Out"], "amount") if not petty.empty else 0

    total_opex = direct_expenses + petty_out + nssf_tax + paye_tax + salary_net
    
    # Bottom Line
    # Profit here is calculated as (Collected - Operating Costs)
    cash_profit = actual_collected - total_opex

    # ==============================
    # 💎 KPI TILES
    # ==============================
    def render_kpi(title, value, color, icon="💰"):
        st.markdown(f"""
            <div style="padding:16px; border-radius:12px; background:white; border:1px solid #E5E7EB; box-shadow:0 2px 4px rgba(0,0,0,0.02);">
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
    st.markdown("<br>### 📈 Monthly Profit & Loss Trend", unsafe_allow_html=True)
    
    # Prepare Time Series
    payments["date"] = pd.to_datetime(payments.get("date"), errors="coerce")
    expenses["date"] = pd.to_datetime(expenses.get("date"), errors="coerce")
    
    # Aggregate Income/Expense by Month
    inc_m = payments.set_index("date").resample("M")["amount"].sum() if not payments.empty else pd.Series()
    exp_m = expenses.set_index("date").resample("M")["amount"].sum() if not expenses.empty else pd.Series()
    
    pl_combined = pd.concat([inc_m, exp_m], axis=1).fillna(0)
    pl_combined.columns = ["Income", "Expenses"]
    pl_combined["Net"] = pl_combined["Income"] - pl_combined["Expenses"]
    pl_combined.index = pl_combined.index.strftime('%b %Y')
    
    fig_trend = px.area(pl_combined, barmode='group', color_discrete_map={"Income": "#059669", "Expenses": "#EF4444", "Net": "#1E3A8A"})
    fig_trend.update_layout(hovermode="x unified", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_trend, use_container_width=True)

    # ==============================
    # 🧠 INVESTOR INTELLIGENCE (PAR & YIELD)
    # ==============================
    st.markdown("### 🧠 Investor Intelligence Portfolio Metrics")
    
    # 1. PAR (Portfolio At Risk)
    overdue_loans = loans[loans["status"].astype(str).str.upper().str.contains("OVERDUE", na=False)]
    par_value = col_sum(overdue_loans, "balance")
    par_ratio = (par_value / total_capital_out * 100) if total_capital_out > 0 else 0
    
    # 2. Portfolio Yield
    yield_pct = (projected_interest / total_capital_out * 100) if total_capital_out > 0 else 0
    
    # 3. Collection Efficiency
    coll_eff = (actual_collected / (total_capital_out + projected_interest) * 100) if (total_capital_out + projected_interest) > 0 else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Portfolio Yield", f"{yield_pct:.1f}%")
    m2.metric("Collection Eff.", f"{coll_eff:.1f}%")
    m3.metric("PAR Ratio", f"{par_ratio:.1f}%", delta=f"{par_ratio:.1f}%", delta_color="inverse")
    m4.metric("OpEx Ratio", f"{(total_opex/actual_collected*100 if actual_collected > 0 else 0):.1f}%")

    # ==============================
    # 🧾 STATEMENTS
    # ==============================
    st.markdown("<br>", unsafe_allow_html=True)
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
        st.markdown("#### 🧾 balance Sheet Position")
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
            "Metric": ["Capital Out", "interest Revenue", "Total OpEx", "Cash Profit", "Portfolio Yield %", "PAR %"],
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

        Active_company = tenant_resp.data[0]

    except Exception as e:
        st.error(f"❌ Connection Error: {e}")
        return

    # ==============================
    # BRANDING FALLBACK SAFETY
    # ==============================
    # Priority: Session State -> Database -> Default Navy
    brand_color = st.session_state.get(
        "theme_color", 
        Active_company.get("brand_color", "#2B3F87")
    )

    st.markdown(
        f"<h2 style='color: {brand_color};'>⚙️ Portal Settings & Branding</h2>",
        unsafe_allow_html=True
    )

    # --- BUSINESS IDENTITY SECTION ---
    st.subheader("🏢 Business Identity")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown(f"**Current Business name:** {Active_company.get('name', 'Unknown')}")

        new_color = st.color_picker(
            "🎨 Change Brand Color",
            Active_company.get('brand_color', '#2B3F87'),
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
        
        logo_url = Active_company.get("logo_url")

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
                file_path = f"logos/{Active_company.get('id')}_logo.png"

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
            supabase.table("tenants").update(updated_data).eq("id", Active_company.get("id")).execute()
            
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
