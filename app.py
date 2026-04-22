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
import streamlit as st
import pandas as pd
from datetime import datetime
import uuid
# ==============================
# 🔒 SAFETY: Ensure supabase always exists
# ==============================
if "supabase" not in globals():
    supabase = None

# 1. CORE DATA ENGINE (Must be at the top level)
@st.cache_data(ttl=600)
def get_cached_data_legacy(table_name):  # 🔥 renamed (NOT deleted)
    """Fetches and caches data from Supabase for all pages."""
    try:
        if supabase is None:
            return pd.DataFrame()

        # Use your existing supabase client connection here
        response = supabase.table(table_name).select("*").execute()
        return pd.DataFrame(response.data)
    except Exception as e:
        # This provides the error message you saw in your screenshots
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Move this to the absolute top to prevent "Set Page Config" errors
st.set_page_config(
    page_title="Lending Manager Pro",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

SESSION_TIMEOUT = 30

# ==============================
# 1. THEME ENGINE (ENTERPRISE SAFE)
# ==============================
# ==============================
# 1. THEME ENGINE (ENTERPRISE SAFE)
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

    /* 🔥 FIX NAV TEXT VISIBILITY */
    div[role="radiogroup"] label {{
        color: rgba(255,255,255,0.95) !important;
        font-weight: 500 !important;
    }}

    /* 🔥 FIX ICON + TEXT ROW */
    div[role="radiogroup"] label span {{
        color: rgba(255,255,255,0.95) !important;
    }}

    /* 🔥 INACTIVE ITEMS (slightly dim but visible) */
    div[role="radiogroup"] label {{
        opacity: 0.85;
        padding: 10px !important;
        border-radius: 10px;
        transition: 0.2s ease;
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
# 🔌 SUPABASE INIT (SAFE GLOBAL)
# ==============================

@st.cache_resource
def init_supabase():
    try:
        url = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
        key = st.secrets.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")

        if not url or not key:
            return None

        return create_client(url, key)

    except Exception:
        return None


supabase = init_supabase()

# ⚠️ DO NOT STOP APP GLOBALLY
if supabase is None:
    st.warning("⚠️ Supabase not connected (some features may not work)")

@st.cache_data(ttl=60, show_spinner=False)
def get_cached_data(table_name):
    if not table_name:
        return []

    try:
        response = supabase.table(table_name).select("*").execute()

        if hasattr(response, "data") and response.data:
            return response.data

        return []

    except Exception as e:
        print(f"[DATA ERROR] {table_name}: {e}")  # avoids UI spam
        return []


# ==============================
# 3. MULTI-TENANT SESSION CORE
# ==============================
if "tenant_id" not in st.session_state:
    st.session_state.tenant_id = None
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "theme_color" not in st.session_state:
    st.session_state.theme_color = "#2B3F87"


def get_tenant_id():
    return st.session_state.get("tenant_id")


def require_tenant():
    if not st.session_state.get("tenant_id"):
        st.error("Session expired or unauthorized access. Please log in again.")
        st.stop()


# ==============================
# 4. STORAGE HELPERS (FIXED + SAFE)
# ==============================

def generate_invite_token():
    import secrets
    return secrets.token_urlsafe(32)
def upload_image(file, bucket="collateral-photos"):
    try:
        if supabase is None:
            st.error("Storage unavailable")
            return None

        require_tenant()
        tenant_id = get_tenant_id()

        clean_name = re.sub(r'[^a-zA-Z0-9._-]', '_', file.name)
        file_name = f"{tenant_id}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{clean_name}"

        file_content = file.getvalue()
        content_type = file.type

        supabase.storage.from_(bucket).upload(
            path=file_name,
            file=file_content,
            file_options={"content-type": content_type}
        )

        response = supabase.storage.from_(bucket).get_public_url(file_name)
        return response

    except Exception as e:
        st.error(f"Upload failed: {e}")
        return None

import streamlit as st
import uuid
import time
import random

def run_auth_ui(supabase):
    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    # Logged Out Views
    current_view = st.session_state["view"]
    if current_view == "login":
        login_page(supabase)
    elif current_view == "signup":
        staff_signup_page(supabase)
    elif current_view == "create_company":
        admin_company_registration(supabase)
# ==============================
# 5. DATA LAYER (MERGED - NO DUPLICATES)
# ==============================
def safe_series(df, col, default=0):
    if df is None or df.empty or col not in df.columns:
        return pd.Series([default]*len(df) if df is not None else [], dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default)

@st.cache_data(ttl=600)
def get_cached_data(table_name):  # ✅ MAIN FUNCTION
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


def save_data(table_name, dataframe):
    try:
        if supabase is None:
            st.error("Database not connected")
            return False

        require_tenant()

        if dataframe is None or dataframe.empty:
            return False

        dataframe["tenant_id"] = get_tenant_id()
        records = dataframe.replace({np.nan: None}).to_dict("records")
        
        supabase.table(table_name).upsert(records).execute()
        
        st.cache_data.clear()
        return True

    except Exception as e:
        st.error(f"Database Save Error [{table_name}]: {e}")
        return False


# ==============================
# 🔌 SUPABASE INIT (ROBUST - SAFE MERGED)
# ==============================

SUPABASE_URL = (
    st.secrets.get("supabase_url") or
    st.secrets.get("SUPABASE_URL") or
    os.getenv("SUPABASE_URL")
)

SUPABASE_KEY = (
    st.secrets.get("supabase_key") or
    st.secrets.get("SUPABASE_KEY") or
    os.getenv("SUPABASE_KEY")
)

if not SUPABASE_URL or not SUPABASE_KEY:
    st.warning("⚠️ Supabase credentials not configured")

    try:
        st.write("DEBUG → Available secrets:", list(st.secrets.keys()))
    except:
        pass

    SUPABASE_DISABLED = True
else:
    SUPABASE_DISABLED = False

try:
    if not SUPABASE_DISABLED:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    st.warning(f"⚠️ Supabase initialization failed: {e}")
    supabase = None

# ==============================
# 6. AUTH CORE (UNIFIED - FINAL)
# ==============================
def authenticate(supabase, company_code, email, password):
    try:
        # Step 1: Auth
        res = supabase.auth.sign_in_with_password({
            "email": email,
            "password": password
        })

        if not res.user:
            return {"success": False, "error": "Invalid email or password"}

        # Step 2: Fetch Profile
        profile = supabase.table("users")\
            .select("tenant_id, role, tenants(company_code, name)")\
            .eq("id", res.user.id)\
            .execute()

        if not profile.data:
            return {"success": False, "error": "User profile not found"}

        record = profile.data[0]
        tenant_info = record.get("tenants")

        if not tenant_info:
            return {"success": False, "error": "No business entity linked"}

        # Step 3: Company validation
        if tenant_info["company_code"].strip().upper() != company_code.strip().upper():
            return {"success": False, "error": "Incorrect Company Code"}

        return {
            "success": True,
            "user_id": res.user.id,
            "tenant_id": record["tenant_id"],
            "role": record.get("role", "Staff"),
            "company": tenant_info.get("name")
        }
        if st.session_state.get("role") == "Admin":
            if st.button("👑 Manage Invites"):
                st.session_state["view"] = "admin_invites"
                st.rerun()

    except Exception as e:
        return {"success": False, "error": str(e)}

# ==============================
# 7. SESSION CREATION (UNIFIED)
# ==============================
def create_session(user_data, remember_me=False):
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

    st.success("Login successful")
    st.rerun()

# ==============================
# 8. SESSION SECURITY
# ==============================
SESSION_TIMEOUT = 30

def check_session_timeout():
    if not st.session_state.get("logged_in"):
        return

    last = st.session_state.get("last_activity", datetime.now())
    if (datetime.now() - last) > timedelta(minutes=SESSION_TIMEOUT):
        st.session_state.clear()
        st.warning("Session timed out. Please log in again.")
        st.stop()

    st.session_state["last_activity"] = datetime.now()

# ==============================
# 9. RATE LIMITING
# ==============================
MAX_ATTEMPTS = 5
LOCKOUT_MINUTES = 10

def check_rate_limit(email):
    attempts = st.session_state.get("login_attempts", {})
    if email in attempts:
        count, last = attempts[email]
        if count >= MAX_ATTEMPTS and (datetime.now() - last) < timedelta(minutes=LOCKOUT_MINUTES):
            return False
    return True

def record_failed_attempt(email):
    attempts = st.session_state.setdefault("login_attempts", {})
    count, _ = attempts.get(email, (0, datetime.now()))
    attempts[email] = (count + 1, datetime.now())

import streamlit as st
import time

# ==============================
# 10. TENANT FILTER
# ==============================
def tenant_filter(df):
    if df is None or df.empty:
        return df
    if "tenant_id" not in df.columns:
        return df
    
    current_tenant = st.session_state.get("tenant_id")
    return df[df["tenant_id"] == current_tenant].copy()


# ==============================
# 🏢 ADMIN COMPANY REGISTRATION
# ==============================
def admin_company_registration(supabase):
    st.header("🏢 Register Your Company")
    
    company_name = st.text_input("Organization Name")
    admin_name = st.text_input("Admin Full Name")
    email = st.text_input("Business Email").strip().lower()
    password = st.text_input("Password", type="password")

    if st.button("Create Organization", use_container_width=True):
        try:
            # Generate IDs and 14-digit Code
            tenant_id = str(uuid.uuid4())
            company_code = str(random.randint(10**13, (10**14)-1))

            # 1. Create Auth Account
            res = supabase.auth.sign_up({
                "email": email,
                "password": password
            })

            # ✅ FIX: Validate response
            if not res or not res.user:
                raise Exception(f"Signup failed: {res}")

            user_id = res.user.id

            # 2. Insert Tenant Row
            tenant_res = supabase.table("tenants").insert({
                "id": tenant_id,
                "name": company_name,
                "company_code": company_code
            }).execute()

            if not tenant_res.data:
                raise Exception("Failed to create tenant.")

            # 3. Create Admin Profile
            profile_payload = {
                "id": user_id,
                "name": admin_name,
                "email": email,
                "tenant_id": tenant_id,
                "role": "Admin"
            }

            # ✅ Keep slight delay (Supabase consistency)
            time.sleep(1)

            profile_res = supabase.table("users").insert(profile_payload).execute()

            if not profile_res.data:
                raise Exception("Failed to create user profile.")

            st.success(f"✅ Registered! YOUR LOGIN CODE: {company_code}")
            st.info("Write this code down; your staff needs it to join.")

            if st.button("Go to Login"):
                st.session_state["view"] = "login"
                st.rerun()

        except Exception as e:
            st.error(f"Registration Error: {e}")


# ==============================
# 👥 STAFF SIGNUP
# ==============================
def view_staff_signup(supabase):
    st.header("🆕 Join an Organization")
    
    with st.form("staff_form"):
        code = st.text_input("Company Code", help="Get this from your manager").strip()
        name = st.text_input("Your Full Name")
        email = st.text_input("Email").strip().lower()
        pwd = st.text_input("Create Password", type="password")
        
        submit = st.form_submit_button("Join Company", use_container_width=True)

    if submit:
        try:
            # 1. Verify Company
            tenant = supabase.table("tenants").select("id").eq("company_code", code).execute()

            if not tenant.data:
                st.error("Invalid Company Code.")
                return
            
            t_id = tenant.data[0]['id']

            # 2. Create Auth
            res = supabase.auth.sign_up({
                "email": email,
                "password": pwd
            })

            # ✅ FIX: Validate signup
            if not res or not res.user:
                raise Exception(f"Signup failed: {res}")

            user_id = res.user.id

            # 3. Create Staff Profile
            time.sleep(1)

            profile_res = supabase.table("users").insert({
                "id": user_id,
                "name": name,
                "email": email,
                "tenant_id": t_id,
                "role": "Staff"
            }).execute()

            if not profile_res.data:
                raise Exception("Failed to create user profile.")

            st.success("Account created! You can now log in.")
            st.session_state["view"] = "login"
            st.rerun()

        except Exception as e:
            st.error(f"Signup failed: {e}")


# ==============================
# 🔑 LOGIN PAGE
# ==============================
def login_page(supabase):
    st.markdown("## 🔐 Finance Portal Login")
    
    c_code = st.text_input("14-Digit Company Code", placeholder="e.g., 10293847561023").strip()
    email = st.text_input("Email").strip().lower()
    pwd = st.text_input("Password", type="password")

    if st.button("Access Dashboard", use_container_width=True, type="primary"):
        try:
            # 1. Authenticate
            auth_res = supabase.auth.sign_in_with_password({
                "email": email,
                "password": pwd
            })

            if not auth_res or not auth_res.user:
                raise Exception("Authentication failed")

            user_id = auth_res.user.id
            user_email = auth_res.user.email

            # 2. Fetch user + tenant (SAFE VERSION)
            user_data = supabase.table("users") \
                .select("*, tenants(company_code)") \
                .eq("id", user_id) \
                .execute()

            # 🔥 AUTO-HEAL if ID mismatch
            if not user_data.data:
                user_data = supabase.table("users") \
                    .select("*, tenants(company_code)") \
                    .eq("email", user_email) \
                    .execute()

                if user_data.data:
                    supabase.table("users") \
                        .update({"id": user_id}) \
                        .eq("email", user_email) \
                        .execute()

            if not user_data.data:
                raise Exception("User profile not found")

            user_data = user_data.data[0]

            db_code = str(user_data.get('tenants', {}).get('company_code'))

            if db_code == c_code:
                st.session_state.update({
                    "authenticated": True,
                    "logged_in": True,
                    "user_id": user_id,
                    "tenant_id": user_data['tenant_id'],
                    "user_name": user_data['name'],
                    "role": user_data['role']
                })
                st.rerun()
            else:
                st.error("Invalid Company Code for this account.")
                supabase.auth.sign_out()

        except Exception as e:
            st.error(f"Login failed: {e}")

    st.divider()

    col1, col2 = st.columns(2)
    if col1.button("🏢 Register Company"):
        st.session_state["view"] = "create_company"
        st.rerun()

    if col2.button("👥 Join as Staff"):
        st.session_state["view"] = "signup"
        st.rerun()


# ==============================
# 🔒 ROUTER
# ==============================
def run_auth_ui(supabase):
    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    if st.session_state.get("authenticated"):
        st.success(f"Welcome back, {st.session_state.get('user_name', 'User')}! 🚀")

        if st.button("Log Out", use_container_width=True):
            st.session_state.clear()
            st.rerun()
        return

    current_view = st.session_state["view"]

    if current_view == "login":
        login_page(supabase)
    elif current_view == "signup":
        view_staff_signup(supabase)  # ✅ FIXED NAME
    elif current_view == "create_company":
        admin_company_registration(supabase)
def render_sidebar():
    # ==============================
    # 1. FETCH TENANTS (UNCHANGED)
    # ==============================
    try:
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
    # 2. SIDEBAR UI
    # ==============================
    with st.sidebar:
        # Header Padding
        st.markdown('<div style="padding-top:10px;"></div>', unsafe_allow_html=True)

        if tenant_map:
            options = list(tenant_map.keys())
            default_index = 0

            if current_tenant_id:
                for i, name in enumerate(options):
                    if str(tenant_map[name]['id']) == str(current_tenant_id):
                        default_index = i
                        break

            Active_company_name = st.selectbox(
                "🏢 Business",
                options,
                index=default_index,
                key="sidebar_portal_select"
            )

            Active_company = tenant_map.get(Active_company_name, None)

            # ==============================
            # 🔁 TENANT SYNC (INSIDE SIDEBAR)
            # ==============================
            if Active_company:
                if str(st.session_state.get('tenant_id')) != str(Active_company['id']):
                    st.session_state['tenant_id'] = Active_company['id']
                    st.session_state['theme_color'] = Active_company.get('brand_color', '#2B3F87')
                    st.session_state['company'] = Active_company.get('name')

                    st.cache_data.clear()
                    try:
                        st.rerun()
                    except:
                        pass
        else:
            st.sidebar.warning("No business entities found.")
            st.stop()

                        # ==============================
        # 💎 SIDEBAR BRANDING (SAFE + GLOW)
        # ==============================
        import time

        logo_val = Active_company.get('logo_url') if Active_company else None
        final_logo_url = None

        if logo_val and str(logo_val).lower() not in ["0", "none", "null", ""]:
            if str(logo_val).startswith("http"):
                final_logo_url = logo_val
            else:
                try:
                    project_url = st.secrets.get("supabase_url") or st.secrets.get("SUPABASE_URL")
                    project_url = project_url.strip("/")
                    final_logo_url = f"{project_url}/storage/v1/object/public/company-logos/{logo_val}"
                except Exception:
                    final_logo_url = None

        # ✅ CENTER EVERYTHING SAFELY
        col1, col2, col3 = st.columns([1, 2, 1])

        with col2:
            if final_logo_url:
                st.image(final_logo_url, width=70)
            else:
                st.markdown("### 🏢")
                # ✅ SAFE LOGO BLOCK
        if final_logo_url:
            logo_component = f"""
            <div style="
                display:flex;
                justify-content:center;
                align-items:center;
                margin-top:10px;
            ">
                <div style="
                    padding:10px;
                    border-radius:50%;
                    background: radial-gradient(circle, rgba(255,255,255,0.25) 0%, rgba(255,255,255,0.05) 70%);
                    box-shadow: 0 0 20px rgba(255,255,255,0.12);
                ">
                    <img src="{final_logo_url}?t={int(time.time())}"
                         width="70"
                         style="border-radius:50%; object-fit:cover;" />
                </div>
            </div>
            """.strip()
        else:
            logo_component = """
            <div style="text-align:center; margin-top:10px;">
                <h1 style="font-size:38px; margin:0;">🏢</h1>
            </div>
            """

        # ✅ COMPANY NAME (WITH GLOW)
        st.markdown(
            f"""
            <div style='
                text-align:center;
                font-weight:600;
                font-size:15px;
                margin-top:5px;
                color:#f1f5f9;
                text-shadow:
                    0 0 4px rgba(255,255,255,0.4),
                    0 0 8px rgba(255,255,255,0.2);
            '>
                {Active_company_name}
                <span style="
                    color:#22c55e;
                    margin-left:4px;
                    text-shadow: 0 0 6px rgba(34,197,94,0.6);
                ">✔</span>
            </div>
            """,
            unsafe_allow_html=True
        )

        # ✅ SUBTEXT (VISIBLE ON DARK BACKGROUND)
        st.markdown(
            "<div style='text-align:center; font-size:11px; color:rgba(255,255,255,0.7); letter-spacing:1px;'>FINANCE CORE</div>",
            unsafe_allow_html=True
        )

        # ✅ DIVIDER
        st.divider()

        # ==============================
        # 📍 MENU
        # ==============================
        menu = {
            "Overview": "📈", "Loans": "💵", "Borrowers": "👥", "Collateral": "🛡️",
            "Calendar": "📅", "Ledger": "📄", "Payroll": "💳", "Expenses": "📉",
            "Petty Cash": "🪙", "Overdue Tracker": "🚨", "Payments": "💰", "Reports": "📊", "Settings": "⚙️"
        }

        menu_options = [f"{emoji} {name}" for name, emoji in menu.items()]
        current_p = st.session_state.get('current_page', "Overview")

        try:
            default_ix = list(menu.keys()).index(current_p)
        except:
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

        st.markdown("<br>", unsafe_allow_html=True)

        # ==============================
        # 🔐 LOGOUT
        # ==============================
        if st.session_state.get("authenticated"):
            st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)

            if st.button("🚪 Logout", use_container_width=True):
                st.session_state["logged_in"] = False
                st.session_state["authenticated"] = False

                for key in list(st.session_state.keys()):
                    if key not in ["theme_color", "logged_in", "authenticated"]:
                        del st.session_state[key]

                st.success("Logging out...")
                time.sleep(0.5)
                st.rerun()

    return selected_page

# ==============================
# 🚀 BORROWERS ENGINE (PRODUCTION)
# ==============================
import streamlit as st
import pandas as pd
import numpy as np
import uuid
from datetime import datetime

def show_borrowers():

    # ==============================
    # 🎨 BRAND
    # ==============================
    brand_color = st.session_state.get("theme_color", "#1E3A8A")
    st.markdown(f"<h2 style='color:{brand_color};'>🚀 Borrowers</h2>", unsafe_allow_html=True)

    # ==============================
    # 🔐 TENANT
    # ==============================
    tenant_id = st.session_state.get("tenant_id")
    if not tenant_id:
        st.error("Session expired")
        st.stop()

    # ==============================
    # 🧠 SAFE HELPERS (CORRECTLY NESTED)
    # ==============================
    def safe_df(df):
        """Ensures we always have a DataFrame object to avoid 'NoneType' errors."""
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    def safe_numeric(df, col, default=0.0):
        """Standardizes columns to numeric Series, handling missing data safely."""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.Series(dtype="float64")
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
        else:
            s = pd.Series([default] * len(df), index=df.index)
        return s.fillna(default)

    def force_series(x, length=0, default=0):
        """Guarantees a pandas Series output regardless of input type."""
        if isinstance(x, pd.Series):
            return x
        return pd.Series([default] * length)

    # ==============================
    # 📥 LOAD DATA
    # ==============================
    borrowers_df = safe_df(get_cached_data("borrowers"))
    loans_df = safe_df(get_cached_data("loans"))

    # Normalize columns
    for df in [borrowers_df, loans_df]:
        if not df.empty:
            df.columns = df.columns.str.strip().str.lower()

    # Tenant filter
    if "tenant_id" in borrowers_df.columns:
        borrowers_df = borrowers_df[borrowers_df["tenant_id"].astype(str) == str(tenant_id)]

    if "tenant_id" in loans_df.columns:
        loans_df = loans_df[loans_df["tenant_id"].astype(str) == str(tenant_id)]

    # Ensure borrower structure
    for col in ["id", "name", "phone", "email", "status"]:
        if col not in borrowers_df.columns:
            borrowers_df[col] = ""

    # ==============================
    # 🔥 LOAN ENGINE (LINKED)
    # ==============================
    risk_map = {}
    if not loans_df.empty:
        loans_df["balance"] = safe_numeric(loans_df, "balance")
        loans_df["due_date"] = pd.to_datetime(loans_df.get("due_date"), errors="coerce")

        today = pd.Timestamp.today()
        loans_df["days_overdue"] = (today - loans_df["due_date"]).dt.days
        loans_df["days_overdue"] = loans_df["days_overdue"].apply(lambda x: x if x > 0 else 0)
        loans_df["is_overdue"] = (loans_df["days_overdue"] > 0) & (loans_df["balance"] > 0)

        # Aggregate by borrower_id
        risk_df = loans_df.groupby("borrower_id").agg({
            "balance": "sum",
            "is_overdue": "sum",
            "days_overdue": "max"
        }).reset_index()

        risk_df.rename(columns={
            "balance": "exposure",
            "is_overdue": "overdue_loans",
            "days_overdue": "max_days"
        }, inplace=True)

        def classify(row):
            if row["overdue_loans"] == 0: return "🟢 Healthy"
            elif row["max_days"] <= 7: return "🟡 Watch"
            elif row["max_days"] <= 30: return "🟠 Risk"
            else: return "🔴 Critical"

        risk_df["risk"] = risk_df.apply(classify, axis=1)
        risk_map = risk_df.set_index("borrower_id").to_dict("index")

    # ==============================
    # 📑 UI TABS
    # ==============================
    tab_view, tab_add = st.tabs(["📋 View Borrowers", "➕ Add Borrower"])

    with tab_add:
        with st.form("add_borrower_form", clear_on_submit=True):
            st.markdown(f"<h4 style='color: {brand_color};'>📝 Register New Borrower</h4>", unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            name = c1.text_input("Full Name*")
            phone = c2.text_input("Phone Number*")
            email = c1.text_input("Email Address")
            nid = c2.text_input("National ID / NIN")
            addr = c1.text_input("Physical Address")
            nok = c2.text_input("Next of Kin (Name & Contact)")
            
            if st.form_submit_button("🚀 Save Borrower Profile", use_container_width=True):
                if name and phone:
                    new_id = str(uuid.uuid4())
                    t_id = st.session_state.get('tenant_id', 'test-tenant-123')
                    new_entry = pd.DataFrame([{
                        "id": new_id, "name": name, "phone": phone, "email": email,
                        "national_id": nid, "address": addr, "next_of_kin": nok,
                        "status": "Active", "tenant_id": t_id 
                    }])
                    if save_data("borrowers", new_entry):
                        st.success(f"✅ {name} registered!")
                        st.rerun()
                else:
                    st.error("⚠️ Please fill in Name and Phone Number.")

    with tab_view:
        # ==============================
        # 🔍 SEARCH
        # ==============================
        search = st.text_input("🔍 Search name / phone").lower()

        # ==============================
        # 📊 TABLE VIEW
        # ==============================
        if not borrowers_df.empty:
            df_to_show = borrowers_df.copy()
            df_to_show["name"] = df_to_show["name"].astype(str)
            df_to_show["phone"] = df_to_show["phone"].astype(str)
            # Ensure new columns exist for string conversion
            for col in ["national_id", "next_of_kin"]:
                if col in df_to_show.columns:
                    df_to_show[col] = df_to_show[col].astype(str)

            mask = (
                df_to_show["name"].str.lower().str.contains(search, na=False) |
                df_to_show["phone"].str.contains(search, na=False)
            )
            filtered_df = df_to_show[mask]

            if not filtered_df.empty:
                rows_html = ""
                for i, r in filtered_df.reset_index().iterrows():
                    bg_color = "#F0F8FF" if i % 2 == 0 else "#FFFFFF"
                    b_id = str(r.get("id", ""))
                    
                    # Risk Logic
                    risk = risk_map.get(b_id, {})
                    risk_label = risk.get("risk", "🟢 Healthy")
                    
                    if "🔴" in risk_label: color = "#dc2626"
                    elif "🟠" in risk_label: color = "#ea580c"
                    elif "🟡" in risk_label: color = "#f59e0b"
                    else: color = "#16a34a"

                    rows_html += f"""
                    <tr style="background-color: {bg_color}; border-bottom: 1px solid #ddd;">
                        <td style="padding:12px;"><b>{r.get('name', 'Unknown')}</b></td>
                        <td style="padding:12px;">{r.get('phone', 'N/A')}</td>
                        <td style="padding:12px; font-size:11px; color:#666;">{r.get('national_id', 'N/A')}</td>
                        <td style="padding:12px; font-size:11px;">{r.get('next_of_kin', 'N/A')}</td>
                        <td style="padding:12px;">
                            <span style="background:{color}; color:white; padding:3px 8px; border-radius:12px; font-size:11px;">
                                {risk_label}
                            </span>
                        </td>
                        <td style="padding:12px; text-align:center;">
                            <span style="background:{brand_color}; color:white; padding:3px 8px; border-radius:12px; font-size:10px;">
                                {r.get('status', 'Active')}
                            </span>
                        </td>
                    </tr>"""

                st.markdown(f"""
                <div style='border:2px solid {brand_color}33; border-radius:10px; overflow:hidden; margin-top:20px;'>
                    <table style='width:100%; border-collapse:collapse; font-family:sans-serif; font-size:13px;'>
                        <thead>
                            <tr style='background:{brand_color}; color:white; text-align:left;'>
                                <th style='padding:12px;'>Borrower Name</th>
                                <th style='padding:12px;'>Phone</th>
                                <th style='padding:12px;'>National ID</th>
                                <th style='padding:12px;'>Next of Kin</th>
                                <th style='padding:12px;'>Risk Status</th>
                                <th style='padding:12px; text-align:center;'>Status</th>
                            </tr>
                        </thead>
                        <tbody>{rows_html}</tbody>
                    </table>
                </div>""", unsafe_allow_html=True)

                # ==============================
                # 🖱️ SELECTION INTERACTION
                # ==============================
                st.write("")
                selected_name = st.selectbox(
                    "🎯 Select a borrower to manage profile:", 
                    options=["-- Select --"] + filtered_df["name"].tolist()
                )
                if selected_name != "-- Select --":
                    sel_id = filtered_df[filtered_df["name"] == selected_name]["id"].values[0]
                    st.session_state["selected_borrower"] = sel_id
                
            else:
                st.info("No borrowers found matching your search.")
        else:
            st.info("No borrowers registered yet.")

    # ==============================
    # 👤 BORROWER PROFILE PANEL
    # ==============================
    selected_id = st.session_state.get("selected_borrower")

    if selected_id:
        st.write("---")
        st.markdown("## 👤 Borrower Profile")

        borrower_query = borrowers_df[borrowers_df["id"].astype(str) == str(selected_id)]

        if borrower_query.empty:
            st.warning("Borrower not found")
        else:
            borrower = borrower_query.iloc[0] if not df.empty else None if not df.empty else None

            with st.container(border=True):
                c1, c2 = st.columns(2)
                name = c1.text_input("Name", borrower["name"])
                phone = c2.text_input("Phone", borrower["phone"])
                email = c1.text_input("Email", borrower["email"])
                
                # New inputs for ID and Next of Kin
                c3, c4 = st.columns(2)
                nid = c3.text_input("National ID", borrower.get("national_id", ""))
                nok = c4.text_input("Next of Kin", borrower.get("next_of_kin", ""))

                # ==============================
                # 📊 LOANS LINKED (CLEAN VIEW)
                # ==============================
                user_loans = loans_df[loans_df["borrower_id"].astype(str) == str(selected_id)].copy()

                st.markdown("### 📊 Loan History")

                if not user_loans.empty:
                    st.dataframe(
                        user_loans, 
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "id": None, "tenant_id": None, "borrower_id": None, 
                            "created_at": None, "type": None, "borrower_name": None,
                            "status_new": None, "due_date": None, "days_overdue": None, "is_overdue": None,
                            "national_id": None, "next_of_kin": None, # Hide in loan history

                            "principal": st.column_config.NumberColumn("Principal", format="%,d"),
                            "interest": st.column_config.NumberColumn("Interest", format="%,d"),
                            "total_repayable": st.column_config.NumberColumn("Total Due", format="%,d"),
                            "amount_paid": st.column_config.NumberColumn("Paid", format="%,d"),
                            "balance": st.column_config.NumberColumn("Balance", format="%,d"),
                            
                            "start_date": st.column_config.DateColumn("Started"),
                            "end_date": st.column_config.DateColumn("Due Date"),
                        }
                    )
                    
                    # Export Button 
                    csv = user_loans.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Export Loan Statement (CSV)",
                        data=csv,
                        file_name=f"Statement_{name.replace(' ', '_')}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("No loans found for this borrower")
                # ==============================
                # 🛠️ ACTIONS
                # ==============================
                st.write("---")
                act_c1, act_c2 = st.columns(2)

                if act_c1.button("💾 Update Borrower", use_container_width=True):
                    borrowers_df.loc[borrowers_df["id"].astype(str) == str(selected_id), ["name","phone","email"]] = [name, phone, email]
                    if save_data("borrowers", borrowers_df):
                        st.success("Updated")
                        st.rerun()

                if act_c2.button("🗑️ Delete Borrower", use_container_width=True):
                    updated = borrowers_df[borrowers_df["id"].astype(str) != str(selected_id)]
                    if save_data("borrowers", updated):
                        st.warning("Deleted")
                        st.session_state.pop("selected_borrower", None)
                        st.rerun()

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta

# Note: Ensure 'supabase' client is initialized in your main app file
# from supabase import create_client
# supabase = create_client(url, key)

# ==============================
# 🔐 TENANT & STATE
# ==============================
def get_current_tenant():
    return st.session_state.get("tenant_id", "default")

def show_loans():
    st.title("📊 Loan Portfolio Manager")

    tenant_id = get_current_tenant()

    # Mocking session state for example; ensure this is loaded from Supabase in production
    def load_loans():
        res = supabase.table("loans")\
            .select("*")\
            .eq("tenant_id", tenant_id)\
            .execute()
        return pd.DataFrame(res.data)

    loans_df = load_loans()

    st.markdown("""
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    [data-testid="stMetric"] {
        background: #111;
        padding: 15px;
        border-radius: 12px;
    }

    div[data-baseweb="select"] {
        border-radius: 10px;
    }

    .stButton>button {
        border-radius: 10px;
        height: 45px;
        font-weight: 600;
    }
    </style>
    """, unsafe_allow_html=True)

    # ======================
    # CLEAN DATA
    # ======================
    if loans_df.empty:
        loans_df = pd.DataFrame(columns=[
            "Loan_ID", "Borrower", "Principal", "Interest",
            "Total_Repayable", "Amount_Paid",
            "Start_Date", "End_Date", "Status"
        ])

    # Normalize columns
    loans_df.columns = [c.replace(" ", "_") for c in loans_df.columns]

    for col in ["Principal", "Interest", "Total_Repayable", "Amount_Paid"]:
        loans_df[col] = pd.to_numeric(loans_df.get(col, 0), errors="coerce").fillna(0)

    loans_df["Balance"] = (loans_df["Total_Repayable"] - loans_df["Amount_Paid"]).clip(lower=0)
    
    # Consistent date format for comparison
    today = date.today()

    # ======================
    # STATUS ENGINE
    # ======================
    def get_status(row):
        try:
            # Handle both string and datetime objects
            due_date = pd.to_datetime(row["End_Date"]).date()
        except:
            return "Pending"

        if row["Amount_Paid"] >= row["Total_Repayable"]:
            return "Cleared"
        if due_date < today:
            return "Overdue"
        if row["Amount_Paid"] == 0:
            return "BCF"
        return "Pending"

    if not loans_df.empty:
        loans_df["Status"] = loans_df.apply(get_status, axis=1)

    # ======================
    # KPI DASHBOARD
    # ======================
    total_disbursed = loans_df["Principal"].sum()
    total_collected = loans_df["Amount_Paid"].sum()
    outstanding = loans_df["Balance"].sum()
    overdue = loans_df[loans_df["Status"] == "Overdue"]["Balance"].sum() if not loans_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("💰 Disbursed", f"{total_disbursed:,.0f}")
    c2.metric("💵 Collected", f"{total_collected:,.0f}")
    c3.metric("📊 Outstanding", f"{outstanding:,.0f}")
    c4.metric("🚨 Overdue", f"{overdue:,.0f}")

    st.markdown("---")

    # ======================
    # TABS
    # ======================
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Portfolio", 
        "👤 Borrower View", 
        "➕ New Loan", 
        "🔄 Rollover", 
        "⚙️ Admin"
    ])

    # 📊 TAB 1: PORTFOLIO TABLE
    with tab1:
        def color_rows(row):
            colors = {
                "Cleared": 'background-color:#00C853;color:white',
                "BCF": 'background-color:#FFA726',
                "Pending": 'background-color:#FFEB3B',
                "Overdue": 'background-color:#FF1744;color:white'
            }
            return [colors.get(row["Status"], '')] * len(row)

        if not loans_df.empty:
            st.dataframe(
                loans_df.sort_values(["Loan_ID", "Start_Date"])
                .style
                .format("{:,.0f}", subset=["Principal", "Interest", "Total_Repayable", "Amount_Paid", "Balance"])
                .apply(color_rows, axis=1),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No loans recorded yet.")

    # ==============================
    # ➕ TAB: NEW LOAN (ENTERPRISE)
    # ==============================
    with tab3:
        st.subheader("➕ Issue New Loan")

        # 🔽 LOAD BORROWERS FROM SUPABASE
        borrowers_res = supabase.table("borrowers")\
            .select("id,name,phone")\
            .eq("tenant_id", tenant_id)\
            .execute()

        borrowers_df = pd.DataFrame(borrowers_res.data)

        if borrowers_df.empty:
            st.warning("No borrowers found. Create borrowers first.")
            st.stop()

        # 🔍 SEARCHABLE DROPDOWN
        borrowers_df["label"] = borrowers_df.apply(
            lambda x: f"{x['name']} • {x['phone']}",
            axis=1
        )

        selected_label = st.selectbox(
            "Search Borrower",
            borrowers_df["label"]
        )

        selected_row = borrowers_df[
            borrowers_df["label"] == selected_label
        ].iloc[0] if not loans_df.empty else None

        st.markdown("### 💰 Loan Details")

        col1, col2, col3 = st.columns(3)

        with col1:
            principal = st.number_input("Principal", min_value=0.0)

        with col2:
            rate = st.number_input("Interest (%)", value=3.0)

        with col3:
            days = st.number_input("Duration (days)", value=30)

        if st.button("🚀 Create Loan", use_container_width=True):

            interest = principal * (rate / 100)
            total = principal + interest

            new_loan = {
                "tenant_id": tenant_id,
                "loan_id": f"LN-{int(datetime.now().timestamp())}",
                "borrower_id": selected_row["id"],  # 🔥 KEY FIX
                "borrower": selected_row["name"],
                "principal": principal,
                "interest": interest,
                "total_repayable": total,
                "amount_paid": 0,
                "balance": total,
                "start_date": str(date.today()),
                "end_date": str(date.today() + timedelta(days=int(days))),
                "status": "BCF",
                "cycle": 1
            }

            supabase.table("loans").insert(new_loan).execute()

            st.success(f"Loan created for {selected_row['name']}")
            st.rerun()

    # 🔄 TAB 4: ROLLOVER ENGINE
    with tab4:
        st.subheader("Rollover Engine")
        if not loans_df.empty:
            latest = loans_df.sort_values("Start_Date").groupby("Loan_ID").last().reset_index()
            eligible = latest[(latest["Amount_Paid"] == 0) & (latest["Status"] != "Cleared")]

            if eligible.empty:
                st.info("No eligible loans for rollover (only loans with 0 payments can be rolled over).")
            else:
                selected_b = st.selectbox("Select Loan to Rollover", eligible["Borrower"])
                row = eligible[eligible["Borrower"] == selected_b].iloc[0] if not loans_df.empty else None

                st.write(f"Current Balance: **{row['Total_Repayable']:,.0f}**")
                roll_rate = st.slider("Rollover Interest Rate", 0.01, 0.20, 0.03)

                new_int = row["Total_Repayable"] * roll_rate
                st.warning(f"New Loan: {row['Total_Repayable']:,.0f} + {new_int:,.0f} Interest")

                if st.button("Apply Rollover"):
                    new_entry = {
                        "Loan_ID": row["Loan_ID"],
                        "Borrower": row["Borrower"],
                        "Principal": row["Total_Repayable"],
                        "Interest": new_int,
                        "Total_Repayable": row["Total_Repayable"] + new_int,
                        "Amount_Paid": 0,
                        "Start_Date": str(today),
                        "End_Date": str(today + timedelta(days=30)),
                        "tenant_id": tenant_id,
                        "Status": "BCF"
                    }
                    st.session_state.loans = pd.concat([loans_df, pd.DataFrame([new_entry])], ignore_index=True)
                    st.success("Rollover Applied")
                    st.rerun()

    # ======================
    # ⚙️ ADMIN
    # ======================
    with tab5:
        st.subheader("System Controls")

        if st.button("🔄 Auto Process All Rollovers"):
            new_rows = []

            latest = loans_df.sort_values("Start_Date").groupby("Loan_ID").last().reset_index()

            for _, row in latest.iterrows():
                if row["Amount_Paid"] == 0:
                    new_rows.append({
                        "Loan_ID": row["Loan_ID"],
                        "Borrower": row["Borrower"],
                        "Principal": row["Total_Repayable"],
                        "Interest": row["Total_Repayable"] * 0.03,
                        "Total_Repayable": row["Total_Repayable"] * 1.03,
                        "Amount_Paid": 0,
                        "Start_Date": datetime.now().strftime("%Y-%m-%d"),
                        "End_Date": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d"),
                        "tenant_id": tenant_id
                    })

            if new_rows:
                st.session_state.loans = pd.concat([loans_df, pd.DataFrame(new_rows)], ignore_index=True)
                st.success("All rollovers processed")
                st.rerun()

        if loans_df.empty:
            st.info("No loans")
            return

        loans_df["label"] = loans_df.apply(
            lambda r: f"{r['Borrower']} • {r['Loan_ID']} • {r['Balance']:,.0f}",
            axis=1
        )

        selected = st.selectbox("Select Loan", loans_df["label"])
        row = loans_df[loans_df["label"] == selected].iloc[-1]

        new_status = st.selectbox("Status", ["BCF","Pending","Cleared"])

        if st.button("Update"):
            st.success("Updated (connect Supabase here)")
            st.rerun()

        if st.button("Delete Loan History"):
            st.warning("Deleted (connect Supabase here)")
            st.rerun()

            
import pandas as pd
from datetime import datetime
import uuid
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

    # ==============================
    # 🛡️ NORMALIZATION
    # ==============================
    for df in [loans_df, payments_df, borrowers_df]:
        if not df.empty:
            df.columns = df.columns.str.lower().str.strip()

    if "id" in borrowers_df.columns:
        borrowers_df["id"] = borrowers_df["id"].astype(str)

    if "borrower_id" in loans_df.columns:
        loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)

    if "id" in loans_df.columns:
        loans_df["id"] = loans_df["id"].astype(str)

    if "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].astype(str)

    # ==============================
    # 👤 BORROWER NAME RESOLUTION
    # ==============================
    if (not borrowers_df.empty and "id" in borrowers_df.columns and 
        "name" in borrowers_df.columns and "borrower_id" in loans_df.columns):
        borrower_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].map(borrower_map).fillna("Unknown")
    else:
        if "borrower" not in loans_df.columns:
            loans_df["borrower"] = "Unknown"

    # ==============================
    # 📊 DEFAULT FIELDS
    # ==============================
    if "status" not in loans_df.columns:
        loans_df["status"] = "ACTIVE"

    loans_df["status"] = loans_df["status"].astype(str).str.upper()

    loans_df["amount_paid"] = pd.to_numeric(loans_df.get("amount_paid", 0), errors="coerce").fillna(0)
    loans_df["total_repayable"] = pd.to_numeric(loans_df.get("total_repayable", 0), errors="coerce").fillna(0)

    # ==============================
    # 🔥 CRITICAL FIX: SYNC PAYMENTS → LOANS
    # ==============================
    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(payments_df.get("amount", 0), errors="coerce").fillna(0)
        payment_sums = payments_df.groupby("loan_id")["amount"].sum().to_dict()
        loans_df["amount_paid"] = loans_df["id"].map(payment_sums).fillna(0)

    # ==============================
    # 📑 TABS
    # ==============================
    tab1, tab2 = st.tabs(["➕ Record Payment", "📜 History"])

    # ==============================
    # ➕ TAB 1: RECORD PAYMENT
    # ==============================
    with tab1:
        active_loans = loans_df[loans_df["status"] == "ACTIVE"].copy()

        if active_loans.empty:
            st.success("🎉 No active loans.")
        else:
            search = st.text_input("🔍 Search borrower or loan")
            if search:
                search = search.lower()
                active_loans = active_loans[
                    active_loans.apply(lambda r: search in str(r.get("borrower", "")).lower() 
                    or search in str(r.get("id", "")).lower(), axis=1)
                ]

            if active_loans.empty:
                st.warning("No matching loans.")
            else:
                def format_loan(row):
                    balance = row["total_repayable"] - row["amount_paid"]
                    return f"{row['borrower']} • UGX {balance:,.0f}"

                options = {format_loan(row): row["id"] for _, row in active_loans.iterrows()}
                selected = st.selectbox("Select Loan", list(options.keys()))
                loan_id = options[selected]
                loan = active_loans[active_loans["id"] == loan_id].iloc[0] if not df.empty else None if not df.empty else None

                total = loan["total_repayable"]
                paid = loan["amount_paid"]
                balance = total - paid

                c1, c2 = st.columns(2)
                c1.metric("👤 Borrower", loan["borrower"])
                c2.metric("💰 Balance", f"UGX {balance:,.0f}")

                with st.form("payment_form"):
                    amount = st.number_input("Amount", min_value=0.0)
                    method = st.selectbox("Method", ["Cash", "Mobile Money", "Bank"])
                    date = st.date_input("Date", datetime.now())
                    submit = st.form_submit_button("Post Payment")

                # ✅ OUTSIDE FORM
                if submit:
                    if amount <= 0:
                        st.warning("Enter valid amount.")
                    else:
                        try:
                            tenant_id = st.session_state.get("tenant_id")

                            if not tenant_id:
                                st.error("❌ Tenant not found. Please login again.")
                                st.stop()

                            receipt_no = generate_receipt_no(supabase, tenant_id)
                            loan_label = f"LN-{loan_id[:6]}"

                            # ✅ INSERT PAYMENT
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

                            # ✅ UPDATE LOAN
                            new_paid = paid + amount
                            new_status = "CLOSED" if new_paid >= total else "ACTIVE"

                            supabase.table("loans").update({
                                "amount_paid": new_paid,
                                "status": new_status,
                                "loan_id_label": loan_label
                            }).eq("id", loan_id).execute()

                            # ✅ RECEIPT
                            file_path = f"/tmp/{receipt_no}.pdf"
                            generate_receipt_pdf({
                                "Receipt No": receipt_no,
                                "Borrower": loan["borrower"],
                                "Amount": f"UGX {amount:,.0f}",
                                "Method": method,
                                "Date": date.strftime("%Y-%m-%d"),
                                "Recorded By": st.session_state.get("user", "Staff")
                            }, file_path)

                            with open(file_path, "rb") as f:
                                st.session_state["receipt_pdf"] = f.read()
                                st.session_state["show_receipt"] = True

                            st.success(f"✅ Payment recorded | Receipt: {receipt_no}")

                            st.cache_data.clear()
                            st.rerun()

                        except Exception as e:
                            st.error(f"❌ {e}")

        # ==============================
        # 📥 DOWNLOAD RECEIPT
        # ==============================
        if st.session_state.get("show_receipt"):
            st.download_button(
                "📥 Download Latest Receipt",
                data=st.session_state["receipt_pdf"],
                file_name="receipt.pdf",
                mime="application/pdf"
            )

            if st.button("Clear Receipt"):
                st.session_state["show_receipt"] = False
                st.rerun()

    # ==============================
    # 📜 TAB 2: HISTORY
    # ==============================
    with tab2:
        if payments_df.empty:
            st.info("No payments yet.")
            return

        df = payments_df.copy()

        df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0)
        df["amount"] = df["amount"].apply(lambda x: f"UGX {x:,.0f}")

        if "date" in df.columns:
            df = df.sort_values("date", ascending=False)

        cols = [c for c in ["date", "borrower", "amount", "method", "receipt_no"] if c in df.columns]
        st.dataframe(df[cols], use_container_width=True, hide_index=True)
# ==============================
# 15. COLLATERAL MANAGEMENT PAGE (SAAS + ENTERPRISE UPGRADE)
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
    # FETCH DATA (SAFE)
    # ==============================
    # Using the safe adapters from previous parts
    collateral_df = get_data("collateral") 
    loans_df = get_data("loans")

    # ==============================
    # NORMALIZE LOANS COLUMNS (SAFE)
    # ==============================
    if loans_df is not None and not loans_df.empty:
        # Filter for loans that actually need collateral (Active, Overdue, Pending)
        Active_statuses = ["Active", "overdue", "pending"]
        available_loans = loans_df[loans_df["status"].str.lower().isin(Active_statuses)].copy()
    else:
        available_loans = pd.DataFrame()

    # --- TABS ---
    tab_reg, tab_view = st.tabs(["➕ Register Asset", "📋 Inventory & Status"])

    # ==============================
    # TAB 1: REGISTER COLLATERAL
    # ==============================
    with tab_reg:
        if available_loans.empty:
            st.warning("⚠️ No Active loans found to attach collateral to.")
        else:
            with st.form("collateral_reg_form", clear_on_submit=True):
                st.write("### Link Asset to Loan")
                c1, c2 = st.columns(2)

                # Create dropdown labels: "Borrower Name (Loan ID)"
                loan_labels = available_loans.apply(
                    lambda x: f"{x['borrower']} (ID: {x['loan_id']})", axis=1
                ).tolist()
                
                selected_label = c1.selectbox("Select Loan/Borrower", options=loan_labels)
                asset_type = c2.selectbox(
                    "Asset Type",
                    ["Logbook (Car)", "Land Title", "Electronics", "House Deed", "Business Stock", "Other"]
                )

                desc = st.text_input("Detailed Asset Description (e.g. Plate No, Plot No)")
                est_value = st.number_input("Estimated Market Value (UGX)", min_value=0, step=100000)
                
                st.markdown("---")
                # Photo upload (Note: In a real app, this would stream to Supabase Storage)
                uploaded_photo = st.file_uploader("Upload Asset Photo (Verification)", type=["jpg", "png", "jpeg"])

                if st.form_submit_button("💾 Save & Secure Asset", use_container_width=True):
                    if not desc or est_value <= 0:
                        st.error("Please provide a description and valid value.")
                    else:
                        # Extract the actual loan_id from the label
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
    # TAB 2: INVENTORY & STATUS
    # ==============================
    with tab_view:
        if collateral_df is None or collateral_df.empty:
            st.info("💡 No assets currently in the registry.")
        else:
            # Metrics
            total_value = collateral_df["value"].sum()
            held_count = len(collateral_df[collateral_df["status"] == "In Custody"])
            
            m1, m2 = st.columns(2)
            m1.metric("Total Asset Value", f"UGX {total_value:,.0f}")
            m2.metric("Items in Custody", held_count)

            st.markdown("### Asset Ledger")
            
            # Clean display for the table
            display_df = collateral_df.copy()
            display_df["value"] = display_df["value"].apply(lambda x: f"{x:,.0f}")
            
            st.dataframe(
                display_df[["date_added", "borrower", "type", "description", "value", "status"]],
                use_container_width=True,
                hide_index=True
            )

            # --- MANAGE SECTION ---
            with st.expander("⚙️ Release or Dispose Assets"):
                # Filter for assets that haven't been released yet
                manageable = collateral_df[collateral_df["status"] != "Released"].copy()
                
                if manageable.empty:
                    st.write("All assets are currently released.")
                else:
                    asset_to_manage = st.selectbox(
                        "Select Asset to Update", 
                        manageable.apply(lambda x: f"{x['borrower']} - {x['description']}", axis=1)
                    )
                    
                    # Logic to find the specific ID
                    selected_idx = manageable.index[manageable.apply(lambda x: f"{x['borrower']} - {x['description']}", axis=1) == asset_to_manage][0]
                    asset_id = manageable.at[selected_idx, "id"] if "id" in manageable.columns else None

                    col_stat, col_btn = st.columns([2,1])
                    new_stat = col_stat.selectbox("Set New Status", ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"])
                    
                    if col_btn.button("Update Status", use_container_width=True):
                        update_row = pd.DataFrame([{
                            "id": asset_id, # Requires the DB generated ID
                            "status": new_stat,
                            "tenant_id": str(current_tenant)
                        }])
                        
                        if save_data_saas("collateral", update_row):
                            st.success("Asset status updated!")
                            st.cache_data.clear()
                            st.rerun()
            

# ==============================
# 17. ACTIVITY CALENDAR PAGE
# ==============================
def show_calendar():
    
    st.markdown("<h2 style='color: #2B3F87;'>📅 Activity Calendar</h2>", unsafe_allow_html=True)

    # 1. FETCH DATA
    loans_df = get_cached_data("loans")
    borrowers_df = get_cached_data("borrowers") # Added to fetch names

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    # --- INJECT BORROWER NAMES (Fixes the "Strings" in Calendar & Tables) ---
    if borrowers_df is not None and not borrowers_df.empty:
        bor_map = dict(zip(borrowers_df['id'], borrowers_df['name']))
        loans_df['borrower'] = loans_df['borrower_id'].map(bor_map).fillna("Unknown")
    else:
        loans_df['borrower'] = "Unknown"

    # Standardize types for SaaS logic
    loans_df["end_date"] = pd.to_datetime(loans_df["end_date"], errors="coerce")
    loans_df["total_repayable"] = pd.to_numeric(loans_df["total_repayable"], errors="coerce").fillna(0)
    
    today = pd.Timestamp.today().normalize()
    
    # Filter for active loans
    active_loans = loans_df[loans_df["status"].astype(str).str.lower() != "closed"].copy()

    # --- VISUAL CALENDAR WIDGET ---
    calendar_events = []
    for _, r in active_loans.iterrows():
        if pd.notna(r['end_date']):
            # Color logic: Red for overdue, Blue for upcoming
            is_overdue = r['end_date'].date() < today.date()
            ev_color = "#FF4B4B" if is_overdue else "#4A90E2"
            
            # Use the mapped 'borrower' name here
            calendar_events.append({
                "title": f"UGX {float(r['total_repayable']):,.0f} - {r['borrower']}",
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

    # 2. DAILY WORKLOAD METRICS
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

    # 3. REVENUE FORECAST (This Month)
    st.markdown("---")
    st.markdown("<h4 style='color: #2B3F87;'>📊 Revenue Forecast (This Month)</h4>", unsafe_allow_html=True)
    
    this_month_df = active_loans[active_loans["end_date"].dt.month == today.month]
    total_expected = this_month_df["total_repayable"].sum()
    
    f1, f2 = st.columns(2)
    f1.metric("Expected Collections", f"{total_expected:,.0f} UGX")
    f2.metric("Remaining Appointments", len(this_month_df))

    # 4. ACTION ITEMS (Formatted with Human Names)
    st.markdown("<h4 style='color: #2B3F87;'>📌 Action Items for Today</h4>", unsafe_allow_html=True)
    if due_today_df.empty:
        st.success("✨ No deadlines for today.")
    else:
        # Fixed: Using 'loan_id_label' for ID and 'borrower' for name
        today_rows = "".join([f"""<tr style="background:#F0F8FF;"><td style="padding:10px;"><b>#{r.get('loan_id_label', r['id'])}</b></td><td style="padding:10px;">{r['borrower']}</td><td style="padding:10px;text-align:right;">{r['total_repayable']:,.0f}</td><td style="padding:10px;text-align:center;"><span style="background:#2B3F87;color:white;padding:2px 8px;border-radius:10px;font-size:10px;">💰 COLLECT NOW</span></td></tr>""" for _, r in due_today_df.iterrows()])
        st.markdown(f"""<div style="border:2px solid #2B3F87;border-radius:10px;overflow:hidden;"><table style="width:100%;border-collapse:collapse;font-size:12px;"><tr style="background:#2B3F87;color:white;"><th style="padding:10px;">ID</th><th style="padding:10px;">Borrower</th><th style="padding:10px;text-align:right;">Amount</th><th style="padding:10px;text-align:center;">Action</th></tr>{today_rows}</table></div>""", unsafe_allow_html=True)

    # 5. OVERDUE FOLLOW-UP
    st.markdown("<br><h4 style='color: #FF4B4B;'>🔴 Past Due (Immediate Attention)</h4>", unsafe_allow_html=True)
    overdue_df = active_loans[active_loans["end_date"] < today].copy()
    if not overdue_df.empty:
        overdue_df["days_late"] = (today - overdue_df["end_date"]).dt.days
        od_rows = ""
        for _, r in overdue_df.iterrows():
            late_color = "#FF4B4B" if r['days_late'] > 7 else "#FFA500"
            # Fixed: Using 'loan_id_label' for ID and 'borrower' for name
            od_rows += f"""<tr style="background:#FFF5F5;"><td style="padding:10px;"><b>#{r.get('loan_id_label', r['id'])}</b></td><td style="padding:10px;">{r['borrower']}</td><td style="padding:10px;color:{late_color};font-weight:bold;">{r['days_late']} Days</td><td style="padding:10px;text-align:center;"><span style="background:{late_color};color:white;padding:2px 8px;border-radius:10px;font-size:10px;">{r['status']}</span></td></tr>"""
        st.markdown(f"""<div style="border:2px solid #FF4B4B;border-radius:10px;overflow:hidden;"><table style="width:100%;border-collapse:collapse;font-size:12px;"><tr style="background:#FF4B4B;color:white;"><th style="padding:10px;">ID</th><th style="padding:10px;">Borrower</th><th style="padding:10px;text-align:center;">Late By</th><th style="padding:10px;text-align:center;">Status</th></tr>{od_rows}</table></div>""", unsafe_allow_html=True)                                                                
                                                                                                                                                                                                                                                                 
# ==============================
# 18. EXPENSE MANAGEMENT PAGE (SAAS + ENTERPRISE UPGRADE)
# ==============================

import plotly.express as px
import uuid
import pandas as pd
import streamlit as st
from datetime import datetime

def show_expenses():
    """
    Tracks business operational costs for specific tenants.
    (Upgraded for enterprise SaaS safety)
    """
    st.markdown("<h2 style='color: #2B3F87;'>📁 Expense Management</h2>", unsafe_allow_html=True)
    
    # ==============================
    # 🔐 SAAS TENANT CONTEXT (HARDENED)
    # ==============================
    current_tenant = st.session_state.get('tenant_id', 'default_tenant')

    # ==============================
    # 1. FETCH DATA (SAFE WRAPPER ADDED)
    # ==============================
    try:
        # Pulling data using your existing cache logic
        df = get_cached_data("expenses")
    except Exception:
        df = pd.DataFrame()

    # ==============================
    # SAAS FILTER (UNCHANGED LOGIC + SAFETY)
    # ==============================
    if df is not None and not df.empty:
        if "tenant_id" in df.columns:
            df = df[df["tenant_id"].astype(str) == str(current_tenant)]
        else:
            df["tenant_id"] = current_tenant

    EXPENSE_CATS = ["Rent", "Insurance Account", "Utilities", "Salaries", "Marketing", "Office Expenses"]

    # ==============================
    # EMPTY DATA SAFE INIT (UNCHANGED STRUCTURE)
    # ==============================
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "id", "category", "amount", "date",
            "description", "payment_date", "receipt_no", "tenant_id"
        ])

    # ==============================
    # COLUMN GUARANTEE (NO LOGIC REMOVED)
    # ==============================
    for col in ["id", "category", "amount", "date", "description",
                "payment_date", "receipt_no", "tenant_id"]:
        if col not in df.columns:
            df[col] = None

    # ==============================
    # TABS (UNCHANGED)
    # ==============================
    tab_add, tab_view, tab_manage = st.tabs([
        "➕ Record Expense", "📊 Spending Analysis", "⚙️ Manage/Delete"
    ])

    # ==============================
    # TAB 1: ADD (SAFE WRAPPER ONLY)
    # ==============================
    with tab_add:
        with st.form("add_expense_form", clear_on_submit=True):
            col1, col2 = st.columns(2)

            category = col1.selectbox("Category", EXPENSE_CATS)
            amount = col2.number_input("Amount (UGX)", min_value=0, step=1000)
            desc = st.text_input("Description")

            c_date, c_receipt = st.columns(2)
            p_date = c_date.date_input("Actual Payment Date", value=datetime.now())
            receipt_no = c_receipt.text_input("Receipt / Invoice #")

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
                            "tenant_id": current_tenant
                        }])

                        updated_df = pd.concat([df, new_entry], ignore_index=True)

                        if save_data("expenses", updated_df):
                            st.success("✅ Expense recorded!")
                            st.cache_data.clear() # Ensure analytics update
                            st.rerun()
                    except Exception as e:
                        st.error(f"🚨 Save failed: {e}")
                else:
                    st.error("⚠️ Provide amount & description.")

    # --- TAB 2: ANALYSIS & LOG ---
    with tab_view:
        if not df.empty:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
            total_spent = df["amount"].sum()
            
            st.markdown(f"""<div style="background-color:#fff;padding:20px;border-radius:15px;border-left:5px solid #FF4B4B;box-shadow:2px 2px 10px rgba(0,0,0,0.05);"><p style="margin:0;font-size:12px;color:#666;font-weight:bold;">TOTAL MONTHLY OUTFLOW</p><h2 style="margin:0;color:#FF4B4B;">{total_spent:,.0f} <span style="font-size:14px;">UGX</span></h2></div>""", unsafe_allow_html=True)
            
            # Pie Chart Analysis (Branding Preserved)
            cat_summary = df.groupby("category")["amount"].sum().reset_index()
            fig_exp = px.pie(cat_summary, names="category", values="amount", title="Spending Distribution", hole=0.4, color_discrete_sequence=["#2B3F87", "#F0F8FF", "#FF4B4B", "#ADB5BD"])
            fig_exp.update_layout(paper_bgcolor='rgba(0,0,0,0)', font_color="#2B3F87")
            st.plotly_chart(fig_exp, use_container_width=True)
            
            # Detailed Expense Log (Custom HTML Table)
            rows_html = ""
            for i, r in df.sort_values("date", ascending=False).reset_index().iterrows():
                bg = "#F0F8FF" if i % 2 == 0 else "#FFFFFF"
                rows_html += f"""<tr style="background-color:{bg};border-bottom:1px solid #ddd;"><td style="padding:10px;color:#666;font-size:11px;">{r['date']}</td><td style="padding:10px;"><b>{r['category']}</b></td><td style="padding:10px;font-size:11px;">{r['description']}</td><td style="padding:10px;text-align:right;font-weight:bold;color:#FF4B4B;">{float(r['amount']):,.0f}</td><td style="padding:10px;text-align:center;color:#666;font-size:10px;">{r['receipt_no']}</td></tr>"""

            st.markdown(f"""<div style="border:2px solid #2B3F87;border-radius:10px;overflow:hidden;"><table style="width:100%;border-collapse:collapse;font-size:12px;"><thead><tr style="background:#2B3F87;color:white;text-align:left;"><th style="padding:12px;">Date</th><th style="padding:12px;">Category</th><th style="padding:12px;">Description</th><th style="padding:12px;text-align:right;">Amount (UGX)</th><th style="padding:12px;text-align:center;">Receipt #</th></tr></thead><tbody>{rows_html}</tbody></table></div>""", unsafe_allow_html=True)

    # ==============================
    # TAB 3: MANAGE (ENTERPRISE SAFE)
    # ==============================
    with tab_manage:
        st.markdown("### 🛠️ Manage Outflow Records")

        if df.empty:
            st.info("ℹ️ No expenses found to manage.")
        else:
            try:
                df = df.copy()

                df["id"] = df["id"].astype(str)
                df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

                # Create a selection label for the dropdown
                df["label"] = df.apply(
                    lambda r: f"{r['category']} - {r['amount']:,.0f} UGX | {str(r['payment_date'])[:10]}",
                    axis=1
                )

                exp_map = {row["label"]: row for _, row in df.iterrows()}
                selected_label = st.selectbox("🔍 Select Expense to Edit/Delete", list(exp_map.keys()))

                if selected_label:
                    exp_to_edit = exp_map[selected_label]
                    exp_id = exp_to_edit["id"]

                    with st.form("edit_expense_form"):
                        up_amt = st.number_input("Update Amount", value=float(exp_to_edit['amount']))
                        
                        if st.form_submit_button("💾 Save Changes"):
                            df.loc[df["id"] == exp_id, "amount"] = up_amt
                            # Clean up the helper label before saving
                            final_df = df.drop(columns=['label'])
                            if save_data("expenses", final_df):
                                st.success("✅ Updated!")
                                st.cache_data.clear()
                                st.rerun()

                    st.divider()

                    if st.button("🗑️ Delete Selected Expense", type="secondary", use_container_width=True):
                        df = df[df["id"] != exp_id]
                        final_df = df.drop(columns=['label'])
                        if save_data("expenses", final_df):
                            st.success("✅ Deleted!")
                            st.cache_data.clear()
                            st.rerun()

            except Exception as e:
                st.error(f"🚨 Manage error: {e}")
# ==============================
# 19. PETTY CASH MANAGEMENT PAGE
# ==============================

def show_petty_cash():
    """
    Manages daily office cash transactions. Tracks inflows/outflows
    for specific tenants with real-time balance alerts.
    """

    # ==============================
    # 🎨 BANKING UI SYSTEM
    # ==============================
    st.markdown("""
    <style>

    .block-container {
        padding-top: 1.2rem;
    }

    /* Glass Cards */
    .glass-card {
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        background: linear-gradient(145deg, rgba(255,255,255,0.7), rgba(255,255,255,0.4));
        border-radius: 16px;
        padding: 18px;
        border: 1px solid rgba(255,255,255,0.25);
        box-shadow: 0 6px 20px rgba(0,0,0,0.06);
        transition: all 0.25s ease;
    }

    .glass-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 30px rgba(0,0,0,0.1);
    }

    /* Metric Titles */
    .metric-title {
        font-size: 11px;
        color: #6b7280;
        font-weight: 600;
        letter-spacing: 0.6px;
    }

    /* Metric Values */
    .metric-value {
        font-size: 22px;
        font-weight: 700;
        margin-top: 6px;
    }

    /* Status badge */
    .status-badge {
        font-size: 10px;
        padding: 3px 8px;
        border-radius: 20px;
        margin-left: 6px;
        font-weight: 600;
    }

    .safe {
        background: rgba(16,185,129,0.15);
        color: #10B981;
    }

    .low {
        background: rgba(255,75,75,0.15);
        color: #FF4B4B;
    }

    /* Tabs */
    .stTabs [role="tab"] {
        font-weight: 600;
        padding: 10px 18px;
    }

    .stTabs [aria-selected="true"] {
        border-bottom: 3px solid #2B3F87;
    }

    </style>
    """, unsafe_allow_html=True)

    st.markdown("<h2 style='color:#2B3F87;'>💵 Petty Cash Management</h2>", unsafe_allow_html=True)

    # 1. FETCH TENANT DATA
    df = get_cached_data("petty_cash")

    if df.empty:
        df = pd.DataFrame(columns=["id", "type", "amount", "date", "description"])
    else:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    # 2. SMART BALANCE METRICS
    inflow = df[df["type"] == "In"]["amount"].sum()
    outflow = df[df["type"] == "Out"]["amount"].sum()
    balance = inflow - outflow

    # Balance intelligence
    bal_color = "#10B981" if balance >= 50000 else "#FF4B4B"
    bal_status = "SAFE" if balance >= 50000 else "LOW"

    # ==============================
    # 💎 METRIC CARDS
    # ==============================
    c1, c2, c3 = st.columns(3)

    c1.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">TOTAL CASH IN</div>
        <div class="metric-value" style="color:#10B981;">
            {inflow:,.0f} UGX
        </div>
    </div>
    """, unsafe_allow_html=True)

    c2.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">TOTAL CASH OUT</div>
        <div class="metric-value" style="color:#FF4B4B;">
            {outflow:,.0f} UGX
        </div>
    </div>
    """, unsafe_allow_html=True)

    c3.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">
            CURRENT BALANCE
            <span class="status-badge {'safe' if balance >= 50000 else 'low'}">
                {bal_status}
            </span>
        </div>
        <div class="metric-value" style="color:{bal_color};">
            {balance:,.0f} UGX
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ==============================
    # 📊 SECTION HEADER
    # ==============================
    st.markdown("### 📊 Cash Activity")

    tab_record, tab_history = st.tabs(["➕ Record Entry", "📜 Transaction History"])

    # --- TAB 1 ---
    with tab_record:
        with st.form("petty_cash_form", clear_on_submit=True):
            col_a, col_b = st.columns(2)
            ttype = col_a.selectbox("Transaction Type", ["Out", "In"])
            t_amount = col_b.number_input("Amount (UGX)", min_value=0, step=1000)
            desc = st.text_input("Purpose / Description", placeholder="e.g., Office Water Refill")

            if st.form_submit_button("💾 Save to Cashbook"):
                if t_amount > 0 and desc:
                    new_row = pd.DataFrame([{
                        "type": ttype,
                        "amount": float(t_amount),
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "description": desc,
                        "tenant_id": st.session_state.tenant_id
                    }])
                    
                    if save_data("petty_cash", new_row):
                        st.success(f"Successfully recorded {t_amount:,.0f} UGX!")
                        st.rerun()
                else:
                    st.error("Please provide amount and description.")

    # --- TAB 2 ---
    with tab_history:
        st.markdown("### 📜 Transaction Log")

        if not df.empty:
            def color_type(val):
                return 'color: #10B981;' if val == 'In' else 'color: #FF4B4B;'
            
            st.dataframe(
                df.sort_values("date", ascending=False)
                .style.map(color_type, subset=['type'])
                .format({"amount": "{:,.0f}"}),
                use_container_width=True, hide_index=True
            )

            st.markdown("<br>", unsafe_allow_html=True)

            with st.expander("⚙️ Advanced: Edit or Delete Transaction"):
                options = [f"ID: {int(row['id'])} | {row['type']} - {row['description']}" for _, row in df.iterrows()]
                selected_task = st.selectbox("Select Entry to Modify", options)
                
                sel_id = int(selected_task.split(" | ")[0].replace("ID: ", ""))
                item = df[df["id"] == sel_id].iloc[0] if not df.empty else None if not df.empty else None

                up_type = st.selectbox("Update Type", ["In", "Out"], index=0 if item["type"] == "In" else 1)
                up_amt = st.number_input("Update Amount", value=float(item["amount"]), step=1000.0)
                up_desc = st.text_input("Update Description", value=str(item["description"]))

                c_up, c_del = st.columns(2)
                if c_up.button("💾 Save Changes", use_container_width=True):
                    update_entry = pd.DataFrame([{
                        "id": sel_id,
                        "type": up_type,
                        "amount": up_amt,
                        "description": up_desc,
                        "tenant_id": st.session_state.tenant_id
                    }])
                    if save_data("petty_cash", update_entry):
                        st.success("Updated Successfully!")
                        st.rerun()

                if c_del.button("🗑️ Delete Permanently", use_container_width=True):
                    supabase.table("petty_cash").delete().eq("id", sel_id).execute()
                    st.warning("Entry Deleted."); st.rerun()
                

# ==============================
# 🚨 OVERDUE TRACKER (AI + BULLETPROOF)
# ==============================

def show_overdue_tracker():
    """
    Tracks overdue loans with AI-style risk scoring.
    Fully hardened for production use.
    """

    # ==============================
    # 🎨 UI SYSTEM
    # ==============================
    st.markdown("""
    <style>
    .glass-card {
        backdrop-filter: blur(10px);
        background: linear-gradient(145deg, rgba(255,255,255,0.7), rgba(255,255,255,0.4));
        border-radius: 16px;
        padding: 18px;
        border: 1px solid rgba(255,255,255,0.25);
        box-shadow: 0 6px 20px rgba(0,0,0,0.06);
    }
    .metric-title {
        font-size: 11px;
        color: #6b7280;
        font-weight: 600;
    }
    .metric-value {
        font-size: 22px;
        font-weight: 700;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<h2 style='color:#2B3F87;'>🚨 AI Overdue Intelligence</h2>", unsafe_allow_html=True)

    # ==============================
    # 📥 FETCH DATA (SAFE)
    # ==============================
    try:
        # CRITICAL: Ensure this function is defined above this block!
        # If using a custom function, ensure it exists. 
        # If using standard Supabase/SQL, it might be 'get_data()'
        loans_raw = get_cached_data("loans") 
    except NameError:
        st.error("Function 'get_cached_data' is not defined. Please check your function names.")
        return
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return

    # Properly indented block
    if loans_raw is None:
        st.warning("No loan data available.")
        return

    # Convert safely
    try:
        loans_df = pd.DataFrame(loans_raw)
    except Exception as e:
        st.error(f"Failed to convert data to table: {e}")
        return

    if loans_df.empty:
        st.warning("No loan data available.")
        return

    # ==============================
    # 🛡️ COLUMN SAFETY LAYER
    # ==============================

    # Ensure required columns exist
    required_cols = ["id", "amount", "due_date"]
    for col in required_cols:
        if col not in loans_df.columns:
            loans_df[col] = None

    # Clean amount safely
    loans_df["amount"] = pd.to_numeric(loans_df["amount"], errors="coerce")
    loans_df["amount"] = loans_df["amount"].fillna(0)

    # Clean due date safely
    loans_df["due_date"] = pd.to_datetime(loans_df["due_date"], errors="coerce")

    # Remove rows with no due_date
    loans_df = loans_df.dropna(subset=["due_date"])

    if loans_df.empty:
        st.info("No valid due dates found.")
        return

    # ==============================
    # 🧠 OVERDUE LOGIC
    # ==============================
    today = pd.Timestamp.today()

    loans_df["days_overdue"] = (today - loans_df["due_date"]).dt.days

    overdue_df = loans_df[loans_df["days_overdue"] > 0].copy()

    if overdue_df.empty:
        st.success("🎉 No overdue loans.")
        return

    # ==============================
    # 🧠 AI SCORING ENGINE
    # ==============================
    def compute_score(row):
        try:
            score = 0

            # Days overdue weight
            score += min(max(row["days_overdue"], 0) * 1.5, 50)

            # Amount weight
            amt = row["amount"]

            if amt > 1_000_000:
                score += 25
            elif amt > 300_000:
                score += 15
            else:
                score += 5

            return min(score, 100)
        except:
            return 0

    overdue_df["risk_score"] = overdue_df.apply(compute_score, axis=1)

    # Prediction label
    def predict(score):
        if score >= 70:
            return "High Risk"
        elif score >= 40:
            return "Watch"
        return "Stable"

    overdue_df["prediction"] = overdue_df["risk_score"].apply(predict)

    # ==============================
    # 💎 KPI METRICS
    # ==============================
    total_overdue = int(len(overdue_df))
    high_risk = int((overdue_df["prediction"] == "High Risk").sum())
    exposure = float(overdue_df["amount"].sum())

    c1, c2, c3 = st.columns(3)

    c1.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">OVERDUE LOANS</div>
        <div class="metric-value">{total_overdue}</div>
    </div>
    """, unsafe_allow_html=True)

    c2.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">HIGH RISK CASES</div>
        <div class="metric-value" style="color:#EF4444;">{high_risk}</div>
    </div>
    """, unsafe_allow_html=True)

    c3.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">TOTAL EXPOSURE</div>
        <div class="metric-value">{exposure:,.0f} UGX</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ==============================
    # 🔍 FILTERS
    # ==============================
    col1, col2 = st.columns(2)

    risk_filter = col1.selectbox("Filter by Risk", ["All", "High Risk", "Watch", "Stable"])
    search_term = col2.text_input("Search")

    filtered_df = overdue_df.copy()

    if risk_filter != "All":
        filtered_df = filtered_df[filtered_df["prediction"] == risk_filter]

    if search_term:
        search_term = search_term.lower()
        filtered_df = filtered_df[
            filtered_df.astype(str).apply(lambda row: search_term in row.to_string().lower(), axis=1)
        ]

    # ==============================
    # 🎨 TABLE
    # ==============================
    def color_pred(val):
        if val == "High Risk":
            return "color:#EF4444; font-weight:700;"
        elif val == "Watch":
            return "color:#F59E0B;"
        return "color:#10B981;"

    st.markdown("### 🔥 Priority List (AI Ranked)")

    st.dataframe(
        filtered_df.sort_values("risk_score", ascending=False)
        .style.map(color_pred, subset=["prediction"])
        .format({"amount": "{:,.0f}", "risk_score": "{:.0f}"}),
        use_container_width=True,
        hide_index=True
    )

    # ==============================
    # 🧠 AI INSIGHT PANEL
    # ==============================
    st.markdown("<br>", unsafe_allow_html=True)

    try:
        worst = overdue_df.sort_values("risk_score", ascending=False).iloc[0] if not df.empty else None if not df.empty else None

        st.markdown(f"""
        <div class="glass-card">
            <b>🧠 AI Insight:</b><br><br>
            Highest risk loan is <b>ID {int(worst['id'])}</b><br>
            • Days overdue: {int(worst['days_overdue'])}<br>
            • Amount: {worst['amount']:,.0f} UGX<br>
            • Risk Score: {worst['risk_score']:.0f}/100<br><br>
            👉 Recommended action: Immediate follow-up.
        </div>
        """, unsafe_allow_html=True)
    except:
        st.info("No insight available.")

    # ==============================
    # ⚙️ ACTION PANEL
    # ==============================
    st.markdown("<br>", unsafe_allow_html=True)

    with st.expander("⚙️ Take Action"):
        options = [
            f"ID: {int(row['id'])} | Score: {row['risk_score']:.0f}"
            for _, row in filtered_df.iterrows()
        ]

        if options:
            selected = st.selectbox("Select Loan", options)
            sel_id = int(selected.split(" | ")[0].replace("ID: ", ""))

            if st.button("📞 Mark as Contacted"):
                st.success("Follow-up recorded.")

            if st.button("✅ Mark as Paid"):
                try:
                    supabase.table("loans").update({"status": "Paid"}).eq("id", sel_id).execute()
                    st.success("Marked as paid.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Update failed: {e}")
        else:
            st.info("No selectable records.")

# ==============================
# 💣 BALLISTIC PAYROLL SYSTEM (ELITE UI)
# ==============================

import pandas as pd
import streamlit as st
from datetime import datetime

def show_payroll():

    # ==============================
    # 🔐 ACCESS CONTROL
    # ==============================
    if st.session_state.get("role") != "Admin":
        st.error("🔒 Restricted Access: Admins only.")
        return

    brand_color = "#2B3F87"

    st.markdown(f"""
    <h2 style='color:{brand_color};'>💣 Payroll Intelligence System</h2>
    """, unsafe_allow_html=True)

    tenant = st.session_state.get("tenant_id")
    if not tenant:
        st.error("Session expired")
        st.stop()

    # ==============================
    # 📥 LOAD DATA
    # ==============================
    df = get_cached_data("payroll")

    if df is not None and not df.empty:
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    if df is None or df.empty:
        df = pd.DataFrame()

    if "tenant_id" in df.columns:
        df = df[df["tenant_id"].astype(str) == str(tenant)]

    # ==============================
    # 🧠 CALC ENGINE (UNCHANGED)
    # ==============================
    def calc_engine(basic, arrears, absent, advance, other):
        basic, arrears = float(basic or 0), float(arrears or 0)
        absent, advance, other = float(absent or 0), float(advance or 0), float(other or 0)

        gross = (basic + arrears) - absent
        lst = 100000 / 12 if gross > 1000000 else 0
        n5, n10 = gross * 0.05, gross * 0.10
        n15 = n5 + n10

        paye = 0
        if gross > 410000:
            paye = 25000 + (0.30 * (gross - 410000))
        elif gross > 235000:
            paye = (gross - 235000) * 0.10

        net = gross - (paye + lst + n5 + advance + other)

        return {
            "gross": round(gross),
            "paye": round(paye),
            "n5": round(n5),
            "n10": round(n10),
            "n15": round(n15),
            "net": round(net),
            "lst": round(lst)
        }

    # ==============================
    # 📊 SUMMARY CARDS
    # ==============================
    if not df.empty:
        total_net = df["net_pay"].sum() if "net_pay" in df else 0
        total_paye = df["paye"].sum() if "paye" in df else 0
        total_nssf = df["nssf_15"].sum() if "nssf_15" in df else 0

        c1, c2, c3 = st.columns(3)

        c1.metric("💰 Total Net Payroll", f"UGX {total_net:,.0f}")
        c2.metric("🏛 PAYE Tax", f"UGX {total_paye:,.0f}")
        c3.metric("🛡 NSSF (15%)", f"UGX {total_nssf:,.0f}")

    st.divider()

    # ==============================
    # 📑 TABS
    # ==============================
    tab_process, tab_logs, tab_ledger = st.tabs(["💳 Process Payroll", "📜 Payroll Ledger", "Logs"])

    # ==============================
    # 💳 PROCESS PAYROLL
    # ==============================
    with tab_process:
        st.markdown("### 🧾 Salary Processing Engine")

        with st.form("payroll_form", clear_on_submit=True):
            name = st.text_input("Employee Name")

            c1, c2, c3 = st.columns(3)
            tin = c1.text_input("TIN")
            role = c2.text_input("Designation")
            mob = c3.text_input("Phone")

            c4, c5 = st.columns(2)
            acc = c4.text_input("Account No.")
            nssf_input = c5.text_input("NSSF No.")

            st.markdown("#### 💵 Salary Inputs")

            c6, c7, c8 = st.columns(3)
            arrears = c6.number_input("Arrears", min_value=0.0)
            basic = c7.number_input("Basic Salary", min_value=0.0)
            absent = c8.number_input("Absent Deduction", min_value=0.0)

            c9, c10 = st.columns(2)
            advance = c9.number_input("Advance", min_value=0.0)
            other = c10.number_input("Other Deductions", min_value=0.0)

            # 🔥 LIVE PREVIEW (Uses the calc_engine defined previously)
            preview = calc_engine(basic, arrears, absent, advance, other)

            st.markdown(f"""
            <div style="padding:15px;border-radius:10px;background:#F8FAFC;margin-top:10px;border:1px solid #E2E8F0;">
                <b style="color:#1E293B;">💡 Live Salary Breakdown</b><br><br>
                <span style="color:#64748B;">Gross:</span> UGX {preview['gross']:,} <br>
                <span style="color:#64748B;">PAYE:</span> UGX {preview['paye']:,} <br>
                <span style="color:#64748B;">NSSF (5%):</span> UGX {preview['n5']:,} <br>
                <span style="color:#1E293B; font-weight:bold;">Net Pay: UGX {preview['net']:,}</span>
            </div>
            """, unsafe_allow_html=True)

            if st.form_submit_button("🚀 Process Payroll", use_container_width=True):
                if name and basic > 0:
                    # Constructing the dataframe with correct indentation
                    payload = pd.DataFrame([{
                        "employee": name,
                        "tin": tin,
                        "designation": role,
                        "mob_no": mob,
                        "account_no": acc,
                        "nssf_no": nssf_input,
                        "arrears": arrears,
                        "basic_salary": basic,
                        "absent_deduction": absent,
                        "gross_salary": preview["gross"],
                        "paye": preview["paye"],
                        "nssf_5": preview["n5"],
                        "nssf_10": preview["n10"],
                        "nssf_15": preview["n15"],
                        "advance_drs": advance,
                        "other_deductions": other,
                        "net_pay": preview["net"],
                        "lst": preview["lst"],
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "tenant_id": str(tenant)
                    }])

                    if save_data("payroll", payload):
                        st.success(f"✅ Payroll processed for {name}")
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.error("⚠️ Please enter a valid employee name and basic salary.")

    # ==============================
    # 📜 PAYROLL TABLE
    # ==============================
    with tab_ledger:
        if not df.empty:
            def fm(x):
                try:
                    return f"{int(float(x or 0)):,}"
                except:
                    return "0"

            rows = ""
            for i, r in df.iterrows():
                rows += f"""
                <tr>
                    <td>{i+1}</td>
                    <td><b>{r.get('employee')}</b><br><small>{r.get('designation')}</small></td>
                    <td style="text-align:right;">{fm(r.get('gross_salary'))}</td>
                    <td style="text-align:right;">{fm(r.get('paye'))}</td>
                    <td style="text-align:right;">{fm(r.get('nssf_5'))}</td>
                    <td style="text-align:right;background:#ECFDF5;font-weight:bold;">{fm(r.get('net_pay'))}</td>
                </tr>
                """

            st.markdown(f"""
            <style>
            table {{
                width:100%;
                border-collapse:collapse;
                font-size:13px;
            }}
            th {{
                background:{brand_color if 'brand_color' in locals() else '#2B3F87'};
                color:white;
                padding:10px;
                text-align:left;
            }}
            td {{
                padding:10px;
                border-bottom:1px solid #eee;
            }}
            tr:hover {{
                background:#f9fafb;
            }}
            </style>

            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Employee</th>
                        <th>Gross</th>
                        <th>PAYE</th>
                        <th>NSSF</th>
                        <th>Net</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
            """, unsafe_allow_html=True)
        else:
            st.info("No payroll records yet.")

    with tab_logs:
        if not df.empty:
            def fm(x):
                try:
                    return f"{int(float(x or 0)):,}"
                except:
                    return "0"

            header_html = """
            <style>
                .pay-table { width: 100%; border-collapse: collapse; font-family: sans-serif; font-size: 13px; }
                .pay-table th { background: #f8f9fa; border: 1px solid #ddd; padding: 10px; }
                .pay-table td { border: 1px solid #ddd; padding: 10px; }
            </style>
            <table class='pay-table'>
                <thead>
                    <tr>
                        <th>#</th><th>Employee</th><th>Arrears</th><th>Basic</th><th>Gross</th>
                        <th>PAYE</th><th>NSSF 5%</th><th>Net Pay</th><th>NSSF 10%</th><th>NSSF 15%</th>
                    </tr>
                </thead>
                <tbody>
            """

            rows_html = ""
            for i, r in df.iterrows():
                rows_html += f"""
                <tr>
                    <td style='text-align:center;'>{i+1}</td>
                    <td><b>{r.get('employee','')}</b><br><small>{r.get('designation','-')}</small></td>
                    <td style='text-align:right;'>{fm(r.get('arrears',0))}</td>
                    <td style='text-align:right;'>{fm(r.get('basic_salary',0))}</td>
                    <td style='text-align:right;font-weight:bold;'>{fm(r.get('gross_salary',0))}</td>
                    <td style='text-align:right;'>{fm(r.get('paye',0))}</td>
                    <td style='text-align:right;'>{fm(r.get('nssf_5',0))}</td>
                    <td style='text-align:right;background:#E3F2FD;font-weight:bold;'>{fm(r.get('net_pay',0))}</td>
                    <td style='text-align:right;background:#FFF9C4;'>{fm(r.get('nssf_10',0))}</td>
                    <td style='text-align:right;background:#FFF9C4;font-weight:bold;'>{fm(r.get('nssf_15',0))}</td>
                </tr>
                """

            total_net = df["net_pay"].sum()

            footer_html = f"""
                </tbody>
                <tfoot>
                    <tr style="background:#2B3F87;color:white;font-weight:bold;">
                        <td colspan="7" style="text-align:center;padding:12px;">GRAND TOTALS</td>
                        <td style="text-align:right;padding:12px;">{fm(total_net)}</td>
                        <td colspan="2"></td>
                    </tr>
                </tfoot>
            </table>
            """

            full_html = header_html + rows_html + footer_html

            if st.button("📥 Print PDF", key="print_pay_btn"):
                st.components.v1.html("<script>window.print();</script>", height=0)

            st.components.v1.html(full_html, height=600, scrolling=True)
            st.write("---")
            with st.expander("⚙️ Manage Record"):
                if not df.empty:
                    sel_opt = st.selectbox(
                        "Select Record",
                        [f"{r.get('employee','')} (ID: {r.get('id','')})" for _, r in df.iterrows()]
                    )

                    if st.button("🗑️ Delete Record"):
                        try:
                            sid = sel_opt.split("(ID: ")[1].replace(")", "")
                            supabase.table("payroll").delete().eq("id", sid).execute()
                            st.warning("Deleted.")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Delete failed: {e}")
# ==========================================
# 🚀 BALLISTIC FINTECH REPORTS ENGINE (FINAL)
# ==========================================
import plotly.express as px
import pandas as pd
import streamlit as st

def show_reports():

    # ==============================
    # HEADER (LIGHT)
    # ==============================
    st.markdown("""
    <h2 style='
        background: linear-gradient(90deg,#EFF6FF,#DBEAFE);
        padding:14px 18px;
        border-radius:12px;
        color:#1E3A8A;
        border:1px solid #E5E7EB;
    '>📊 Financial Intelligence Dashboard</h2>
    """, unsafe_allow_html=True)

    tenant = st.session_state.get("tenant_id", "default")

    # ==============================
    # SAFE FETCH
    # ==============================
    def safe(df):
        if df is None or df.empty:
            return pd.DataFrame()
        if "tenant_id" in df.columns:
            return df[df["tenant_id"].astype(str) == str(tenant)]
        return df

    loans = safe(get_cached_data("loans"))
    payments = safe(get_cached_data("payments"))
    expenses = safe(get_cached_data("expenses"))
    payroll = safe(get_cached_data("payroll"))
    petty = safe(get_cached_data("petty_cash"))
    borrowers = safe(get_cached_data("borrowers"))

    if loans.empty:
        st.info("No financial data yet.")
        return

    # ==============================
    # SAFE NUMERIC ENGINE (MERGED)
    # ==============================
    def col_sum(df, col):
        if df is None or df.empty or col not in df.columns:
            return 0.0
        return pd.to_numeric(pd.Series(df[col]), errors="coerce").fillna(0).sum()

    def safe_series(df, col):
        if df is None or df.empty or col not in df.columns:
            return pd.Series(dtype=float)
        return pd.to_numeric(df[col], errors="coerce").fillna(0)

    # normalize loan numeric
    for c in ["principal", "interest", "total_repayable", "balance"]:
        if c in loans.columns:
            loans[c] = pd.to_numeric(loans[c], errors="coerce").fillna(0)

    # borrower mapping
    if not borrowers.empty and "borrower_id" in loans.columns:
        m = dict(zip(borrowers["id"].astype(str), borrowers["name"]))
        loans["borrower"] = loans["borrower_id"].astype(str).map(m).fillna("Unknown")
    else:
        loans["borrower"] = "Unknown"

    # ==============================
    # CORE METRICS
    # ==============================
    capital = col_sum(loans, "principal")
    interest = col_sum(loans, "interest")
    collected = col_sum(payments, "amount")
    expenses_total = col_sum(expenses, "amount")

    nssf = col_sum(payroll, "nssf_5") + col_sum(payroll, "nssf_10")
    paye = col_sum(payroll, "paye")

    petty_out = 0
    if not petty.empty:
        petty_out = col_sum(petty[petty["type"] == "Out"], "amount")

    operating_costs = expenses_total + petty_out + nssf + paye
    profit = collected - operating_costs

    # ==============================
    # KPI CARDS
    # ==============================
    def kpi(title, value, color):
        return f"""
        <div style="
            padding:16px;
            border-radius:12px;
            background:#FFFFFF;
            border:1px solid #E5E7EB;
        ">
            <p style="font-size:11px;color:#6B7280;margin:0;">{title}</p>
            <h2 style="margin:0;color:{color}">UGX {value:,.0f}</h2>
        </div>
        """

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(kpi("CAPITAL", capital, "#2563EB"), True)
    c2.markdown(kpi("INTEREST", interest, "#16A34A"), True)
    c3.markdown(kpi("COLLECTIONS", collected, "#7C3AED"), True)
    c4.markdown(kpi("PROFIT", profit, "#16A34A" if profit>=0 else "#DC2626"), True)

    # ==========================================
    # 🧠 INVESTOR INTELLIGENCE LAYER (MERGED)
    # ==========================================
    st.markdown("---")
    st.markdown("## 🧠 Investor Intelligence")

    # normalize dates
    payments["date"] = pd.to_datetime(payments.get("date"), errors="coerce")
    expenses["date"] = pd.to_datetime(expenses.get("date"), errors="coerce")

    # ==============================
    # MONTHLY P/L TREND
    # ==============================
    st.markdown("### 📈 Monthly Profit & Loss")

    inc = payments.copy()
    inc["amount"] = safe_series(payments, "amount")
    inc["month"] = inc["date"].dt.to_period("M")

    exp = expenses.copy()
    exp["amount"] = safe_series(expenses, "amount")
    exp["month"] = exp["date"].dt.to_period("M")

    inc_m = inc.groupby("month")["amount"].sum()
    exp_m = exp.groupby("month")["amount"].sum()

    pl = pd.concat([inc_m, exp_m], axis=1).fillna(0)
    pl.columns = ["Income", "Expenses"]
    pl["Profit"] = pl["Income"] - pl["Expenses"]
    pl = pl.reset_index().astype({"month": str})

    fig = px.line(pl, x="month", y=["Income", "Expenses", "Profit"], markers=True)
    fig.update_layout(paper_bgcolor="white", plot_bgcolor="white")
    st.plotly_chart(fig, use_container_width=True)

    # ==============================
    # TRUE PROFIT (IMPORTANT)
    # ==============================
    interest_income = safe_series(loans, "interest").sum()
    true_profit = interest_income - operating_costs

    # ==============================
    # BALANCE SHEET (REAL)
    # ==============================
    loan_book = safe_series(loans, "balance").sum()
    cash_position = collected - operating_costs
    total_assets = loan_book + cash_position

    # ==============================
    # INVESTOR METRICS
    # ==============================
    portfolio_yield = (interest_income / capital * 100) if capital > 0 else 0
    expense_ratio = (operating_costs / collected * 100) if collected > 0 else 0

    loans["end_date"] = pd.to_datetime(loans.get("end_date"), errors="coerce")

    overdue = loans[
        (loans["status"].str.upper().str.contains("OVERDUE")) &
        (loans["end_date"] < pd.Timestamp.today())
    ]

    par_value = safe_series(overdue, "principal").sum()
    par_percent = (par_value / capital * 100) if capital > 0 else 0

    collection_eff = (collected / capital * 100) if capital > 0 else 0

    # ==============================
    # METRICS DASHBOARD
    # ==============================
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Yield %", f"{portfolio_yield:.1f}%")
    m2.metric("Expense Ratio", f"{expense_ratio:.1f}%")
    m3.metric("Collection Efficiency", f"{collection_eff:.1f}%")
    m4.metric("PAR %", f"{par_percent:.1f}%")

    # ==============================
    # INVESTOR STATEMENTS
    # ==============================
    s1, s2 = st.columns(2)

    with s1:
        st.markdown("### 💰 Investor P/L")
        st.markdown(f"""
        <div style="background:#FFFFFF;padding:18px;border-radius:12px;border:1px solid #E5E7EB">
        Revenue: UGX {interest_income:,.0f}<br>
        Costs: UGX {operating_costs:,.0f}<br><br>
        <b style="color:{'#16A34A' if true_profit>=0 else '#DC2626'}">
        TRUE PROFIT: UGX {true_profit:,.0f}
        </b>
        </div>
        """, True)

    with s2:
        st.markdown("### 🧾 Balance Sheet")
        st.markdown(f"""
        <div style="background:#FFFFFF;padding:18px;border-radius:12px;border:1px solid #E5E7EB">
        Cash: UGX {cash_position:,.0f}<br>
        Loan Book: UGX {loan_book:,.0f}<br>
        <b>Total: UGX {total_assets:,.0f}</b>
        </div>
        """, True)

    # ==============================
    # EXPORT
    # ==============================
    st.markdown("### 📤 Investor Export")

    export_df = pd.DataFrame({
        "Metric": [
            "Capital",
            "Interest",
            "Collections",
            "Costs",
            "True Profit",
            "Yield %",
            "PAR %",
            "Expense Ratio %",
            "Collection Efficiency %"
        ],
        "Value": [
            capital,
            interest_income,
            collected,
            operating_costs,
            true_profit,
            portfolio_yield,
            par_percent,
            expense_ratio,
            collection_eff
        ]
    })

    st.dataframe(export_df, use_container_width=True)

    st.download_button(
        "⬇️ Download Investor Report",
        export_df.to_csv(index=False),
        file_name="investor_report.csv"
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

        data = [["Date", "Description", "Debit", "Credit", "Balance"]]

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

        # Adjusted colWidths: widened the Description and Balance columns
        table = Table(data, repeatRows=1, colWidths=[75, 170, 85, 85, 100])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.darkblue),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
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
# MAIN LEDGER FUNCTION (MERGED)
# ==============================
def show_ledger():
    st.markdown("<h2 style='color: #2B3F87;'>📘 Master Ledger</h2>", unsafe_allow_html=True)

    loans_df = get_cached_data("loans")
    payments_df = get_cached_data("payments")

    if loans_df is None or loans_df.empty:
        st.info("💡 Your system is clear! No Active loans found.")
        return

    loans_df.columns = loans_df.columns.str.strip().str.lower().str.replace(" ", "_")

    if payments_df is not None and not payments_df.empty:
        payments_df.columns = payments_df.columns.str.strip().str.lower().str.replace(" ", "_")

    borrowers_df = get_cached_data("borrowers")
    bor_map = {}

    if borrowers_df is not None and not borrowers_df.empty:
        borrowers_df.columns = borrowers_df.columns.str.strip().str.lower().str.replace(" ", "_")
        bor_map = dict(zip(borrowers_df["id"].astype(str), borrowers_df["name"]))

    if "borrower" not in loans_df.columns:
        loans_df["borrower"] = loans_df["borrower_id"].astype(str).map(bor_map).fillna("Unknown")

    # ==============================
    # SELECTION
    # ==============================
    loan_map = {
        f"ID: {r.get('loan_id_label', r['id'])} - {r['borrower']}": str(r["id"])
        for _, r in loans_df.iterrows()
    }

    selected_label = st.selectbox("Select Loan", list(loan_map.keys()))
    raw_id = loan_map[selected_label]
    loan_info = loans_df[loans_df["id"].astype(str) == raw_id].iloc[0] if not df.empty else None if not df.empty else None

    # ==============================
    # LEDGER TABLE
    # ==============================
    current_p = float(loan_info.get("principal", 0))
    interest_amt = float(loan_info.get("interest", 0))

    ledger_data = []
    running = current_p + interest_amt

    ledger_data.append({
        "Date": str(loan_info.get("start_date", "-"))[:10],
        "Description": "Disbursement",
        "Debit": current_p,
        "Credit": 0,
        "Balance": running
    })

    if interest_amt > 0:
        ledger_data.append({
            "Date": str(loan_info.get("start_date", "-"))[:10],
            "Description": "Interest",
            "Debit": interest_amt,
            "Credit": 0,
            "Balance": running
        })

    if payments_df is not None and not payments_df.empty:
        rel = payments_df[payments_df["loan_id"].astype(str) == raw_id]

        for _, p in rel.iterrows():
            amt = float(p.get("amount", 0))
            running -= amt

            ledger_data.append({
                "Date": str(p.get("date", p.get("payment_date", "-")))[:10],
                "Description": "Repayment",
                "Debit": 0,
                "Credit": amt,
                "Balance": running
            })

    st.dataframe(pd.DataFrame(ledger_data), use_container_width=True)

    st.markdown("---")

    # ==============================
    # PDF DOWNLOAD
    # ==============================
    if st.button("📄 Download Premium Statement"):
        client_name = loan_info.get("borrower", "Unknown")
        client_loans = loans_df[loans_df["borrower"] == client_name]

        pdf = generate_pdf_statement(client_name, client_loans, payments_df)

        st.download_button(
            "⬇️ Download PDF",
            pdf,
            file_name=f"{client_name}_Statement.pdf",
            mime="application/pdf"
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
        st.markdown(f"**Current Business Name:** {Active_company.get('name', 'Unknown')}")

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

def get_Active_color():
    """Helper to get the current theme color for consistent UI styling."""
    return st.session_state.get('theme_color', '#1E3A8A')

def show_dashboard_view():
    """
    Main Dashboard view. 
    Upgraded: performance layer, safer finance engine, SaaS-safe computation.
    """
    # 0. INITIALIZE THEME (Prevents NameError for brand_color)
    brand_color = get_Active_color()
    
    st.markdown(f"<h2 style='color: {brand_color};'>📊 Financial Dashboard</h2>", unsafe_allow_html=True)

    # --- 1. LOAD DATA ---
    df = get_cached_data("loans")
    pay_df = get_cached_data("payments")
    exp_df = get_cached_data("expenses") 
    bor_df = get_cached_data("borrowers")

    if df is None or df.empty:
        st.info("👋 Welcome! Start by adding your first borrower or loan in the sidebar.")
        st.stop()

    # --- 2. SAFE UTILS ---
    def safe_numeric(df, col_list):
        for col in col_list:
            if df is not None and col in df.columns:
                return pd.to_numeric(df[col], errors="coerce").fillna(0)
        return pd.Series(0.0, index=df.index if df is not None else [])

    def safe_date(df, col_list):
        for col in col_list:
            if df is not None and col in df.columns:
                return pd.to_datetime(df[col], errors="coerce")
        return pd.Series(pd.NaT, index=df.index if df is not None else [])

    # --- 3. SAFE COLUMN STANDARDIZATION ---
    def normalize(d):
        if d is not None and not d.empty:
            d.columns = d.columns.str.strip().str.lower().str.replace(" ", "_")
        return d

    df = normalize(df)
    pay_df = normalize(pay_df)
    exp_df = normalize(exp_df)
    bor_df = normalize(bor_df)

    df_clean = df.copy()

    # --- 4. BORROWER MAPPING ---
    bor_map = {}
    if bor_df is not None and not bor_df.empty:
        b_id = next((c for c in bor_df.columns if 'id' in c), None)
        b_nm = next((c for c in bor_df.columns if 'name' in c or 'borrower' in c), None)
        if b_id and b_nm:
            bor_map = dict(zip(bor_df[b_id].astype(str), bor_df[b_nm].astype(str)))

    link_col = next((c for c in df_clean.columns if 'borrower_id' in c or 'borrower' in c), None)
    if link_col:
        df_clean["borrower_name"] = df_clean[link_col].astype(str).map(bor_map).fillna(df_clean[link_col])
    else:
        df_clean["borrower_name"] = "Unknown Borrower"

    # --- 5. FINANCIAL ENGINE ---
    df_clean["principal_clean"] = safe_numeric(df_clean, ["principal", "amount"])
    df_clean["interest_clean"] = safe_numeric(df_clean, ["interest", "interest_amount"])
    df_clean["paid_clean"] = (
        safe_numeric(df_clean, ["paid"]) + 
        safe_numeric(df_clean, ["repaid"]) + 
        safe_numeric(df_clean, ["amount_paid"])
    )

    stat_col = next((c for c in df_clean.columns if "status" in c), None)
    df_clean["status_clean"] = df_clean[stat_col].astype(str).str.title() if stat_col else "Active"
    df_clean["end_date_dt"] = safe_date(df_clean, ["end_date", "due_date", "date"])

    # --- 6. PRE-FILTER ENGINE ---
    today = pd.Timestamp.now().normalize()
    Active_statuses = ["Active", "Overdue", "Rolled/Overdue"]
    Active_df = df_clean[df_clean["status_clean"].isin(Active_statuses)].copy()
    overdue_df = Active_df[Active_df["end_date_dt"] < today]

    # --- 7. CORE METRICS ---
    total_issued = Active_df["principal_clean"].sum()
    total_interest_expected = Active_df["interest_clean"].sum()
    total_collected = df_clean["paid_clean"].sum()
    overdue_count = len(overdue_df)

    # --- 8. DISPLAY METRIC CARDS ---
    m1, m2, m3, m4 = st.columns(4)
    card_style = f"background:#fff;padding:20px;border-radius:15px;border-left:5px solid {brand_color};box-shadow:2px 2px 10px rgba(0,0,0,0.05);"

    m1.markdown(f'<div style="{card_style}"><b>💰 Active PRINCIPAL</b><h3>{total_issued:,.0f} UGX</h3></div>', unsafe_allow_html=True)
    m2.markdown(f'<div style="{card_style}"><b>📈 EXPECTED INTEREST</b><h3>{total_interest_expected:,.0f} UGX</h3></div>', unsafe_allow_html=True)
    m3.markdown(f'<div style="{card_style.replace("#fff","#F0FFF4")}"><b>✅ TOTAL COLLECTED</b><h3>{total_collected:,.0f} UGX</h3></div>', unsafe_allow_html=True)
    m4.markdown(f'<div style="{card_style.replace("#fff","#FFF5F5")}"><b>🚨 OVERDUE FILES</b><h3>{overdue_count}</h3></div>', unsafe_allow_html=True)

    st.write("---")

    # --- 9. RECENT LOANS TABLE (Fixed Indentation) ---
    t1, t2 = st.columns(2)

    with t1:
        st.markdown(f"<h4 style='color:{brand_color};'>📝 Recent Portfolio Activity</h4>", unsafe_allow_html=True)

        if not Active_df.empty:
            recent = Active_df.sort_values("end_date_dt", ascending=False).head(5)
            rows_html = ""
            for idx, (i, r) in enumerate(recent.iterrows()):
                bg = "#F8FAFC" if idx % 2 == 0 else "#FFFFFF"
                rows_html += f"""
                <tr style="background:{bg}; border-bottom: 1px solid #eee;">
                    <td style="padding:8px;">{r.get('borrower_name', 'Unknown')}</td>
                    <td style="padding:8px; text-align:right; color:{brand_color}; font-weight:bold;">{r['principal_clean']:,.0f}</td>
                    <td style="padding:8px; text-align:center;">{r.get('status_clean', 'Active')}</td>
                    <td style="padding:8px; text-align:center;">{r['end_date_dt'].strftime('%d %b') if pd.notna(r['end_date_dt']) else '-'}</td>
                </tr>"""

            full_table_html = f"""
            <table style="width:100%; font-size:13px; border-collapse:collapse; font-family:sans-serif;">
                <tr style="background:{brand_color}; color:white;"><th style="padding:10px; text-align:left;">Borrower</th><th style="padding:10px; text-align:right;">Principal</th><th style="padding:10px; text-align:center;">Status</th><th style="padding:10px; text-align:center;">Due</th></tr>
                {rows_html}
            </table>"""
            st.components.v1.html(full_table_html, height=250)
        else:
            st.info("No Active loans found.")

    # --- 10. PAYMENTS TABLE (Fixed Indentation) ---
    with t2:
        st.markdown("<h4 style='color:#2E7D32;'>💸 Recent Cash Inflows</h4>", unsafe_allow_html=True)
        if pay_df is not None and not pay_df.empty:
            pay_df["amount_clean"] = safe_numeric(pay_df, ["amount", "amount_paid"])
            recent_pay = pay_df.sort_values("date", ascending=False).head(5)
            pay_rows = ""
            for idx, (i, r) in enumerate(recent_pay.iterrows()):
                bg = "#F0F8FF" if idx % 2 == 0 else "#FFFFFF"
                pay_rows += f"""<tr style="background:{bg}; border-bottom:1px solid #eee;"><td style="padding:8px;">{r.get('borrower', 'Unknown')}</td><td style="padding:8px; text-align:right; color:#2E7D32; font-weight:bold;">{r['amount_clean']:,.0f}</td><td style="padding:8px; text-align:center;">{r.get('date', '-')}</td></tr>"""
            
            pay_table_html = f'<table style="width:100%; font-size:13px; border-collapse:collapse; font-family:sans-serif;"><tr style="background:#2E7D32; color:white;"><th style="padding:10px; text-align:left;">Borrower</th><th style="padding:10px; text-align:right;">Amount</th><th style="padding:10px; text-align:center;">Date</th></tr>{pay_rows}</table>'
            st.components.v1.html(pay_table_html, height=250)
        else:
            st.write("No recent payments found.")

    # --- 11. CHARTS (Fixed Indentation) ---
    st.write("---")
    c1, c2 = st.columns(2)

    with c1:
        if not df_clean.empty:
            status_counts = df_clean["status_clean"].value_counts().reset_index()
            status_counts.columns = ["Status", "Count"]
            fig = px.pie(status_counts, names="Status", values="Count", hole=0.5, color_discrete_sequence=["#4A90E2","#FF4B4B","#FFA500"])
            fig.update_layout(title="Loan Status Distribution", margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        if pay_df is not None and not pay_df.empty:
            pay_df["date_dt"] = pd.to_datetime(pay_df["date"], errors="coerce")
            inc = pay_df.groupby(pay_df["date_dt"].dt.strftime("%b %Y"))["amount_clean"].sum().reset_index()
            # ... Income/Expense Logic ...
            st.write("Monthly Cashflow View Active")
# ==========================================
# FINAL APP ROUTER (REACTIVE & STABLE)
# ==========================================

if __name__ == "__main__":

    # Ensure defaults
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    # ======================
    # 🔐 AUTH FLOW (ONLY ONCE)
    # ======================
    if not st.session_state["logged_in"]:
        st.session_state['theme_color'] = "#1E3A8A"
        apply_master_theme()

        # ✅ ONLY CALL THIS ONCE
        run_auth_ui(supabase)

    # ======================
    # 🚀 MAIN APP
    # ======================
    else:
        try:
            check_session_timeout()

            # Sidebar
            page = render_sidebar()
            # Theme
            apply_master_theme()

            # Views
            if page == "Settings":
                show_settings()
            elif page == "Overview":
                show_dashboard_view()
            elif page == "Loans":
                show_loans()
            elif page == "Borrowers":
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
                show_reports() # This will now call your developed module
            else:
                # This only shows if you add a new sidebar item without a function
                st.info(f"The {page} module is coming online soon.")
        except Exception as e:

            st.error(f"Application Error: {e}")
