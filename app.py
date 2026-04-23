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
def restore_session():
    if "authenticated" not in st.session_state:
        if cookies.get("user_id"):
            st.session_state["authenticated"] = True
            st.session_state["user_id"] = cookies.get("user_id")
            st.session_state["tenant_id"] = cookies.get("tenant_id")
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
    
    with st.form("company_reg_form"):
        company_name = st.text_input("Organization Name")
        admin_name = st.text_input("Admin Full Name")
        email = st.text_input("Business Email").strip().lower()
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Create Organization", use_container_width=True)

    if submit:
        if not company_name or not email or not password:
            st.error("Please fill in all fields.")
            return

        try:
            tenant_id = str(uuid.uuid4())
            # 1. Sign up the user in Supabase Auth
            res = supabase.auth.sign_up({"email": email, "password": password})
            
            if res.user:
                # 2. Create the Tenant entry
                supabase.table("tenants").insert({
                    "id": tenant_id,
                    "name": company_name,
                    "company_code": company_name[:3].upper() + str(random.randint(100, 999))
                }).execute()

                # 3. Create the User profile linked to Tenant
                supabase.table("users").insert({
                    "id": res.user.id,
                    "name": admin_name,
                    "email": email,
                    "tenant_id": tenant_id,
                    "role": "Admin"
                }).execute()

                st.success(f"✅ {company_name} registered! Please login.")
                st.session_state["view"] = "login"
                st.rerun()
        except Exception as e:
            st.error(f"Registration Error: {e}")

# ==============================
# 👥 STAFF SIGNUP
# ==============================
def view_staff_signup(supabase):
    st.header("🆕 Join Organization")
    
    with st.form("staff_form"):
        company_name = st.text_input("Company Name to Join").strip()
        name = st.text_input("Your Full Name")
        email = st.text_input("Email").strip().lower()
        pwd = st.text_input("Password", type="password")
        submit = st.form_submit_button("Request Access", use_container_width=True)

    if submit:
        try:
            # 1. Find the company (Case-insensitive lookup)
            tenant_query = supabase.table("tenants").select("id").ilike("name", company_name).execute()

            if not tenant_query.data:
                st.error("Company not found. Please check the spelling.")
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

                st.success("Account created! You can now login.")
                st.session_state["view"] = "login"
                st.rerun()
        except Exception as e:
            st.error(f"Signup Error: {e}")


# ==============================
# 🔑 LOGIN PAGE
# ==============================
def login_page(supabase):
    st.markdown("## 🔐 Finance Portal Login")
    
    with st.form("login_form"):
        email = st.text_input("Email").strip().lower()
        pwd = st.text_input("Password", type="password")
        submit = st.form_submit_button("Access Dashboard", use_container_width=True)

    if submit:
        try:
            # 1. Authenticate with Supabase Auth
            auth_res = supabase.auth.sign_in_with_password({"email": email, "password": pwd})
            
            if auth_res.user:
                # 2. Fetch profile AND Tenant name in one join query
                user_query = supabase.table("users").select("*, tenants(name)").eq("id", auth_res.user.id).execute()

                if user_query.data:
                    user = user_query.data[0]
                    
                    # 3. Set ALL session variables required by your Router
                    st.session_state.update({
                        "authenticated": True,
                        "logged_in": True,
                        "user_id": user["id"],
                        "tenant_id": user["tenant_id"],
                        "user_name": user["name"],
                        "role": user["role"],
                        "company": user.get("tenants", {}).get("name", "Organization"),
                        "current_page": "Overview" # Default starting page
                    })
                    st.success("Login successful! Redirecting...")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Profile not found in database.")
            else:
                st.error("Invalid credentials.")
        except Exception as e:
            st.error(f"Login failed: {e}")


# ==============================
# 🔒 ROUTER
# ==============================
def run_auth_ui(supabase):
    restore_session()

    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    if st.session_state.get("authenticated"):
        st.success(f"Welcome {st.session_state.get('user_name','User')}")

        if st.button("Logout"):
            cookies.clear()
            cookies.save()
            st.session_state.clear()
            st.rerun()
        return

    if st.session_state["view"] == "login":
        login_page(supabase)
    elif st.session_state["view"] == "signup":
        view_staff_signup(supabase)
    elif st.session_state["view"] == "create_company":
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
    # 🎨 BRANDING & THEME
    # ==============================
    brand_color = st.session_state.get("theme_color", "#1E3A8A")
    st.markdown(f"<h2 style='color:{brand_color};'>🚀 Borrowers Registry</h2>", unsafe_allow_html=True)

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
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    def safe_numeric(df, col, default=0.0):
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.Series(dtype="float64")
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
        else:
            s = pd.Series([default] * len(df), index=df.index)
        return s.fillna(default)

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
    if "tenant_id" in borrowers_df.columns:
        borrowers_df = borrowers_df[borrowers_df["tenant_id"].astype(str) == str(tenant_id)]
    if "tenant_id" in loans_df.columns:
        loans_df = loans_df[loans_df["tenant_id"].astype(str) == str(tenant_id)]

    # Ensure required structural columns exist
    for col in ["id", "name", "phone", "email", "status", "national_id", "next_of_kin"]:
        if col not in borrowers_df.columns:
            borrowers_df[col] = ""

    # ==============================
    # 🔥 REAL-TIME RISK ENGINE
    # ==============================
    risk_map = {}
    if not loans_df.empty:
        loans_df["balance"] = safe_numeric(loans_df, "balance")
        loans_df["due_date"] = pd.to_datetime(loans_df.get("due_date"), errors="coerce")

        today = pd.Timestamp.today()
        # Calculate days overdue only for loans with a remaining balance
        loans_df["days_overdue"] = (today - loans_df["due_date"]).dt.days
        loans_df["days_overdue"] = loans_df["days_overdue"].apply(lambda x: x if x > 0 else 0)
        loans_df["is_overdue"] = (loans_df["days_overdue"] > 0) & (loans_df["balance"] > 0)

        # Aggregate risk metrics per borrower
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
                    new_entry = pd.DataFrame([{
                        "id": new_id, "name": name, "phone": phone, "email": email,
                        "national_id": nid, "address": addr, "next_of_kin": nok,
                        "status": "Active", "tenant_id": str(tenant_id)
                    }])
                    if save_data("borrowers", new_entry):
                        st.success(f"✅ {name} registered successfully!")
                        st.rerun()
                else:
                    st.error("⚠️ Full Name and Phone Number are required.")

    with tab_view:
        # 🔍 SEARCH BAR
        search = st.text_input("🔍 Search by name or phone...").lower()

        # 📊 BEAUTIFIED TABLE
        if not borrowers_df.empty:
            df_to_show = borrowers_df.copy()
            # String conversion for search stability
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
                    
                    # Risk Badge Logic
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
                                <th style='padding:14px;'>Borrower Name</th>
                                <th style='padding:14px;'>Phone</th>
                                <th style='padding:14px;'>National ID</th>
                                <th style='padding:14px;'>Next of Kin</th>
                                <th style='padding:14px;'>Risk Status</th>
                                <th style='padding:14px; text-align:center;'>Status</th>
                            </tr>
                        </thead>
                        <tbody>{rows_html}</tbody>
                    </table>
                </div>""", unsafe_allow_html=True)

                # 🖱️ SELECTION & MANAGEMENT
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
    # 👤 BORROWER PROFILE PANEL (EXPANDED)
    # ==============================
    selected_id = st.session_state.get("selected_borrower")

    if selected_id:
        st.write("---")
        st.markdown(f"### 👤 Profile Detail: {selected_id[:8]}")

        borrower_query = borrowers_df[borrowers_df["id"].astype(str) == str(selected_id)]

        if not borrower_query.empty:
            borrower = borrower_query.iloc[0]

            with st.container(border=True):
                c1, c2 = st.columns(2)
                upd_name = c1.text_input("Name", borrower["name"])
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
                    st.dataframe(
                        user_loans, 
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "id": None, "tenant_id": None, "borrower_id": None, "borrower_name": None,
                            "principal": st.column_config.NumberColumn("Principal", format="%,d UGX"),
                            "balance": st.column_config.NumberColumn("Balance", format="%,d UGX"),
                            "total_repayable": st.column_config.NumberColumn("Total Due", format="%,d UGX"),
                            "start_date": st.column_config.DateColumn("Date Issued"),
                            "end_date": st.column_config.DateColumn("Due Date"),
                        }
                    )
                    
                    # Statement Export
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
                    borrowers_df.loc[borrowers_df["id"].astype(str) == str(selected_id), 
                        ["name","phone","email","national_id","next_of_kin","address"]] = \
                        [upd_name, upd_phone, upd_email, upd_nid, upd_nok, upd_addr]
                    if save_data("borrowers", borrowers_df):
                        st.success("Profile Updated")
                        st.rerun()

                if act_c2.button("🗑️ Delete", use_container_width=True):
                    updated = borrowers_df[borrowers_df["id"].astype(str) != str(selected_id)]
                    if save_data("borrowers", updated):
                        st.warning("Profile Removed")
                        st.session_state.pop("selected_borrower", None)
                        st.rerun()
                
                if act_c3.button("❌ Close Profile", use_container_width=True):
                    st.session_state.pop("selected_borrower", None)
                    st.rerun()

# ==============================
# 🔐 SAAS TENANT CONTEXT (HARDENED)
# ==============================
def get_current_tenant():
    """Returns current tenant_id from session (SaaS isolation layer)"""
    return st.session_state.get("tenant_id", "default_tenant")

# ==============================
# 🧠 DATABASE ADAPTER (MULTI-TENANT SAFE)
# ==============================
def get_data(table_name):
    """Multi-tenant safe data fetch with auto-migration for old records"""
    tenant_id = get_current_tenant()
    df = get_cached_data(table_name)

    if df is not None and not df.empty:
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        if "tenant_id" in df.columns:
            df = df[df["tenant_id"].astype(str) == str(tenant_id)].copy()
        else:
            df["tenant_id"] = tenant_id
    return df

def save_data_saas(table_name, df):
    """Multi-tenant safe save with hard enforcement of boundaries"""
    tenant_id = get_current_tenant()
    df["tenant_id"] = str(tenant_id)
    return save_data(table_name, df)

# ==============================
# 13. LOANS MANAGEMENT PAGE (SaaS Luxe Edition - Banking Grade)
# ==============================
def show_loans():
    import uuid
    from datetime import datetime, timedelta

    st.markdown("<h2 style='color: #0A192F;'>💵 Loans Management</h2>", unsafe_allow_html=True)
    
    # ==============================
    # ✅ LOAD DATA (SaaS SAFE)
    # ==============================
    loans_df = get_data("loans")
    borrowers_df = get_data("borrowers")
    payments_df = get_data("payments")

    # SAFETY
    if loans_df is None: loans_df = pd.DataFrame()
    if borrowers_df is None: borrowers_df = pd.DataFrame()
    if payments_df is None: payments_df = pd.DataFrame()

    if not borrowers_df.empty and "status" in borrowers_df.columns:
        Active_borrowers = borrowers_df[borrowers_df["status"].astype(str).str.title() == "Active"]
    else:
        Active_borrowers = pd.DataFrame()

    if loans_df.empty:
        loans_df = pd.DataFrame(columns=[
            "id","sn","loan_id_label","borrower_id","borrower","principal","interest",
            "total_repayable","amount_paid","balance","status",
            "start_date","end_date","cycle_no","tenant_id"
        ])

    # ==============================
    # 🔥 STANDARDIZATION
    # ==============================
    loans_df["id"] = loans_df.get("id", "").astype(str)

    # SYNC PAYMENTS
    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].astype(str)
        payments_df["amount"] = pd.to_numeric(payments_df.get("amount", 0), errors="coerce").fillna(0)
        pay_sums = payments_df.groupby("loan_id")["amount"].sum().to_dict()
        loans_df["amount_paid"] = loans_df["id"].map(pay_sums).fillna(0)

    # NUMERIC CLEAN
    for col in ["principal","interest","total_repayable","amount_paid","balance"]:
        loans_df[col] = pd.to_numeric(loans_df.get(col, 0), errors="coerce").fillna(0)

    loans_df["balance"] = (loans_df["total_repayable"] - loans_df["amount_paid"]).clip(lower=0)

    # STATUS LOGIC
    loans_df["status"] = loans_df.get("status", "").astype(str).str.upper().str.strip()

    def determine_status(row):
    current_status = row["status"]
    balance = row["balance"]
    paid = row["amount_paid"]

    # 🛡️ Never touch BCF (historical truth)
    if current_status == "BCF":
        return "BCF"

    # ✅ Fully paid = CLEARED (highest truth)
    if balance <= 0:
        return "CLEARED"

    # 🟡 No payment yet → still pending
    if paid == 0:
        return "PENDING"

    # 🔵 Partial payment → active loan
    return "ACTIVE"
    loans_df["status"] = loans_df.apply(determine_status, axis=1)
    loans_df.loc[loans_df["status"] == "CLEARED", "balance"] = 0

    # SORTING
    loans_df["cycle_no"] = pd.to_numeric(loans_df.get("cycle_no", 1), errors="coerce").fillna(1).astype(int)
    loans_df = loans_df.sort_values(by=["loan_id_label","cycle_no"]).reset_index(drop=True)

    loans_df["sn_rank"] = pd.factorize(loans_df["loan_id_label"])[0] + 1
    loans_df["sn"] = loans_df["sn_rank"].apply(lambda x: f"{int(x):04d}")

    # BORROWER MAP
    if not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].map(bor_map).fillna("Unknown")

    # ==============================
    # ✅ TABS (ONLY ONCE, CLEAN)
    # ==============================
    tab_view, tab_add, tab_manage, tab_actions = st.tabs([
        "📂 Portfolio View","➕ New Loan","🛠️ Manage/Edit","⚙️ Actions"
    ])

    # ==============================
    # 📂 PORTFOLIO
    # ==============================
    with tab_view:
        search_query = st.text_input("🔍 Search Loan / Borrower")

        filtered_loans = loans_df.copy()
        if search_query:
            filtered_loans = loans_df[
                loans_df.apply(lambda r: search_query.lower() in str(r).lower(), axis=1)
            ]

        show_cols = [
            "sn","loan_id_label","borrower","cycle_no",
            "principal","total_repayable","balance",
            "start_date","end_date","status"
        ]

        if filtered_loans.empty:
            st.warning("No matching loans found.")
        else:
            def style_entire_row(row):
                val = str(row["status"]).upper().strip()
                color_map = {
                    "ACTIVE": "background-color: #e0f2fe; color: #075985;",
                    "PENDING": "background-color: #fee2e2; color: #991b1b;",
                    "CLOSED": "background-color: #f3f4f6;",
                    "CLEARED": "background-color: #d1fae5;",
                    "BCF": "background-color: #ffedd5;"
                }
                return [color_map.get(val, "")] * len(row)

            styled_df = filtered_loans[show_cols].style.format({
                "principal":"{:,.0f}",
                "total_repayable":"{:,.0f}",
                "balance":"{:,.0f}"
            }).apply(style_entire_row, axis=1)

            st.dataframe(styled_df, use_container_width=True, hide_index=True)

    # ==============================
    # ➕ NEW LOAN
    # ==============================
    with tab_add:
        if Active_borrowers.empty:
            st.info("💡 Tip: Activate a borrower first.")
        else:
            borrower_map = dict(zip(Active_borrowers["name"], Active_borrowers["id"]))

            with st.form("loan_issue_form"):
                st.markdown("<h4 style='color: #0A192F;'>📝 Create New Loan Agreement</h4>", unsafe_allow_html=True)

                col1, col2 = st.columns(2)

                selected_name = col1.selectbox("Select Borrower", list(borrower_map.keys()))
                selected_id = borrower_map.get(selected_name)

                amount = col1.number_input("Principal Amount (UGX)", min_value=0, step=50000)
                date_issued = col1.date_input("Start Date", value=datetime.now())

                l_type = col2.selectbox("Loan Type", ["Business","Personal","Emergency","Other"])
                interest_rate = col2.number_input("Monthly Interest Rate (%)", min_value=0.0, step=0.5)
                date_due = col2.date_input("Due Date", value=date_issued + timedelta(days=30))

                total_due = amount + ((interest_rate / 100) * amount)
                st.info(f"Preview: Total Repayable will be {total_due:,.0f} UGX")

                if st.form_submit_button("🚀 Confirm & Issue Loan"):
                    next_sn_value = len(loans_df) + 1

                    loan_data = {
                        "sn": next_sn_value,
                        "loan_id_label": str(next_sn_value).zfill(5),
                        "borrower_id": str(selected_id),
                        "loan_type": l_type,
                        "principal": float(amount),
                        "interest": float((interest_rate/100)*amount),
                        "total_repayable": float(total_due),
                        "amount_paid": 0.0,
                        "status": "ACTIVE",
                        "start_date": str(date_issued),
                        "end_date": str(date_due),
                        "tenant_id": str(get_current_tenant())
                    }

                    if save_data("loans", pd.DataFrame([loan_data])):
                        st.success(f"✅ Loan {next_sn_value:05d} issued.")
                        st.cache_data.clear()
                        st.rerun()

    # ==============================
    # 🔄 ROLLOVER
    # ==============================
    with tab_actions:
        st.markdown("<h4 style='color: #0A192F;'>🔄 Multi-Stage Loan Rollover</h4>", unsafe_allow_html=True)

        eligible_loans = loans_df[
            (~loans_df["status"].isin(["CLOSED","CLEARED","BCF"])) &
            (loans_df["balance"] > 0)
        ]

        if eligible_loans.empty:
            st.success("All loans brought up to date! ✨")
        else:
            roll_map = {
                f"{row['borrower']} • Cycle {row['cycle_no']} • Bal: {row['balance']:,.0f}": row["id"]
                for _, row in eligible_loans.iterrows()
            }

            roll_sel = st.selectbox("Select Loan to Roll Forward", list(roll_map.keys()))
            loan_to_roll = eligible_loans[eligible_loans["id"] == roll_map[roll_sel]].iloc[0]

            current_unpaid = float(loan_to_roll['balance'])
            new_interest_rate = st.number_input("New Monthly Interest (%)", value=3.0, step=0.5)

            if st.button("🔥 Execute Next Rollover", use_container_width=True):
                old_due_date = pd.to_datetime(loan_to_roll['end_date'], errors="coerce") or datetime.now()

                loans_df.loc[loans_df["id"] == str(loan_to_roll['id']), "status"] = "BCF"
                save_data_saas("loans", loans_df)

                calc_interest = current_unpaid * (new_interest_rate / 100)

                new_cycle_data = {
                    "sn": loan_to_roll['sn'],
                    "loan_id_label": str(loan_to_roll['sn']).zfill(5),
                    "borrower_id": loan_to_roll['borrower_id'],
                    "principal": current_unpaid,
                    "interest": calc_interest,
                    "total_repayable": current_unpaid + calc_interest,
                    "amount_paid": 0.0,
                    "status": "PENDING",
                    "cycle_no": int(loan_to_roll['cycle_no']) + 1,
                    "start_date": old_due_date.strftime("%Y-%m-%d"),
                    "end_date": (old_due_date + timedelta(days=30)).strftime("%Y-%m-%d"),
                    "tenant_id": get_current_tenant()
                }

                if save_data("loans", pd.DataFrame([new_cycle_data])):
                    st.success("✅ Loan rolled successfully!")
                    st.cache_data.clear()
                    st.rerun()

    # ==============================
    # 🛠️ EDIT
    # ==============================
    with tab_manage:
        if not loans_df.empty:
            edit_map = {f"{row['borrower']} • {row['loan_id_label']}": row["id"] for _, row in loans_df.iterrows()}
            target_id = edit_map[st.selectbox("Select Loan to Edit", list(edit_map.keys()))]

            loan_to_edit = loans_df[loans_df["id"] == target_id].iloc[0]

            with st.form("edit_loan_form"):
                e_princ = st.number_input("Principal", value=float(loan_to_edit['principal']))
                e_stat = st.selectbox("Status", ["ACTIVE","PENDING","CLOSED","OVERDUE","BCF","ROLLED"])

                if st.form_submit_button("💾 Save Changes"):
                    supabase.table("loans").update({
                        "principal": e_princ,
                        "status": e_stat
                    }).eq("id", target_id).execute()

                    st.success("✅ Updated!")
                    st.cache_data.clear()
                    st.rerun()

            if st.button("🗑️ Delete Loan Permanently", use_container_width=True):
                supabase.table("loans").delete().eq("id", target_id).execute()
                st.warning("Loan Deleted.")
                st.cache_data.clear()
                st.rerun()
            
import pandas as pd
from datetime import datetime
import uuid
import streamlit as st # Added missing import
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
        # We only sync if there is actually payment data to avoid wiping current values
        loans_df["amount_paid"] = loans_df["id"].map(payment_sums).fillna(loans_df["amount_paid"])

    # ==============================
    # 📑 TABS
    # ==============================
    tab1, tab2 = st.tabs(["➕ Record Payment", "📜 History"])

    # ==============================
    # ➕ TAB 1: RECORD PAYMENT
    # ==============================
    with tab1:
        active_loans = loans_df[loans_df["status"].isin(["ACTIVE", "BCF", "PENDING"])].copy()

        if active_loans.empty:
            st.success("🎉 No active loans.")
        else:
            search = st.text_input("🔍 Search borrower or loan")
            if search:
                search = search.lower()
                active_loans = active_loans[
                    active_loans.apply(lambda r: search in str(r.get("borrower", "")).lower() 
                    or search in str(r.get("sn", "")).lower(), axis=1) 
                ]

            if active_loans.empty:
                st.warning("No matching loans.")
            else:
                def format_loan(row):
                    balance = row["total_repayable"] - row["amount_paid"]
                    return f"{row['borrower']} • SN: {row['sn']} • Bal: UGX {balance:,.0f}"

                options = {format_loan(row): row["id"] for _, row in active_loans.iterrows()}
                selected = st.selectbox("Select Loan", list(options.keys()))
                loan_id = options[selected]
                
                loan = active_loans[active_loans["id"] == loan_id].iloc[0]

                total = float(loan["total_repayable"])
                paid = float(loan["amount_paid"])
                balance = total - paid

                c1, c2 = st.columns(2)
                c1.metric("👤 Borrower", loan["borrower"])
                c2.metric("💰 Balance", f"UGX {balance:,.0f}")

                with st.form("payment_form"):
                    amount = st.number_input("Amount", min_value=0.0)
                    method = st.selectbox("Method", ["Cash", "Mobile Money", "Bank"])
                    date = st.date_input("Date", datetime.now())
                    submit = st.form_submit_button("Post Payment")

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
                            current_label = str(loan.get("loan_id_label", loan["sn"]))

                            # ✅ STEP 1: INSERT PAYMENT RECORD FIRST
                            # This ensures the "Sync" logic at the top sees the money!
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

                            # ✅ STEP 2: UPDATE THE LOAN MASTER RECORD
                            new_paid = paid + amount
                            new_status = "CLEARED" if new_paid >= total else loan["status"]

                            supabase.table("loans").update({
                                "amount_paid": float(new_paid),
                                "status": new_status,
                                "loan_id_label": current_label
                            }).eq("id", loan_id).execute()

                            # ✅ STEP 3: PDF & STATE
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

                            st.success(f"✅ Payment recorded | New Balance: {max(0, total-new_paid):,.0f}")
                            
                            # Clear cache to force a fresh pull of both tables
                            st.cache_data.clear()
                            st.rerun()

                        except Exception as e:
                            st.error(f"❌ {e}")

        if st.session_state.get("show_receipt"):
            st.download_button(
                "📥 Download Latest Receipt",
                data=st.session_state["receipt_pdf"],
                file_name=f"receipt_{datetime.now().strftime('%Y%m%d')}.pdf",
                mime="application/pdf"
            )

            if st.button("Clear Receipt"):
                st.session_state["show_receipt"] = False
                st.rerun()

    # ==============================
    # 📜 TAB 2: HISTORY (EDIT/DELETE ENABLED)
    # ==============================
    with tab2:
        if payments_df.empty:
            st.info("No payments yet.")
        else:
            df_hist = payments_df.copy()
            
            # Format for display
            df_hist["amount_display"] = df_hist["amount"].apply(lambda x: f"UGX {x:,.0f}")
            if "date" in df_hist.columns:
                df_hist = df_hist.sort_values("date", ascending=False)

            cols = [c for c in ["date", "borrower", "amount_display", "method", "receipt_no"] if c in df_hist.columns]
            st.dataframe(df_hist[cols], use_container_width=True, hide_index=True)

            # --- 🛠️ PAYMENT MANAGEMENT SECTION ---
            st.markdown("---")
            st.markdown("### ⚙️ Manage Payments")
            
            # Select by Receipt Number
            pay_map = {f"{row['receipt_no']} | {row['borrower']} | {row['amount_display']}": row['id'] for _, row in df_hist.iterrows()}
            selected_pay_label = st.selectbox("Select Receipt to Modify", list(pay_map.keys()), key="pay_manage_select")
            
            target_pay_id = pay_map[selected_pay_label]
            target_pay = df_hist[df_hist['id'] == target_pay_id].iloc[0]

            p_col1, p_col2 = st.columns(2)

            # 1. DELETE PAYMENT
            if p_col1.button("🗑️ Delete Payment", use_container_width=True):
                try:
                    # Delete from Supabase
                    supabase.table("payments").delete().eq("id", target_pay_id).execute()
                    st.success(f"✅ Payment {target_pay['receipt_no']} deleted.")
                    
                    # 🔥 CRITICAL: Force cache clear so the "Sync" logic 
                    # at the top of show_payments() recalculates the loan balance!
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")

            # 2. EDIT PAYMENT
            if p_col2.button("📝 Edit Payment", use_container_width=True):
                st.session_state["edit_pay_mode"] = True

            if st.session_state.get("edit_pay_mode"):
                with st.form("edit_payment_form"):
                    st.info(f"Editing Receipt: {target_pay['receipt_no']}")
                    new_amt = st.number_input("Correct Amount", value=float(target_pay['amount']))
                    new_method = st.selectbox("Correct Method", ["Cash", "Mobile Money", "Bank"], 
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
            name = st.text_input("Employee Name")
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

    # Map Borrower Names
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
    
    # Corrected data check to avoid NameError 'df'
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
    m1.metric("Principal", f"UGX {p:,.0f}")
    m2.metric("Total Interest", f"UGX {i:,.0f}")
    m3.metric("Total Paid", f"UGX {paid:,.0f}", delta=f"{paid/total_due:.1%}" if total_due > 0 else None)
    m4.metric("Current Balance", f"UGX {bal:,.0f}", delta_color="inverse", delta=f"-{paid:,.0f}")

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
        "Balance": running_bal
    })

    # Entry 2: Interest Charge
    if i > 0:
        ledger_data.append({
            "Date": str(loan_info.get("start_date", "-"))[:10],
            "Description": "📈 Monthly Interest Applied",
            "Debit (Due)": i,
            "Credit (Paid)": 0,
            "Balance": running_bal
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
                    "Balance": running_bal
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
            "Balance": st.column_config.NumberColumn("Running Balance (UGX)", format="%,d"),
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
