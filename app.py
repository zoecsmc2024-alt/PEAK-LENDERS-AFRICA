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
    page_title="Peak-Lenders Africa",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)
# Constants
SESSION_TIMEOUT = 30

# ============================================================
# 🔌 1. SUPABASE INIT (ROBUST & GLOBAL)
# ============================================================
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

if supabase is None:
    st.warning("⚠️ Supabase credentials not configured. Please check your secrets.")

# ============================================================
# 🏢 2. CONFIGURATION & SESSION STATE INITIALIZATION
# ============================================================
SESSION_TIMEOUT = 15  # Minutes
MAX_ATTEMPTS = 5
LOCKOUT_MINUTES = 10

if "tenant_id" not in st.session_state:
    st.session_state["tenant_id"] = None
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
if "theme_color" not in st.session_state:
    st.session_state["theme_color"] = "#1E3A8A" 
if "data_version" not in st.session_state:
    st.session_state["data_version"] = 0
if "view" not in st.session_state:
    st.session_state["view"] = "login"

# 🔒 Hydrate active connection if session cookies exist
if "auth_session" in st.session_state and supabase:
    try:
        supabase.auth.set_session(
            st.session_state["auth_session"].access_token,
            st.session_state["auth_session"].refresh_token
        )
    except Exception:
        pass

# ============================================================
# ⚡ 3. SECURITY & TENANT CORE UTILITIES
# ============================================================
def get_tenant_id():
    return st.session_state.get("tenant_id")

def require_tenant():
    if not st.session_state.get("tenant_id"):
        st.error("Session expired or unauthorized access. Please log in again.")
        st.stop()

def check_session_timeout():
    if not st.session_state.get("logged_in"):
        return

    last_activity = st.session_state.get("last_activity", datetime.now())
    idle_time = (datetime.now() - last_activity).total_seconds() / 60

    if idle_time > SESSION_TIMEOUT:
        auth_keys = ["logged_in", "authenticated", "user_id", "tenant_id", "company", "auth_session", "role"]
        for key in auth_keys:
            st.session_state.pop(key, None)
        st.session_state["view"] = "login"
        st.warning("Session timed out due to inactivity. Please log in again.")
        st.rerun()

    st.session_state["last_activity"] = datetime.now()

# ============================================================
# 📊 4. CACHED DATA LAYER ENGINE
# ============================================================
@st.cache_data(ttl=600, show_spinner=False)
def get_cached_data(table_name: str, tenant_id: str = None):
    """
    Centralized cached data retriever for Supabase tables.
    Explicitly includes tenant_id in the signature to resolve unexpected keyword 
    argument errors and force dynamic multi-tenant cache isolation.
    """
    try:
        if supabase is None:
            return pd.DataFrame()

        # Fallback to centralized state context if tenant_id wasn't passed explicitly
        if not tenant_id:
            require_tenant()
            tenant_id = get_tenant_id()

        # Query isolated data within the active tenant boundary
        res = supabase.table(table_name).select("*").eq("tenant_id", tenant_id).execute()

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
            st.error("❌ Database not connected")
            return False

        require_tenant()

        if dataframe is None or dataframe.empty:
            st.error("No Data to commit.")
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
# 📤 5. STORAGE & AUDIT HELPERS
# ============================================================
def safe_audit_log(payload):
    try:
        if supabase:
            supabase.table("audit_logs").insert(payload).execute()
    except Exception:
        pass

def upload_image(file, bucket="collateral-photos"):
    try:
        if supabase is None:
            st.error("Storage unavailable: Supabase client not initialized.")
            return None

        require_tenant()
        tenant_id = get_tenant_id()

        clean_name = re.sub(r'[^a-zA-Z0-9._-]', '_', file.name)
        file_path = f"{tenant_id}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_{clean_name}"

        supabase.storage.from_(bucket).upload(
            path=file_path,
            file=file.getvalue(),
            file_options={"content-type": file.type}
        )

        return supabase.storage.from_(bucket).get_public_url(file_path)
    except Exception as e:
        st.error(f"Upload failed: {e}")
        return None

# ============================================================
# 🎨 6. THEME CUSTOMIZATION ENGINE
# ============================================================
def apply_master_theme():
    # 🎨 Read the brand color actively set via the settings panel color picker
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    
    # Helper: Convert clean Hex strings into functional translucent RGB arrays for glass layouts
    hex_color = brand_color.lstrip('#')
    try:
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    except Exception:
        rgb = (43, 63, 135) # Fallback to core Slate Blue state

    st.markdown(f"""
    <style>
    /* 1. Base Workspace Canvas Background */
    .stApp {{
        background: linear-gradient(135deg, #F8FAFC 0%, #F1F5F9 100%) !important;
    }}

    /* 2. DYNAMIC SIDEBAR TINT WITH GLASSMORPHISM ENGINE */
    /* Mixes 85% pure white with 15% of your customized settings brand color tint */
    [data-testid="stSidebar"] {{
        background-color: rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, 0.08) !important;
        background-image: linear-gradient(
            180deg, 
            rgba(255, 255, 255, 0.65) 0%, 
            rgba(255, 255, 255, 0.45) 100%
        ) !important;
        backdrop-filter: blur(25px) saturate(180%) !important;
        -webkit-backdrop-filter: blur(25px) saturate(180%) !important;
        border-right: 1px solid rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, 0.12) !important;
        box-shadow: 4px 0 30px 0 rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, 0.05) !important;
    }}
    
    [data-testid="stSidebar"] > div:first-child {{
        background: transparent !important;
    }}

    /* 3. Typography Realignment with High-Contrast Slate Elements */
    [data-testid="stSidebar"] .stMarkdown p, 
    [data-testid="stSidebar"] label, 
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {{
        color: #0F172A !important;
        font-weight: 500 !important;
    }}

    [data-testid="stSidebar"] p[data-testid="stCaptionText"],
    [data-testid="stSidebar"] .stMarkdown caption {{
        color: #475569 !important;
    }}

    /* 4. Selectbox Input Structural Modifications */
    div[data-baseweb="select"] > div {{
        background: rgba(255, 255, 255, 0.85) !important;
        border: 1px solid rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, 0.15) !important;
        border-radius: 12px !important;
    }}
    
    div[data-baseweb="select"] span {{
        color: #0F172A !important;
    }}

    /* 5. Navigation Items Radio Group Styling */
    [data-testid="stSidebar"] [data-testid="stRadio"] div[role="radiogroup"] {{
        background: rgba(255, 255, 255, 0.4) !important;
        border-radius: 16px !important;
        padding: 6px !important;
        border: 1px solid rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, 0.06) !important;
    }}

    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] {{
        background: transparent;
        transition: all 0.2s ease-in-out;
        padding: 8px 12px !important;
        border-radius: 10px !important;
        margin-bottom: 3px !important;
        width: 100% !important;
    }}

    [data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"]:hover {{
        background: rgba(255, 255, 255, 0.75) !important;
    }}

    /* 6. Action Button Syncing (Matches your dynamic hex code perfectly) */
    [data-testid="stSidebar"] button {{
        background: rgba(255, 255, 255, 0.75) !important;
        color: #0F172A !important;
        backdrop-filter: blur(8px) !important;
        border: 1px solid rgba({rgb[0]}, {rgb[1]}, {rgb[2]}, 0.15) !important;
        border-radius: 12px !important;
        font-weight: 500 !important;
        transition: all 0.2s ease !important;
    }}

    [data-testid="stSidebar"] button:hover {{
        background: #FFFFFF !important;
        border-color: {brand_color} !important;
        color: {brand_color} !important;
        transform: translateY(-0.5px);
    }}
    </style>
    """, unsafe_allow_html=True)

def auth_styles():
    st.markdown("""
    <style>
    .stApp { background: linear-gradient(135deg,#EEF4FF,#F8FAFC); }
    .stButton > button { border-radius: 12px; height: 42px; font-weight: 600; }
    .stFormSubmitButton > button { background: linear-gradient(90deg,#1D4ED8,#2563EB); color: white; }
    </style>
    """, unsafe_allow_html=True)

# ============================================================
# 🏢 7. DIALOGS & USER ROUTING INTERFACES
# ============================================================
@st.dialog("🏢 Register Company")
def admin_company_registration():
    st.caption("Create your organization account")
    with st.form("company_reg_form"):
        company_name = st.text_input("Organization Name", placeholder="Peak-Lenders Africa")
        admin_name = st.text_input("Admin Full Name", placeholder="John Doe")
        email = st.text_input("Business Email", placeholder="admin@company.com")
        pwd = st.text_input("Password", type="password")
        submit = st.form_submit_button("✨ Create Organization", use_container_width=True)

    if submit:
        if not all([company_name, admin_name, email, pwd]):
            st.error("All fields are required")
            return
        try:
            res = supabase.auth.sign_up({"email": email, "password": pwd})
            if not res.user:
                st.error("Registration failed")
                return

            tenant_id = str(uuid.uuid4())
            company_code = f"{company_name[:3].upper()}{uuid.uuid4().int % 999}"

            supabase.table("tenants").insert({"id": tenant_id, "name": company_name, "company_code": company_code}).execute()
            supabase.table("users").insert({"id": res.user.id, "name": admin_name, "email": email, "tenant_id": tenant_id, "role": "Admin"}).execute()

            safe_audit_log({
                "user_id": res.user.id, "action": "CREATE_COMPANY", "tenant_id": tenant_id, "timestamp": datetime.now().isoformat()
            })
            st.success(f"✅ Organization created!\n\nCompany Code: **{company_code}**")
        except Exception as e:
            st.error(f"Registration failed: {e}")

@st.dialog("👥 Staff Registration")
def view_staff_signup():
    st.caption("Create a staff account")
    with st.form("staff_signup_form"):
        company_code_input = st.text_input("Company Registration Code", placeholder="PEA123")
        name = st.text_input("Full Name", placeholder="Jane Doe")
        email = st.text_input("Email Address", placeholder="staff@company.com")
        pwd = st.text_input("Password", type="password")
        submit = st.form_submit_button("🚀 Create Staff Account", use_container_width=True)

    if submit:
        if not all([company_code_input, name, email, pwd]):
            st.error("All fields are required")
            return
        try:
            tenant_query = supabase.table("tenants").select("*").eq("company_code", company_code_input.strip().upper()).execute()
            if not tenant_query.data:
                st.error("Invalid Company Registration Code.")
                return

            tenant = tenant_query.data[0]
            res = supabase.auth.sign_up({"email": email, "password": pwd})
            if not res.user:
                st.error("Signup failed")
                return

            supabase.table("users").insert({"id": res.user.id, "name": name, "email": email, "tenant_id": tenant["id"], "role": "Staff"}).execute()
            st.success("✅ Staff account created successfully!")
        except Exception as e:
            st.error(f"Signup failed: {e}")

@st.dialog("🔑 Forgot Password")
def forgot_password_page():
    st.caption("Reset your account password")
    email = st.text_input("Registered Email", placeholder="you@example.com")
    if st.button("📩 Send Reset Link", use_container_width=True):
        if not email:
            st.error("Email required")
            return
        try:
            supabase.auth.reset_password_for_email(email)
            st.success("✅ Password reset email sent")
        except Exception as e:
            st.error(f"Reset failed: {e}")

# ============================================================
# 🔐 8. MAIN ACCESS PORTAL (FIXED INVERSION SCOPE)
# ============================================================
def login_page():
    auth_styles()
    st.write(" ")
    
    _, center, _ = st.columns([1, 1.4, 1])
    with center:
        with st.container(border=True):
            st.markdown("## 💰 PEAK-LENDERS AFRICA")
            st.caption("Secure Business Login Portal")
            st.divider()

            with st.form("login_form"):
                # 🏢 Switched from Company Code back to Business Name
                company_name_input = st.text_input(
                    "Business Name", 
                    placeholder="e.g., Peak-Lenders Africa"
                ).strip()
                
                email = st.text_input("Email Address", placeholder="Enter email").strip().lower()
                pwd = st.text_input("Password", type="password", placeholder="Enter password")
                submit = st.form_submit_button("🔓 Access Dashboard", use_container_width=True)

            if submit:
                if not all([company_name_input, email, pwd]):
                    st.error("All input fields are required.")
                else:
                    try:
                        # Step 1: Attempt normal Supabase authentication
                        res = supabase.auth.sign_in_with_password({"email": email, "password": pwd})
                        if not res or not res.user:
                            st.error("Invalid credentials")
                            return

                        # Step 2: Fetch the user profile along with the linked tenant data
                        user_query = supabase.table("users").select("*, tenants(*)").eq("id", res.user.id).execute()
                        if not user_query.data:
                            st.error("Profile matching registration missing.")
                            return

                        user_profile = user_query.data[0]
                        tenant_data = user_profile.get("tenants") or {}
                        
                        # Extract the exact company name from the DB
                        db_company_name = tenant_data.get("name", "").strip()

                        # Step 3: Case-insensitive match on the clean Business Name string
                        if db_company_name.lower() != company_name_input.lower():
                            st.error(f"Access Denied: This account is not verified under '{company_name_input}'.")
                            return

                        # Step 4: Establish verified session states
                        st.session_state["logged_in"] = True
                        st.session_state["authenticated"] = True
                        st.session_state["user_id"] = user_profile["id"]
                        st.session_state["tenant_id"] = user_profile["tenant_id"]
                        st.session_state["role"] = user_profile.get("role", "Staff")
                        st.session_state["company"] = db_company_name
                        st.session_state["last_activity"] = datetime.now()
                        st.session_state["view"] = "main"

                        st.success("Login authorized!")
                        time.sleep(0.4)
                        st.rerun()

                    except Exception as e:
                        st.error(f"Login failed: {e}")

            st.write(" ")
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🏢 Register", use_container_width=True):
                    admin_company_registration()
            with col2:
                if st.button("👥 Staff", use_container_width=True):
                    view_staff_signup()
            with col3:
                if st.button("🔑 Reset", use_container_width=True):
                    forgot_password_page()
# ============================================================
# 🌐 9. APPLICATION ROUTER GATEWAY
# ============================================================
def run_auth_ui(supabase_client): # Add the parameter here
    check_session_timeout()
    
    view = st.session_state.get("view", "login")
    
    if st.session_state.get("logged_in") and view != "main":
        st.session_state["view"] = "main"
        st.rerun()

    if view == "login":
        login_page() # If login_page accesses global client
   
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
        new_tenant_id = active_company.get("id")

        # 🔥 CRITICAL FIX: If the tenant changed, sync IDs and flush cached calculations
        if current_tenant_id != new_tenant_id:
            st.session_state["tenant_id"] = new_tenant_id
            st.session_state["company"] = selected_name
            # Clear data cache pools so the next page render is forced to read fresh DB values
            if hasattr(st, "cache_data"):
                st.cache_data.clear()
            st.rerun()

        # ----------------------------------------------------
        # 3. UPdate THEME (FAST STATE ONLY)
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
            # Removed border-radius: 50% to show your natural wide logo cleanly
            st.markdown(
                f"""
                <div style="display:flex; justify-content:center; margin-bottom:10px;">
                    <img src="{logo_url}?t={int(time.time())}"
                        style="max-width:180px; max-height:80px; object-fit:contain;" />
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
            st.markdown("<h3 style='text-align:center;'>🏢</h3>", unsafe_allow_html=True)

        # Added dynamic classes matching our slate glass theme rules
        st.markdown(
            f"""
            <div class="sidebar-company-title" style='text-align:center; font-weight:600; font-size:16px; margin-top:5px;'>
                {selected_name}
            </div>
            <div class="sidebar-company-sub" style='text-align:center; font-size:11px; letter-spacing:1px; margin-bottom:10px;'>
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
                # Clear session safely
                for k in list(st.session_state.keys()):
                    del st.session_state[k]

                st.session_state["logged_in"] = False
                st.session_state["view"] = "login"
                st.rerun()

    return selected_page

# ==========================================
# 1. CORE PAGE FUNCTIONS & LAYOUT (Overview)
# ==========================================

def show_dashboard_view():
    """
    Main Multi-Tenant Dashboard view.
    Calculates portfolio metrics and renders visual analytics isolated by tenant_id.
    """
    # Pull the active tenant context using the exact same logic as the loans page
    tenant_id = get_current_tenant()

    # Safeguard: Ensure a valid tenant context exists before doing anything
    if not tenant_id:
        st.error("❌ Access Denied: No valid tenant context detected.")
        return

    st.markdown("## 📊 Financial Dashboard")

    # 1. LOAD ALL DATA AT THE VERY START (Isolated by Tenant via cached infrastructure hooks)
    df = get_cached_data("loans", tenant_id=tenant_id)
    pay_df = get_cached_data("payments", tenant_id=tenant_id)
    exp_df = get_cached_data("expenses", tenant_id=tenant_id)

    if df is None or df.empty:
        st.info("No loan records found.")
        return

    # 2. TRANSLATE HEADERS IMMEDIATELY (The Fix for KeyErrors - matching snake_case conventions)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    if pay_df is not None and not pay_df.empty:
        pay_df.columns = pay_df.columns.str.strip().str.lower().str.replace(" ", "_")
    if exp_df is not None and not exp_df.empty:
        exp_df.columns = exp_df.columns.str.strip().str.lower().str.replace(" ", "_")

    # ============================================================
    # 3. CLEAN DATA TYPES & STANDARDIZE STATUS MATRICES
    # ============================================================
    df["interest"] = pd.to_numeric(df.get("interest", 0), errors="coerce").fillna(0)
    df["amount_paid"] = pd.to_numeric(df.get("amount_paid", 0), errors="coerce").fillna(0)
    df["principal"] = pd.to_numeric(df.get("principal", 0), errors="coerce").fillna(0)
    df["total_repayable"] = pd.to_numeric(df.get("total_repayable", 0), errors="coerce").fillna(0)
    df["balance"] = pd.to_numeric(df.get("balance", 0), errors="coerce").fillna(0)
    df["end_date"] = pd.to_datetime(df.get("end_date"), errors="coerce")
    
    # Handle implicit or explicit loan cycle parsing
    if "cycle" in df.columns:
        df["cycle"] = df["cycle"].astype(str).str.strip()
    else:
        # If your database doesn't have a dedicated cycle column, 
        # let's assume everything is cycle 1 unless marked as a rollover/BCF
        df["cycle"] = "1"

    today = pd.Timestamp.today().normalize()
    
    # 🧼 STRICT FILTER: Isolate entries that are truly UNPAID / ACTIVE
    # Must have a real outstanding balance and cannot be closed/cleared/archived.
    inactive_statuses = ["CLOSED", "CLEARED", "BCF"]
    is_active_loan = (df["balance"] > 0) & (~df["status"].astype(str).str.upper().isin(inactive_statuses))
    
    # Create the unified 'active_df' so downstream components don't crash!
    active_df = df[is_active_loan].copy()
    
    # Create a sub-segment for Cycle 1 specifically to run your precise metric calculation
    cycle_1_active_df = active_df[active_df["cycle"] == "1"]


    # ============================================================
    # 4. METRICS CALCULATION (Perfect alignment with your operational definition)
    # ============================================================
    
    # Original loans only
    original_loans = df[df["cycle_no"] == 1].copy()
    
    # Pending original loans only
    pending_original_loans = original_loans[
        original_loans["status"].astype(str).str.upper() == "PENDING"
    ]
    
    # Sum of original principal amounts
    total_issued = pending_original_loans["principal"].sum()
    
    # Expected Interest
    total_interest_expected = pending_original_loans["interest"].sum()
    
    # Match the broad aggregates seen in your management view card
    total_collected = df["amount_paid"].sum()
    
    # Overdue Count tracking matching remaining active pipelines
    overdue_mask = (active_df["end_date"] < today)
    overdue_count = (
        (df["status"].astype(str).str.upper() == "PENDING")
        & (df["balance"] > 0)
        & (df["end_date"] < today)
    ).sum()
    # 5. METRICS ROW (Zoe Soft Blue Style)
    m1, m2, m3, m4 = st.columns(4)
    
    m1.markdown(f"""<div style="background-color:#fff;padding:20px;border-radius:15px;border-left:5px solid #4A90E2;box-shadow:2px 2px 10px rgba(0,0,0,0.05);"><p style="margin:0;font-size:11px;color:#666;font-weight:bold;">💰 ACTIVE PRINCIPAL</p><h3 style="margin:0;color:#4A90E2;font-size:18px;">{total_issued:,.0f} <span style="font-size:10px;">UGX</span></h3></div>""", unsafe_allow_html=True)
    m2.markdown(f"""<div style="background-color:#fff;padding:20px;border-radius:15px;border-left:5px solid #4A90E2;box-shadow:2px 2px 10px rgba(0,0,0,0.05);"><p style="margin:0;font-size:11px;color:#666;font-weight:bold;">📈 EXPECTED INTEREST</p><h3 style="margin:0;color:#4A90E2;font-size:18px;">{total_interest_expected:,.0f} <span style="font-size:10px;">UGX</span></h3></div>""", unsafe_allow_html=True)
    m3.markdown(f"""<div style="background-color:#fff;padding:20px;border-radius:15px;border-left:5px solid #2E7D32;box-shadow:2px 2px 10px rgba(0,0,0,0.05);"><p style="margin:0;font-size:11px;color:#666;font-weight:bold;">✅ TOTAL COLLECTED</p><h3 style="margin:0;color:#2E7D32;font-size:18px;">{total_collected:,.0f} <span style="font-size:10px;">UGX</span></h3></div>""", unsafe_allow_html=True)
    m4.markdown(f"""<div style="background-color:#fff;padding:20px;border-radius:15px;border-left:5px solid #FF4B4B;box-shadow:2px 2px 10px rgba(0,0,0,0.05);"><p style="margin:0;font-size:11px;color:#666;font-weight:bold;">🚨 OVERDUE FILES</p><h3 style="margin:0;color:#FF4B4B;font-size:18px;">{overdue_count}</h3></div>""", unsafe_allow_html=True)

    # 6. RECENT ACTIVITY TABLES
    st.write("---")
    t1, t2 = st.columns(2)

    with t1:
        st.markdown("<h4 style='color: #4A90E2;'>📝 Recent Portfolio Activity</h4>", unsafe_allow_html=True)
        rows_html = ""
        
        if not active_df.empty:
            recent_loans = active_df.sort_values(by="end_date", ascending=False).head(5)
            for i, (idx, r) in enumerate(recent_loans.iterrows()):
                bg = "#F0F8FF" if i % 2 == 0 else "#FFFFFF"
                b_name = r.get('borrower', 'Unknown')
                p_amt = float(r.get('principal', 0))
                b_stat = r.get('status', 'ACTIVE')
                e_date_raw = r.get('end_date')
                e_date = pd.to_datetime(e_date_raw).strftime('%d %b') if pd.notna(e_date_raw) else "-"

                rows_html += f"""
                <tr style="background-color: {bg}; border-bottom: 1px solid #ddd;">
                    <td style="padding:10px;">{b_name}</td>
                    <td style="padding:10px; text-align:right; font-weight:bold; color:#4A90E2;">{p_amt:,.0f}</td>
                    <td style="padding:10px; text-align:center;"><span style="font-size:10px; background:#e1f5fe; padding:2px 5px; border-radius:5px;">{b_stat}</span></td>
                    <td style="padding:10px; text-align:center; color:#666;">{e_date}</td>
                </tr>"""
        
        st.markdown(f"""
            <table style="width:100%; border-collapse:collapse; font-family:sans-serif; font-size:12px; border: 1px solid #4A90E2;">
                <thead>
                    <tr style="background:#4A90E2; color:white;">
                        <th style="padding:10px;">Borrower</th>
                        <th style="padding:10px; text-align:right;">Principal</th>
                        <th style="padding:10px; text-align:center;">Status</th>
                        <th style="padding:10px; text-align:center;">Due</th>
                    </tr>
                </thead>
                <tbody>{rows_html if rows_html else "<tr><td colspan='4' style='text-align:center;padding:10px;'>No active loans</td></tr>"}</tbody>
            </table>
        """, unsafe_allow_html=True)

    with t2:
        st.markdown("<h4 style='color: #2E7D32;'>💸 Recent Cash Inflows</h4>", unsafe_allow_html=True)
        pay_rows = ""
        
        if pay_df is not None and not pay_df.empty:
            recent_pay = pay_df.sort_values(by="date", ascending=False).head(5)
            for i, (idx, r) in enumerate(recent_pay.iterrows()):
                bg = "#F0F8FF" if i % 2 == 0 else "#FFFFFF"
                p_borr = r.get('borrower', 'Unknown')
                p_val = float(r.get('amount', 0))
                p_date_raw = r.get('date')
                p_date = pd.to_datetime(p_date_raw).strftime('%d %b') if pd.notna(p_date_raw) else "-"
                
                pay_rows += f"""
                <tr style="background-color: {bg}; border-bottom: 1px solid #ddd;">
                    <td style="padding:10px;">{p_borr}</td>
                    <td style="padding:10px; text-align:right; font-weight:bold; color:green;">{p_val:,.0f}</td>
                    <td style="padding:10px; text-align:center; color:#666;">{p_date}</td>
                </tr>"""
        
        st.markdown(f"""
            <table style="width:100%; border-collapse:collapse; font-family:sans-serif; font-size:12px; border: 1px solid #2E7D32;">
                <thead>
                    <tr style="background:#2E7D32; color:white;">
                        <th style="padding:10px;">Borrower</th>
                        <th style="padding:10px; text-align:right;">Amount</th>
                        <th style="padding:10px; text-align:center;">Date</th>
                    </tr>
                </thead>
                <tbody>{pay_rows if pay_rows else "<tr><td colspan='3' style='text-align:center;padding:10px;'>No recent payments</td></tr>"}</tbody>
            </table>
        """, unsafe_allow_html=True)

    # 7. DASHBOARD VISUALS
    st.markdown("---")
    st.markdown("<h4 style='color: #4A90E2;'>📈 Portfolio Analytics</h4>", unsafe_allow_html=True)
    c_pie, c_bar = st.columns(2)

    with c_pie:
        status_counts = df["status"].astype(str).str.upper().value_counts().reset_index()
        status_counts.columns = ["status", "Count"]
        fig_pie = px.pie(status_counts, names="status", values="Count", hole=0.5, title="Loan Distribution", color_discrete_sequence=["#4A90E2", "#FF4B4B", "#FFA500"])
        fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', font_color="#2B3F87", margin=dict(t=40, b=0, l=0, r=0))
        st.plotly_chart(fig_pie, use_container_width=True, key=f"overview_pie_chart_{tenant_id}")

    with c_bar:
        # Combined Cashflow Chart (Income vs expenses)
        if pay_df is not None and not pay_df.empty and exp_df is not None and not exp_df.empty:
            pay_df["date"] = pd.to_datetime(pay_df["date"], errors='coerce')
            exp_df["date"] = pd.to_datetime(exp_df["date"], errors='coerce')
            
            # Formatting for grouping by Month-Year
            inc_m = pay_df.groupby(pay_df["date"].dt.strftime('%b %Y'))["amount"].sum().reset_index()
            exp_m = exp_df.groupby(exp_df["date"].dt.strftime('%b %Y'))["amount"].sum().reset_index()
            
            m_cash = pd.merge(inc_m, exp_m, on="date", how="outer", suffixes=('_Inc', '_Exp')).fillna(0)
            m_cash.columns = ["Month", "Income", "Expenses"]
            
            fig_bar = px.bar(m_cash, x="Month", y=["Income", "Expenses"], barmode="group", title="Performance", color_discrete_map={"Income": "#2E7D32", "Expenses": "#FF4B4B"})
            fig_bar.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="#2B3F87")
            st.plotly_chart(fig_bar, use_container_width=True, key=f"overview_bar_chart_{tenant_id}")
        else:
            st.info("💡 Tip: Record both payments and expenses to see the performance chart.")


import streamlit as st
import pandas as pd
import uuid

# ==============================
# 🚀 BORROWERS ENGINE (PRODUCTION)
# ==============================

def show_borrowers():

    # ==============================
    # 🎨 BRANDING & THEME
    # ==============================
    brand_color = st.session_state.get("theme_color", "#1E3A8A")
    st.markdown(f"<h2 style='color:{brand_color};'>🚀 Borrowers Registry</h2>", unsafe_allow_html=True)

    # ==============================
    # 🔐 TENANT SESSION CHECK
    # ==============================
    # Unified with core page functions to ensure seamless talking between files
    tenant_id = get_current_tenant()
    if not tenant_id:
        st.error("Session expired. Please log in again.")
        st.stop()

    # ==============================
    # 🧠 SAFE HELPERS (INTERNAL)
    # ==============================
    def safe_df(df):
        """Ensure the object is a valid pandas DataFrame."""
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()

    def safe_numeric(df, col, default=0.0, as_int=False):
        """Safely extract a numeric column from a DataFrame."""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return pd.Series(dtype="int64" if as_int else "float64")

        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
        else:
            s = pd.Series([default] * len(df), index=df.index)

        s = s.fillna(default)
        return s.astype("int64") if as_int else s

    # ==============================
    # 📥 LOAD & NORMALIZE DATA
    # ==============================
    borrowers_df = safe_df(get_cached_data("borrowers"))
    loans_df = safe_df(get_cached_data("loans"))
    
    # Force lowercase column names for consistency
    for df in [borrowers_df, loans_df]:
        if not df.empty:
            df.columns = df.columns.str.strip().str.lower()

    # 🔐 STRICT TENANT FILTERS (Prevents Data Leakage Across Companies)
    if not borrowers_df.empty and "tenant_id" in borrowers_df.columns:
        borrowers_df = borrowers_df[borrowers_df["tenant_id"].astype(str) == str(tenant_id)]

    if not loans_df.empty and "tenant_id" in loans_df.columns:
        loans_df = loans_df[loans_df["tenant_id"].astype(str) == str(tenant_id)]

    # Ensure required structural columns exist
    required_cols = ["id", "name", "phone", "email", "status", "national_id", "next_of_kin", "address"]
    for col in required_cols:
        if col not in borrowers_df.columns:
            borrowers_df[col] = ""

    # ==============================
    # 🔗 DATA LINKAGE
    # ==============================
    if not borrowers_df.empty and not loans_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str).str.strip()
        loans_df["borrower_id"] = loans_df["borrower_id"].astype(str).str.strip()
        
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].map(bor_map).fillna("Unknown Borrower")
    else:
        loans_df["borrower"] = "Unknown"

    # ==============================
    # 🔥 REAL-TIME RISK ENGINE
    # ==============================
    risk_map = {}
    if not loans_df.empty:
        loans_df["balance"] = safe_numeric(loans_df, "balance")
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
    tab_view, tab_add = st.tabs(["📋 View Borrowers", "➕ Add Borrower"])

    with tab_add:
        with st.form(f"add_borrower_form_{tenant_id}", clear_on_submit=True):
            st.markdown(f"<h4 style='color: {brand_color};'>📝 Register New Borrower</h4>", unsafe_allow_html=True)
            c1, c2 = st.columns(2)
            name = c1.text_input("Full Name*", key=f"br_add_name_{tenant_id}")
            phone = c2.text_input("Phone Number*", key=f"br_add_phone_{tenant_id}")
            email = c1.text_input("Email Address", key=f"br_add_email_{tenant_id}")
            nid = c2.text_input("National ID / NIN", key=f"br_add_nid_{tenant_id}")
            addr = c1.text_input("Physical Address", key=f"br_add_addr_{tenant_id}")
            nok = c2.text_input("Next of Kin (Name & Contact)", key=f"br_add_nok_{tenant_id}")
            
            if st.form_submit_button("🚀 Save Borrower Profile", use_container_width=True):
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
                    st.error("⚠️ Full Name and Phone Number are required.")

    with tab_view:
        st.markdown("### 👥 Borrowers Registry")
        search = st.text_input("🔍 Search by name or phone...", key=f"borrower_search_field_{tenant_id}").lower()
    
        if not borrowers_df.empty:
            df = borrowers_df.copy()
    
            for col in ["name", "phone", "national_id", "next_of_kin", "status"]:
                df[col] = df[col].astype(str)
    
            def get_risk_label(b_id):
                r = risk_map.get(str(b_id), {})
                return r.get("risk_label", "🟢 Healthy")
    
            df["Risk status"] = df["id"].apply(get_risk_label)
    
            # Search logic
            df_filtered = df[
                df["name"].str.lower().str.contains(search, na=False) |
                df["phone"].str.contains(search, na=False)
            ]
    
            if not df_filtered.empty:
                def style_risk(val):
                    if "🔴" in val: return "color: #EF4444; font-weight:700;"
                    elif "🟠" in val: return "color: #F97316; font-weight:700;"
                    elif "🟡" in val: return "color: #F59E0B; font-weight:700;"
                    else: return "color: #10B981; font-weight:700;"
    
                display_df = df_filtered[["name", "phone", "national_id", "next_of_kin", "Risk status", "status"]].copy()
                display_df.columns = ["Borrower Name", "Phone", "National ID", "Next of Kin", "Risk status", "status"]
                display_df["status"] = display_df["status"].str.upper()
    
                styled_df = display_df.style.map(style_risk, subset=["Risk status"])
    
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
    
                # --- Interactive Dropdown Selection ---
                st.markdown("### 🎯 Management Actions")
                
                # Derive current index safely to keep selectbox synchronized
                current_selection = st.session_state.get("selected_borrower")
                default_index = 0
                borrower_list = df_filtered["name"].tolist()
                
                if current_selection:
                    matched_rows = df_filtered[df_filtered["id"] == current_selection]
                    if not matched_rows.empty:
                        default_index = borrower_list.index(matched_rows.iloc[0]["name"]) + 1

                selected_name = st.selectbox(
                    "Select borrower:",
                    ["-- Choose borrower --"] + borrower_list,
                    index=default_index,
                    key=f"borrower_action_select_{tenant_id}"
                )
    
                if selected_name != "-- Choose borrower --":
                    sel_id = df_filtered[df_filtered["name"] == selected_name]["id"].values[0]
                    if st.session_state.get("selected_borrower") != sel_id:
                        st.session_state["selected_borrower"] = sel_id
                        st.rerun()
            else:
                st.info("No records match your search criteria.")
        else:
            st.info("The registry is currently empty.")

    # ==============================
    # 👤 BORROWER PROFILE PANEL
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
                upd_name = c1.text_input("Name", borrower["name"], key=f"prof_upd_name_{selected_id}")
                upd_phone = c2.text_input("Phone", borrower["phone"], key=f"prof_upd_phone_{selected_id}")
                upd_email = c1.text_input("Email", borrower["email"], key=f"prof_upd_email_{selected_id}")
                upd_nid = c2.text_input("National ID", borrower["national_id"], key=f"prof_upd_nid_{selected_id}")
                
                c3, c4 = st.columns(2)
                upd_nok = c3.text_input("Next of Kin", borrower["next_of_kin"], key=f"prof_upd_nok_{selected_id}")
                upd_addr = c4.text_input("Address", borrower["address"], key=f"prof_upd_addr_{selected_id}")

                # 📊 NESTED LOAN HISTORY
                st.markdown("#### 💳 Loan Statement")
                user_loans = loans_df[loans_df["borrower_id"].astype(str) == str(selected_id)].copy()

                if not user_loans.empty:
                    # Fix applied: Corrected case-sensitivity error from dateColumn -> date_column
                    st.dataframe(
                        user_loans, 
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "id": None, "tenant_id": None, "borrower_id": None, "borrower": None,
                            "principal": st.column_config.NumberColumn("principal", format="%d UGX"),
                            "interest": st.column_config.NumberColumn("Interest", format="%d UGX"),
                            "balance": st.column_config.NumberColumn("Balance", format="%d UGX"),
                            "total_repayable": st.column_config.NumberColumn("Total Due", format="%d UGX"),
                            "start_date": st.column_config.date_column("date Issued"),
                            "end_date": st.column_config.date_column("Due date"),
                        }
                    )
                    
                    csv = user_loans.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Statement (CSV)",
                        data=csv,
                        file_name=f"Statement_{upd_name.replace(' ', '_')}.csv",
                        mime="text/csv",
                        key=f"dl_statement_btn_{selected_id}"
                    )
                else:
                    st.info("This borrower has no loan history.")

                # 🛠️ ACTION BUTTONS
                st.write("---")
                act_c1, act_c2, act_c3 = st.columns([1, 1, 2])

                if act_c1.button("💾 Save Changes", use_container_width=True, key=f"save_changes_btn_{selected_id}"):
                    # Explicit row modification updates safely
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

                if act_c2.button("🗑️ Delete", use_container_width=True, key=f"delete_borrower_btn_{selected_id}"):
                    updated_df = borrowers_df[borrowers_df["id"].astype(str) != str(selected_id)]
                    if save_data_saas("borrowers", updated_df):
                        st.warning("Profile Removed")
                        st.cache_data.clear()
                        st.session_state.pop("selected_borrower", None)
                        st.rerun()
                
                if act_c3.button("❌ Close Profile", use_container_width=True, key=f"close_profile_btn_{selected_id}"):
                    st.session_state.pop("selected_borrower", None)
                    st.rerun()

import streamlit as st
import pandas as pd
import uuid
from datetime import datetime, timedelta

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
# 13. loans MANAGEMENT PAGE
# ==============================
def show_loans():
    # Fetch active tenant scope directly at engine entry point
    tenant_id = get_current_tenant()
    if not tenant_id:
        st.error("Session expired. Please log in again.")
        st.stop()

    st.markdown(
        "<h2 style='color: #0A192F;'>💵 loans Management</h2>",
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
            "amount_paid", "balance", "status", "start_date", "end_date", "cycle_no", "tenant_id"
        ])

    if borrowers_df is None:
        borrowers_df = pd.DataFrame()

    if payments_df is None:
        payments_df = pd.DataFrame()

    # ------------------------------
    # REQUIRED DEFAULTS
    # ------------------------------
    required_defaults = {
        "id": "", "sn": "", "loan_id_label": "", "parent_loan_id": "", "borrower_id": "",
        "borrower": "", "loan_type": "", "principal": 0.0, "interest": 0.0,
        "total_repayable": 0.0, "amount_paid": 0.0, "balance": 0.0, "status": "ACTIVE",
        "start_date": "", "end_date": "", "cycle_no": 1, "tenant_id": ""
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
    # date CLEANUP
    # ------------------------------
    for col in ["start_date", "end_date"]:
        loans_df[col] = pd.to_datetime(loans_df[col], errors="coerce")

    # ------------------------------
    # PAYMENT SYNC
    # ------------------------------
    loans_df["amount_paid"] = 0  # ensure column always exists
    if not payments_df.empty and "loan_id" in payments_df.columns:
        pay_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(pay_sums).fillna(0)

    loans_df["balance"] = (loans_df["total_repayable"] - loans_df["amount_paid"]).clip(lower=0)

    # ==============================
    # SERIAL ENGINE
    # ==============================
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip()
    
    existing_nums = []
    for val in loans_df["sn"]:
        if val.startswith("LN-"):
            try:
                existing_nums.append(int(val.replace("LN-", "")))
            except ValueError:
                pass
    
    next_sn_val = max(existing_nums, default=0)
    
    id_to_sn = dict(zip(loans_df["id"], loans_df["sn"]))
    id_to_parent = dict(zip(loans_df["id"], loans_df["parent_loan_id"].fillna("").astype(str).str.strip()))
    
    # Lineage Walk Assignment
    for i in loans_df.index:
        current_id = loans_df.at[i, "id"]
        if id_to_sn.get(current_id, "").startswith("LN-"):
            continue
    
        visited = set()
        parent_id = id_to_parent.get(current_id, "")
        inherited_sn = ""
    
        while parent_id and parent_id not in ("nan", ""):
            if parent_id in visited:
                break
            visited.add(parent_id)
    
            parent_sn = id_to_sn.get(parent_id, "")
            if parent_sn.startswith("LN-"):
                inherited_sn = parent_sn
                break
            parent_id = id_to_parent.get(parent_id, "")
    
        if inherited_sn:
            loans_df.at[i, "sn"] = inherited_sn
            id_to_sn[current_id] = inherited_sn
        else:
            next_sn_val += 1
            new_sn = f"LN-{next_sn_val:04d}"
            loans_df.at[i, "sn"] = new_sn
            id_to_sn[current_id] = new_sn
    
    loans_df["sn"] = loans_df["sn"].str.upper()
    loans_df = loans_df.sort_values(by=["sn", "start_date", "id"])
    loans_df["cycle_no"] = loans_df.groupby("sn").cumcount() + 1
    loans_df = loans_df.sort_values(by=["sn", "cycle_no", "start_date"])
    loans_df["status"] = ""
    
    for sn_val, grp in loans_df.groupby("sn"):
        indices = grp.index.tolist()
        latest_idx = indices[-1]
    
        if len(indices) > 1:
            loans_df.loc[indices[:-1], "status"] = "BCF"
    
        latest_row = loans_df.loc[latest_idx]
        if abs(latest_row["balance"]) < 1.0:
            loans_df.at[latest_idx, "status"] = "CLEARED"
        else:
            if int(latest_row["cycle_no"]) == 1:
                bytes_stat = "ACTIVE"
            else:
                bytes_stat = "PENDING"
            loans_df.at[latest_idx, "status"] = bytes_stat
    
    # Final overrides
    mask_zero_balance = loans_df["balance"] <= 0
    mask_not_bcf = loans_df["status"] != "BCF"
    loans_df.loc[mask_zero_balance & mask_not_bcf, "status"] = "CLEARED"
    
    loans_df = loans_df.sort_values(by=["sn", "cycle_no"]).reset_index(drop=True)
    loans_df["loan_id_label"] = loans_df["sn"].str.replace("LN-", "", regex=False).str.zfill(4)

    # ==============================
    # 🔄 DATABASE SYNC ENGINE
    # ==============================
    raw_db_df = get_cached_data("loans")
    
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
                    supabase.table("loans").update(sync_data).eq("id", row["id"]).execute()
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
        loans_df["borrower"] = loans_df["borrower_id"].map(bor_map).fillna(loans_df["borrower"]).fillna("Unknown")

    if not borrowers_df.empty and "status" in borrowers_df.columns:
        Active_borrowers = borrowers_df[borrowers_df["status"].astype(str).str.upper() == "ACTIVE"]
    else:
        Active_borrowers = pd.DataFrame(columns=["id", "name"])

    # ==============================
    # TABS DESIGN INTERFACE
    # ==============================
    tab_view, tab_add, tab_manage, tab_actions = st.tabs([
        "📑 Portfolio View", "➕ New Loan", "🛠️ Manage/Edit", "⚙️ Actions"
    ])

    # ==============================
    # TAB VIEW
    # ==============================
    with tab_view:
        search_query = st.text_input("🔍 Search Loan / borrower", key=f"loan_search_main_{tenant_id}")

        # Create a local copy for filtering
        filtered_loans = loans_df.copy() if not loans_df.empty else pd.DataFrame()

        # 🎯 FIX 1: Compute Fiscal Year Fields BEFORE metric computations
        if not filtered_loans.empty:
            if "fiscal_year" not in filtered_loans.columns:
                start_dt = pd.to_datetime(filtered_loans.get("start_date"), errors="coerce")
                start_dt = start_dt.fillna(pd.to_datetime(filtered_loans.get("created_at", pd.Timestamp.today())))
                
                fiscal_years_list = []
                for dt in start_dt:
                    if dt.month >= 7:
                        fiscal_years_list.append(f"{dt.year}/{dt.year + 1}")
                    else:
                        fiscal_years_list.append(f"{dt.year - 1}/{dt.year}")
                
                filtered_loans["fiscal_year"] = fiscal_years_list
            
            fy_unique = sorted(filtered_loans["fiscal_year"].dropna().unique().tolist())
            fy_selected = st.selectbox("Filter by Fiscal Year", ["All"] + fy_unique, key=f"loans_fy_select_{tenant_id}")
            
            if fy_selected != "All":
                filtered_loans = filtered_loans[filtered_loans["fiscal_year"] == fy_selected]

        # Apply general manual keyword search queries
        if not filtered_loans.empty and search_query:
            filtered_loans = filtered_loans[
                filtered_loans.apply(lambda r: search_query.lower() in str(r).lower(), axis=1)
            ]

        # 📊 PORTFOLIO METRICS (Now perfectly handles selection changes)
        if not filtered_loans.empty:
            total_loans = filtered_loans["sn"].nunique()
            original_loans = filtered_loans[filtered_loans["cycle_no"] == 1]  
            total_principal = original_loans["principal"].sum()
            total_repayable = filtered_loans["total_repayable"].sum()
            total_paid = filtered_loans["amount_paid"].sum()
            total_pending = filtered_loans[filtered_loans["status"] == "PENDING"]["total_repayable"].sum()

            col1, col2, col3, col4 = st.columns(4)
            col1.markdown(f'<div style="background: linear-gradient(135deg, #3b82f6, #1e3a8a); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">📄 Total loans</div><div style="font-size:22px;font-weight:bold;">{total_loans}</div></div>', unsafe_allow_html=True)
            col2.markdown(f'<div style="background: linear-gradient(135deg, #10b981, #065f46); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">💰 principal</div><div style="font-size:22px;font-weight:bold;">{total_principal:,.0f}</div></div>', unsafe_allow_html=True)
            col3.markdown(f'<div style="background: linear-gradient(135deg, #f59e0b, #92400e); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">💳 Paid</div><div style="font-size:22px;font-weight:bold;">{total_paid:,.0f}</div></div>', unsafe_allow_html=True)
            col4.markdown(f'<div style="background: linear-gradient(135deg, #ef4444, #991b1b); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">⏳ Total Pending</div><div style="font-size:22px;font-weight:bold;">{total_pending:,.0f}</div></div>', unsafe_allow_html=True)

            st.markdown("---")

        # 📋 LOAN DATA TABLE
        if filtered_loans.empty:
            st.warning("No matching loans found.")
        else:
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
        
            styled_df = (
                filtered_loans[show_cols].style
                .apply(style_entire_row, axis=1)
                .format({
                    "principal": "{:,.0f}", "amount_paid": "{:,.0f}",
                    "total_repayable": "{:,.0f}", "balance": "{:,.0f}"
                })
            )
        
            st.dataframe(styled_df, column_order=show_cols, use_container_width=True, hide_index=True)
            
    # ==============================        
    # TAB ADD LOAN
    # ==============================
    with tab_add:
        if Active_borrowers.empty:
            st.info("💡 Tip: Activate borrower first.")
        else:
            with st.form(f"loan_issue_form_v2_{tenant_id}"):
                st.markdown("<h4 style='color:#0A192F;'>📝 Create New Loan Agreement</h4>", unsafe_allow_html=True)
                col1, col2 = st.columns(2)

                borrower_map = dict(zip(Active_borrowers["name"], Active_borrowers["id"]))
                selected_name = col1.selectbox("Select borrower", list(borrower_map.keys()), key=f"loan_add_bor_select_{tenant_id}")
                selected_id = str(borrower_map[selected_name]).strip()

                amount = col1.number_input("principal amount (UGX)", min_value=0, step=50000, key=f"loan_add_amount_{tenant_id}")
                date_issued = col1.date_input("Start date", value=datetime.now(), key=f"loan_add_start_{tenant_id}")

                loan_type = col2.selectbox("Loan type", ["Business", "Personal", "Emergency", "Other"], key=f"loan_add_type_{tenant_id}")
                interest_rate = col2.number_input("Monthly Interest Rate (%)", min_value=0.0, step=0.5, key=f"loan_add_rate_{tenant_id}")
                date_due = col2.date_input("Due date", value=date_issued + timedelta(days=30), key=f"loan_add_due_{tenant_id}")

                total_due = amount + (amount * interest_rate / 100)
                st.info(f"Preview: Total Repayable {total_due:,.0f} UGX")

                submit = st.form_submit_button("🚀 Confirm & Issue Loan")

                if submit:
                    if not tenant_id:
                        st.error("Tenant session missing.")
                        st.stop()

                    if selected_id == "":
                        st.error("borrower ID missing.")
                        st.stop()

                    loan_data = {
                        "id": str(uuid.uuid4()),
                        "sn": "PENDING",  # FIX 2: Set safe initialization value to prevent infinite engine sync rerun loops
                        "loan_id_label": "PENDING",
                        "parent_loan_id": None,
                        "borrower_id": selected_id,
                        "borrower": selected_name,
                        "loan_type": loan_type,
                        "principal": float(amount),
                        "interest": float(amount * interest_rate / 100),
                        "total_repayable": float(total_due),
                        "amount_paid": 0.0,
                        "balance": float(total_due),
                        "status": "ACTIVE",
                        "start_date": str(date_issued),
                        "end_date": str(date_due),
                        "cycle_no": 1,
                        "tenant_id": tenant_id
                    }

                    if save_data("loans", pd.DataFrame([loan_data])):
                        st.success("✅ Loan issued successfully.")
                        st.cache_data.clear()
                        st.session_state.pop("loans", None)
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
            roll_sel = st.selectbox("Select Loan to Roll Forward", list(roll_map.keys()), key=f"loan_roll_select_{tenant_id}")
            parent_id = roll_map[roll_sel]
    
            loan_to_roll = eligible_loans[eligible_loans["id"] == parent_id].iloc[0]
            new_interest_rate = st.number_input("New Monthly Interest (%)", value=3.0, step=0.5, key=f"loan_roll_rate_{tenant_id}")
    
            if st.button("🔥 Execute Next Rollover", use_container_width=True, key=f"loan_roll_exec_btn_{tenant_id}"):
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
                    "id": str(uuid.uuid4()),
                    "sn": "PENDING",
                    "loan_id_label": "PENDING",
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
                    "cycle_no": int(loan_to_roll["cycle_no"]) + 1,
                    "tenant_id": get_current_tenant()
                }
    
                if save_data("loans", pd.DataFrame([new_row])):
                    st.success("✅ Loan rolled forward.")
                    st.cache_data.clear()
                    st.rerun()

    # ==============================
    # TAB MANAGE
    # ==============================
    with tab_manage:
        if not loans_df.empty:
            edit_map = {f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']}": row["id"] for _, row in loans_df.iterrows()}
            selected = st.selectbox("Select Loan to Edit", list(edit_map.keys()), key=f"loan_edit_target_select_{tenant_id}")
            target_id = edit_map[selected]
    
            loan_match = loans_df[loans_df["id"] == target_id]
            if loan_match.empty:
                st.error("Loan not found.")
                st.stop()
    
            loan_to_edit = loan_match.iloc[0]
    
            with st.form(f"edit_form_{target_id}"):
                col1, col2 = st.columns(2)
                with col1:
                    e_princ = st.number_input("principal", value=float(loan_to_edit["principal"]), key=f"edit_princ_{target_id}")
                with col2:
                    current_interest = float(loan_to_edit["interest"]) if pd.notna(loan_to_edit["interest"]) else 0.0
                    e_interest = st.number_input("Interest amount", value=current_interest, key=f"edit_interest_{target_id}")
    
                col3, col4 = st.columns(2)
                with col3:
                    current_start_date = pd.to_datetime(loan_to_edit["start_date"]).date() if pd.notna(loan_to_edit["start_date"]) else pd.Timestamp.now().date()
                    e_start_date = st.date_input("Start date", value=current_start_date, key=f"edit_start_{target_id}")
    
                with col4:
                    current_end_date = pd.to_datetime(loan_to_edit["end_date"]).date() if pd.notna(loan_to_edit["end_date"]) else pd.Timestamp.now().date()
                    e_end_date = st.date_input("End date", value=current_end_date, key=f"edit_end_{target_id}")
    
                status_options = ["ACTIVE", "PENDING", "CLEARED", "BCF", "CLOSED"]
                current_stat = str(loan_to_edit["status"]).upper()
                idx = status_options.index(current_stat) if current_stat in status_options else 0
    
                e_stat = st.selectbox("status", status_options, index=idx, key=f"edit_status_dropdown_{target_id}")
    
                if st.form_submit_button("💾 Save Changes"):
                    # 🎯 FIX 3: Recompute total_repayable dynamically on submit to keep DB accurate
                    updated_total_repayable = float(e_princ + e_interest)
                    
                    supabase.table("loans").update(
                        {
                            "principal": e_princ,
                            "interest": e_interest,
                            "total_repayable": updated_total_repayable,
                            "start_date": e_start_date.isoformat(),
                            "end_date": e_end_date.isoformat(),
                            "status": e_stat,
                        }
                    ).eq("id", target_id).execute()
    
                    st.success("✅ Updated!")
                    st.cache_data.clear()
                    st.rerun()
    
            if st.button("🗑️ Delete Loan Permanently", use_container_width=True, key=f"delete_perm_btn_{target_id}"):
                supabase.table("loans").delete().eq("id", target_id).execute()
                st.warning("Loan Deleted.")
                st.cache_data.clear()
                st.rerun()
# ==============================
# 🧾 RECEIPT GENERATION & UTILS
# ==============================
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from io import BytesIO
from datetime import datetime
import pandas as pd
import streamlit as st

def generate_receipt_pdf(data):
    """
    Renders receipt documents directly to an in-memory buffer 
    to maximize transaction safety and performance across sessions.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    styles = getSampleStyleSheet()
    content = []
    
    content.append(Paragraph("<b>PAYMENT RECEIPT</b>", styles["Title"]))
    content.append(Spacer(1, 15))
    
    for k, v in data.items():
        content.append(Paragraph(f"<b>{k}:</b> {v}", styles["Normal"]))
        content.append(Spacer(1, 10))
        
    doc.build(content)
    buffer.seek(0)
    return buffer

# ✅ SINGLE SOURCE OF TRUTH (RPC)
def generate_receipt_no(supabase, tenant_id):
    try:
        res = supabase.rpc("get_next_receipt", {"p_tenant": tenant_id}).execute()
        if res.data:
            return res.data
        return f"RCPT-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    except Exception as e:
        return f"RCPT-{datetime.now().strftime('%Y%m%d%H%M%S')}"

# ==============================
# 💵 payments MODULE (CYCLE-AWARE)
# ==============================
def show_payments(supabase):
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    st.markdown(f"<h2 style='color: {brand_color}; margin-bottom: 20px;'>💵 payments Management</h2>", unsafe_allow_html=True)

    try:
        loans_raw = get_cached_data("loans")
        payments_raw = get_cached_data("payments")
        borrowers_raw = get_cached_data("borrowers")
    except Exception as e:
        st.error(f"❌ Data load error: {e}")
        return

    # Structure dataframes safely
    raw_loans_df = pd.DataFrame(loans_raw) if loans_raw is not None else pd.DataFrame()
    raw_payments_df = pd.DataFrame(payments_raw) if payments_raw is not None else pd.DataFrame()
    raw_borrowers_df = pd.DataFrame(borrowers_raw) if borrowers_raw is not None else pd.DataFrame()

    # Normalize column schemas early
    for dataframe in [raw_loans_df, raw_payments_df, raw_borrowers_df]:
        if not dataframe.empty:
            dataframe.columns = dataframe.columns.str.lower().str.strip().str.replace(" ", "_")

    if raw_loans_df.empty:
        st.info("ℹ️ No active corporate loan records discoverable on the network.")
        return

    # 🛡️ TENANT FILTER BOUNDARY: Ensure strict company isolation
    if "tenant_id" in raw_loans_df.columns:
        loans_df = raw_loans_df[raw_loans_df["tenant_id"].astype(str) == str(current_tenant)].copy()
    else:
        loans_df = raw_loans_df.copy()

    if loans_df.empty:
        st.info("ℹ️ No active loan parameters logged under this profile.")
        return

    payments_df = raw_payments_df if "tenant_id" not in raw_payments_df.columns else raw_payments_df[raw_payments_df["tenant_id"].astype(str) == str(current_tenant)].copy()
    borrowers_df = raw_borrowers_df if "tenant_id" not in raw_borrowers_df.columns else raw_borrowers_df[raw_borrowers_df["tenant_id"].astype(str) == str(current_tenant)].copy()

    # Ensure clean string casting for relationship keys
    for df, col in [(borrowers_df, "id"), (loans_df, "borrower_id"), (loans_df, "id"), (payments_df, "loan_id")]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Borrower profile transformations
    if not borrowers_df.empty and "name" in borrowers_df.columns:
        borrower_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].map(borrower_map).fillna("Unknown Profile")
    else:
        loans_df["borrower"] = "Unknown Profile"

    # Numeric Coercion
    numeric_cols = ["total_repayable", "amount_paid", "balance", "principal", "interest"]
    for col in numeric_cols:
        if col in loans_df.columns:
            loans_df[col] = pd.to_numeric(loans_df[col].fillna(0), errors="coerce")

    if not payments_df.empty and "amount" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(payments_df["amount"].fillna(0), errors="coerce")

    # Aggregate dynamically across matching ledgers
    if not payments_df.empty:
        payment_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(payment_sums).fillna(0)
    else:
        loans_df["amount_paid"] = 0

    loans_df["balance"] = loans_df["total_repayable"] - loans_df["amount_paid"]

    # Business Logic Utilities
    def cascade_payment(df_target, sn, changed_cycle_no):
        cycles = df_target[df_target["sn"] == sn].sort_values("cycle_no").copy()
        for idx, row in cycles.iterrows():
            if int(row["cycle_no"]) <= int(changed_cycle_no):
                continue
            pos = cycles.index.get_loc(idx)
            prev_idx = cycles.index[pos - 1]
            
            prev_balance = df_target.loc[prev_idx, "balance"]
            current_interest = row["interest"]
            
            df_target.loc[idx, "principal"] = prev_balance
            df_target.loc[idx, "total_repayable"] = prev_balance + current_interest
            df_target.loc[idx, "balance"] = df_target.loc[idx, "total_repayable"] - df_target.loc[idx, "amount_paid"]

    def get_active_loan(df_source, loan_row):
        current = loan_row
        visited = set()
        while True:
            if current["id"] in visited:
                break
            visited.add(current["id"])
            child = df_source[df_source["parent_loan_id"] == current["id"]]
            if child.empty:
                return current
            current = child.iloc[0]
        return current

    # ==============================
    # 📑 INTERFACE NAVIGATION TABS
    # ==============================
    tab1, tab2 = st.tabs(["➕ Record Payment Collection", "📜 Repayment History Logs"])

    with tab1:
        active_loans = loans_df.copy()
        active_loans["label"] = active_loans.apply(
            lambda r: f"{r['borrower']} | Ref: {r.get('loan_id_label', r['id'][:8])} | Owed: UGX {r['balance']:,.0f}", axis=1
        )

        selected_index = st.selectbox("🎯 Choose Target Active Account Line", active_loans.index, format_func=lambda i: active_loans.loc[i, "label"])
        loan = active_loans.loc[selected_index]
        active_loan = get_active_loan(loans_df, loan)
        loan_id = active_loan["id"]
        current_bal = active_loan["total_repayable"] - active_loan["amount_paid"]

        st.info(f"Targeting Account Pipeline: {active_loan['borrower']} (Ref ID: {loan_id[:8]})")
        st.metric("Verified Balance Position", f"UGX {current_bal:,.0f}")

        with st.form("payment_form"):
            amount = st.number_input("Transaction amount Received (UGX)", min_value=0.0, step=10000.0) # Matched floating signatures
            method = st.selectbox("Collection Channel / Method", ["Cash", "Mobile Money", "Bank"])
            date = st.date_input("Processing Settlement date", datetime.now())
            submit = st.form_submit_button("🚀 Post Repayment Entry", use_container_width=True)

        if submit:
            if amount <= 0:
                st.warning("⚠️ Transaction entries require a positive value contribution.")
            else:
                try:
                    receipt_no = generate_receipt_no(supabase, current_tenant)

                    # 1️⃣ Insert backend payment transaction
                    supabase.table("payments").insert({
                        "receipt_no": receipt_no,
                        "loan_id": loan_id,
                        "borrower": active_loan["borrower"],
                        "amount": float(amount),
                        "date": date.strftime("%Y-%m-%d"),
                        "method": method,
                        "tenant_id": current_tenant
                    }).execute()

                    # 2️⃣ Update state matrices
                    loans_df.loc[loans_df["id"] == loan_id, "amount_paid"] += amount
                    loans_df.loc[loans_df["id"] == loan_id, "balance"] = (
                        loans_df.loc[loans_df["id"] == loan_id, "total_repayable"] - loans_df.loc[loans_df["id"] == loan_id, "amount_paid"]
                    )

                    # 3️⃣ Cascade interest balance implications
                    cascade_payment(loans_df, active_loan["sn"], int(active_loan["cycle_no"]))
                    save_data_saas("loans", loans_df)

                    # 4️⃣ Generate memory-isolated receipts cleanly
                    pdf_buffer = generate_receipt_pdf({
                        "Receipt Number": receipt_no,
                        "Borrower Entity": active_loan["borrower"],
                        "Settlement amount": f"UGX {amount:,.0f}",
                        "Payment Framework": method,
                        "Execution date": date.strftime("%Y-%m-%d"),
                    })

                    st.download_button(
                        label="📥 Download Formal Receipt Blueprint", 
                        data=pdf_buffer, 
                        file_name=f"Receipt_{receipt_no}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )

                    st.success(f"✅ Payment registered. Adjusted Owed Position: UGX {loans_df.loc[loans_df['id'] == loan_id, 'balance'].values[0]:,.0f}")
                    st.cache_data.clear()
                    st.rerun()

                except Exception as e:
                    st.error(f"❌ Post transaction sequence aborted: {e}")

    with tab2:
        if payments_df.empty:
            st.info("No localized accounting receipts currently matched inside this ledger profile.")
        else:
            # Create a clean, separate copy for layout mutations to prevent indexing side-effects
            display_df = payments_df.copy()
            display_df["amount_display"] = display_df["amount"].apply(lambda x: f"UGX {x:,.0f}")
            display_df["receipt_no"] = display_df["receipt_no"].fillna("Pending Log Ref")
            
            date_col = "date" if "date" in display_df.columns else "payment_date"
            display_df = display_df.sort_values(by=date_col, ascending=False)
            
            st.dataframe(
                display_df[[date_col, "borrower", "amount_display", "method", "receipt_no"]], 
                use_container_width=True, 
                hide_index=True
            )

            st.markdown("---")
            st.markdown("### ⚙️ Ledger Log Maintenance Modality")

            # Map selection using unique string IDs
            pay_map = {
                f"{row['receipt_no']} | {row['borrower']} | {row['amount_display']}": str(row['id'])
                for _, row in display_df.iterrows()
            }

            selected_pay_label = st.selectbox("Choose Targeted Transaction Allocation", list(pay_map.keys()))
            target_pay_id = pay_map[selected_pay_label]
            
            # SAFE FILTERING LAYER: Avoid layout index crashes
            matched_rows = display_df[display_df['id'].astype(str) == str(target_pay_id)]
            
            if matched_rows.empty:
                st.error("⚠️ Selected payment records could not be resolved from the current data frame.")
            else:
                # Safely extract target sequence without using rigid positional index assumptions
                target_pay = matched_rows.iloc[0]

                p_col1, p_col2 = st.columns(2)

                if p_col1.button("🗑️ Purge Payment From Records", use_container_width=True):
                    try:
                        # 1️⃣ Execute Supabase backend mutation drop
                        supabase.table("payments").delete().eq("id", target_pay_id).execute()
                        
                        loan_id = str(target_pay["loan_id"])
                        
                        # Guard against downstream lookups failing if loan object metadata structure is modified
                        matching_loans = loans_df[loans_df["id"].astype(str) == loan_id]
                        if not matching_loans.empty:
                            affected_loan = matching_loans.iloc[0]
                            
                            # 2️⃣ Readjust running balance calculations locally
                            loans_df.loc[loans_df["id"].astype(str) == loan_id, "amount_paid"] -= float(target_pay["amount"])
                            loans_df.loc[loans_df["id"].astype(str) == loan_id, "balance"] = (
                                loans_df.loc[loans_df["id"].astype(str) == loan_id, "total_repayable"] - 
                                loans_df.loc[loans_df["id"].astype(str) == loan_id, "amount_paid"]
                            )
                            
                            # 3️⃣ Cascade interest structural sequences down-chain
                            cascade_payment(loans_df, affected_loan["sn"], int(affected_loan["cycle_no"]))
                            save_data_saas("loans", loans_df)
                        
                        st.cache_data.clear()
                        st.warning(f"Payment reference key {target_pay['receipt_no']} dropped from backend successfully.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Execution aborted: {e}")

                if p_col2.button("📝 Edit Asset Metrics", use_container_width=True):
                    st.session_state["edit_pay_mode"] = True

                if st.session_state.get("edit_pay_mode"):
                    with st.form("edit_payment_form"):
                        st.info(f"Modifying Entry Core Attributes: {target_pay['receipt_no']}")
                        
                        # Float value types calibrated with explicit steps to bypass number input crash risks
                        new_amt = st.number_input("Revised amount Allocation (UGX)", value=float(target_pay['amount']), step=5000.0)
                        
                        current_method = target_pay['method']
                        method_options = ["Cash", "Mobile Money", "Bank"]
                        method_idx = method_options.index(current_method) if current_method in method_options else 0
                        new_method = st.selectbox("Revised Collection Channel", method_options, index=method_idx)
                        
                        eb1, eb2 = st.columns(2)

                        if eb1.form_submit_button("💾 Save Adjustment Matrices", use_container_width=True):
                            try:
                                supabase.table("payments").update({
                                    "amount": new_amt,
                                    "method": new_method
                                }).eq("id", target_pay_id).execute()
                                
                                loan_id = str(target_pay["loan_id"])
                                matching_loans = loans_df[loans_df["id"].astype(str) == loan_id]
                                
                                if not matching_loans.empty:
                                    affected_loan = matching_loans.iloc[0]
                                    
                                    diff = new_amt - float(target_pay["amount"])
                                    loans_df.loc[loans_df["id"].astype(str) == loan_id, "amount_paid"] += diff
                                    loans_df.loc[loans_df["id"].astype(str) == loan_id, "balance"] = (
                                        loans_df.loc[loans_df["id"].astype(str) == loan_id, "total_repayable"] - 
                                        loans_df.loc[loans_df["id"].astype(str) == loan_id, "amount_paid"]
                                    )
                                    
                                    cascade_payment(loans_df, affected_loan["sn"], int(affected_loan["cycle_no"]))
                                    save_data_saas("loans", loans_df)
                                
                                st.session_state["edit_pay_mode"] = False
                                st.cache_data.clear()
                                st.success("Changes committed.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Update failed: {e}")

                        if eb2.form_submit_button("❌ Drop Corrections", use_container_width=True):
                            st.session_state["edit_pay_mode"] = False
                            st.rerun()

# ==============================
# 20. PAYROLL MANAGEMENT PAGE
# ==============================

def show_payroll():
    """
    Handles employee compensation, tax compliance (PAYE/LST), 
    and NSSF contributions. Includes a professional printable report.
    Isolated by tenant_id for SaaS architecture.
    """
    if st.session_state.get("role") != "Admin":
        st.error("🔒 Restricted Access: Only Administrators can process payroll.")
        return

    # Safeguard: Ensure a valid tenant context exists before loading page resources
    if not tenant_id:
        st.error("❌ Access Denied: No valid tenant context detected.")
        return

    st.markdown("<h2 style='color: #4A90E2;'>🧾 Payroll Management</h2>", unsafe_allow_html=True)

    # 1. SYNC COLUMNS (Scoped by tenant_id)
    df = get_cached_data("Payroll", tenant_id=tenant_id)
    required_columns = [
        "Payroll_ID", "Employee", "TIN", "Designation", "Mob_No", "Account_No", "NSSF_No",
        "Arrears", "Basic_Salary", "Absent_Deduction", "LST", "Gross_Salary", 
        "PAYE", "NSSF_5", "Advance_DRS", "Other_Deductions", "Net_Pay", 
        "NSSF_10", "NSSF_15", "date"
    ]
    
    if df.empty:
        df = pd.DataFrame(columns=required_columns)
    else:
        df.columns = df.columns.str.strip().str.replace(" ", "_")
        for col in required_columns:
            if col not in df.columns: df[col] = 0
        df = df.fillna(0)

    def run_manual_sync_calculations(basic, arrears, absent_deduct, advance, other):
        # 1. Gross Calculation
        gross = (float(basic) + float(arrears)) - float(absent_deduct)
        
        # 2. Local Service Tax (LST) Logic
        lst = 100000 / 12 if gross > 1000000 else 0
        
        # 3. NSSF Logic (Calculated but NOT subtracted from tax base)
        n5 = gross * 0.05
        n10 = gross * 0.10
        n15 = n5 + n10
        
        # 4. --- THE EXCEL MATCHING PAYE LOGIC ---
        # Based on your sheet: Tax = 25,000 + (30% * (Gross - 410,000))
        paye = 0
        if gross > 410000:
            excess = gross - 410000
            paye = 25000 + (0.30 * excess)
        elif gross > 235000:
            # Lower tier fallback
            paye = (gross - 235000) * 0.10
            
        # 5. Final Deductions & Net Pay
        # Deductions = PAYE + LST + NSSF(5%) + Advance + Other
        total_deductions = paye + lst + n5 + float(advance) + float(other)
        net = gross - total_deductions
        
        return {
            "gross": round(gross), "lst": round(lst), "n5": round(n5), 
            "n10": round(n10), "n15": round(n15), "paye": round(paye), "net": round(net)
        }

    tab_process, tab_logs = st.tabs(["➕ Process Salary", "📜 Payroll History"])

    with tab_process:
        with st.form("new_payroll_form", clear_on_submit=True):
            st.markdown("<h4 style='color: #2B3F87;'>👤 Employee Details</h4>", unsafe_allow_html=True)
            name = st.text_input("Employee Name")
            c1, c2, c3 = st.columns(3); f_tin = c1.text_input("TIN"); f_desig = c2.text_input("Designation"); f_mob = c3.text_input("Mob No.")
            c4, c5 = st.columns(2); f_acc = c4.text_input("Account No."); f_nssf_no = c5.text_input("NSSF No.")
            st.write("---"); st.markdown("<h4 style='color: #2B3F87;'>💰 Earnings & Deductions</h4>", unsafe_allow_html=True)
            c6, c7, c8 = st.columns(3); f_arrears = c6.number_input("ARREARS", min_value=0.0); f_basic = c7.number_input("SALARY (Basic)", min_value=0.0); f_absent = c8.number_input("Absenteeism Deduction", min_value=0.0)
            c9, c10 = st.columns(2); f_adv = c9.number_input("S.DRS / ADVANCE", min_value=0.0); f_other = c10.number_input("Other Deductions", min_value=0.0)

            if st.form_submit_button("💳 Confirm & Release Payment", use_container_width=True):
                if name and f_basic > 0:
                    calc = run_manual_sync_calculations(f_basic, f_arrears, f_absent, f_adv, f_other)
                    new_row = pd.DataFrame([{
                        "Payroll_ID": int(df["Payroll_ID"].max() + 1) if not df.empty else 1,
                        "Employee": name, "TIN": f_tin, "Designation": f_desig, "Mob_No": f_mob,
                        "Account_No": f_acc, "NSSF_No": f_nssf_no, "Arrears": f_arrears,
                        "Basic_Salary": f_basic, "Absent_Deduction": f_absent,
                        "Gross_Salary": calc['gross'], "LST": calc['lst'], "PAYE": calc['paye'],
                        "NSSF_5": calc['n5'], "NSSF_10": calc['n10'], "NSSF_15": calc['n15'],
                        "Advance_DRS": f_adv, "Other_Deductions": f_other, "Net_Pay": calc['net'],
                        "date": datetime.now().strftime("%Y-%m-%d")
                    }])
                    # --- THE CORRECTED SAVE LOGIC (Payroll Fix) ---
                    # 1. Combine the old data with the new record
                    final_save_df = pd.concat([df, new_row], ignore_index=True)
                    
                    # 2. CRITICAL: Replace all NaN/Blanks with 0 to stop the JSON error
                    final_save_df = final_save_df.fillna(0)
                    
                    # 3. Restore spaces for Google Sheets headers
                    final_save_df.columns = [c.replace("_", " ") for c in final_save_df.columns]
                    
                    # 4. Save to Google Sheets (Scoped by tenant_id)
                    if save_data("Payroll", final_save_df, tenant_id=tenant_id):
                        st.success(f"✅ Payroll for {name} saved successfully!")
                        st.rerun()

    with tab_logs:
        if not df.empty:
            p_col1, p_col2 = st.columns([4, 1])
            p_col1.markdown(f"<h3 style='color: #4A90E2;'>{datetime.now().strftime('%B %Y')} Summary</h3>", unsafe_allow_html=True)
            
            def fm(x): 
                try: return f"{int(float(x)):,}" 
                except: return "0"

            # --- CALCULATE PAYROLL TOTALS ---
            t_arrears = df['Arrears'].sum()
            t_basic = df['Basic_Salary'].sum()
            t_gross = df['Gross_Salary'].sum()
            t_paye = df['PAYE'].sum()
            t_n5 = df['NSSF_5'].sum()
            t_net = df['Net_Pay'].sum()
            t_n10 = df['NSSF_10'].sum()
            t_n15 = df['NSSF_15'].sum()

            rows_html = ""
            for i, r in df.iterrows():
                rows_html += f"""
                    <tr>
                        <td style='text-align:center; border:1px solid #ddd; padding: 10px;'>{i+1}</td>
                        <td style='border:1px solid #ddd; padding: 10px;'>
                            <div style="font-weight:bold; font-size:12px;">{r['Employee']}</div>
                            <div style="font-size:10px; color:#555;">{r.get('Designation', '-')}</div>
                        </td>
                        <td style='text-align:right; border:1px solid #ddd; padding: 10px;'>{fm(r['Arrears'])}</td>
                        <td style='text-align:right; border:1px solid #ddd; padding: 10px;'>{fm(r['Basic_Salary'])}</td>
                        <td style='text-align:right; border:1px solid #ddd; padding: 10px; font-weight:bold;'>{fm(r['Gross_Salary'])}</td>
                        <td style='text-align:right; border:1px solid #ddd; padding: 10px;'>{fm(r['PAYE'])}</td>
                        <td style='text-align:right; border:1px solid #ddd; padding: 10px;'>{fm(r['NSSF_5'])}</td>
                        <td style='text-align:right; border:1px solid #ddd; padding: 10px; background:#E3F2FD; font-weight:bold;'>{fm(r['Net_Pay'])}</td>
                        <td style='text-align:right; border:1px solid #ddd; padding: 10px; background:#FFF9C4;'>{fm(r['NSSF_10'])}</td>
                        <td style='text-align:right; border:1px solid #ddd; padding: 10px; background:#FFF9C4; font-weight:bold;'>{fm(r['NSSF_15'])}</td>
                    </tr>"""

            # --- ADD THE TOTALS ROW TO THE HTML ---
            rows_html += f"""
                <tr style="background:#2B3F87; color:white; font-weight:bold;">
                    <td colspan="2" style="text-align:center; padding:12px; border:1px solid #ddd;">GRAND TOTALS</td>
                    <td style='text-align:right; border:1px solid #ddd; padding: 12px;'>{fm(t_arrears)}</td>
                    <td style='text-align:right; border:1px solid #ddd; padding: 12px;'>{fm(t_basic)}</td>
                    <td style='text-align:right; border:1px solid #ddd; padding: 12px;'>{fm(t_gross)}</td>
                    <td style='text-align:right; border:1px solid #ddd; padding: 12px;'>{fm(t_paye)}</td>
                    <td style='text-align:right; border:1px solid #ddd; padding: 12px;'>{fm(t_n5)}</td>
                    <td style='text-align:right; border:1px solid #ddd; padding: 12px;'>{fm(t_net)}</td>
                    <td style='text-align:right; border:1px solid #ddd; padding: 12px;'>{fm(t_n10)}</td>
                    <td style='text-align:right; border:1px solid #ddd; padding: 12px;'>{fm(t_n15)}</td>
                </tr>"""

            printable_html = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: sans-serif; padding: 20px; }}
                    table {{ width: 100%; border-collapse: collapse; font-size: 11px; }}
                    th {{ background: #2B3F87; color: white; padding: 10px; border: 1px solid #ddd; }}
                    @media print {{ @page {{ size: landscape; margin: 1cm; }} }}
                </style>
            </head>
            <body>
                <div style="text-align:center; border-bottom:3px solid #2B3F87; margin-bottom:20px;">
                    <h1 style="color:#2B3F87;">ZOE CONSULTS SMC LTD</h1>
                    <p><b>PAYROLL REPORT - {datetime.now().strftime('%B %Y')}</b></p>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>S/N</th><th>Employee</th><th>Arrears</th><th>Basic</th><th>Gross</th>
                            <th>P.A.Y.E</th><th>NSSF(5%)</th><th>Net Pay</th><th>NSSF(10%)</th><th>NSSF(15%)</th>
                        </tr>
                    </thead>
                    <tbody>{rows_html}</tbody>
                </table>
                <div style="margin-top:50px; display:flex; justify-content:space-around;">
                    <p>___________________<br>Prepared By</p>
                    <p>___________________<br>Approved By</p>
                </div>
            </body>
            </html>
            """
            if p_col2.button("📥 Print PDF", key=f"print_payroll_trigger_{tenant_id}"):
                st.components.v1.html(printable_html + "<script>window.print();</script>", height=0)

            st.components.v1.html(printable_html, height=600, scrolling=True)

            csv_text = df.to_csv(index=False).encode('utf-8')
            st.download_button("📄 Download CSV Backup", data=csv_text, file_name=f"Payroll_Zoe_{tenant_id}.csv", mime="text/csv", key=f"download_csv_{tenant_id}")
            
            st.write("---")
            with st.expander("⚙️ Modify / Delete Record"):
                pay_opts = [f"{r['Employee']} (ID: {r['Payroll_ID']})" for _, r in df.iterrows()]
                if pay_opts:
                    sel_opt = st.selectbox("Select Record to Manage", pay_opts, key=f"payroll_edit_selectbox_{tenant_id}")
                    try:
                        sid = str(sel_opt.split("(ID: ")[1].replace(")", ""))
                        item = df[df['Payroll_ID'].astype(str) == sid].iloc[0]
                        st.text_input("Edit Name (Preview)", value=str(item['Employee']), disabled=True, key=f"payroll_preview_name_{tenant_id}")
                        st.info("Direct modification of payroll math is locked. Delete and re-process for errors.")
                        if st.button("🗑️ Delete This Record", use_container_width=True, key=f"payroll_delete_btn_{tenant_id}"):
                            df_new = df[df['Payroll_ID'].astype(str) != sid]
                            df_new.columns = [c.replace("_", " ") for c in df_new.columns]
                            if save_data("Payroll", df_new, tenant_id=tenant_id):
                                st.warning("Payroll record deleted.")
                                st.rerun()
                    except Exception as e:
                        st.error(f"Selection error: {e}")
        else:
            st.info("No payroll records found for this period.")
# ==============================
# 21. ADVANCED ANALYTICS & REPORTS
# ==============================

def show_reports(tenant_id: str):
    """
    Consolidates data across all modules to provide high-level 
    financial health metrics, cash flow trends, and risk assessment.
    Isolated by tenant_id for SaaS architecture with all Petty Cash elements removed.
    """
    # Safeguard: Ensure a valid tenant context exists before loading page resources
    if not tenant_id:
        st.error("❌ Access Denied: No valid tenant context detected.")
        return

    st.markdown("<h2 style='color: #4A90E2;'>📊 Advanced Analytics & Reports</h2>", unsafe_allow_html=True)
    
    # 1. FETCH DATA (Scoped by tenant_id)
    loans = get_cached_data("loans", tenant_id=tenant_id)
    payments = get_cached_data("payments", tenant_id=tenant_id)
    expenses = get_cached_data("expenses", tenant_id=tenant_id)
    payroll = get_cached_data("Payroll", tenant_id=tenant_id)

    if loans is None or loans.empty:
        st.info("📈 Record more loans to see your financial analytics.")
        return

    # 2. THE ULTIMATE PAYROLL SAFETY CHECK
    if not isinstance(payroll, pd.DataFrame):
        payroll = pd.DataFrame()

    # Initializing tax/deduction totals to 0 to prevent local variable errors
    nssf_total, paye_total = 0, 0
    
    if not payroll.empty:
        # Standardize payroll headers for logic
        payroll.columns = payroll.columns.str.strip().str.lower().str.replace(" ", "_")
        # Use a super-safe way to pull column totals
        n5 = pd.to_numeric(payroll.get("nssf_5", 0), errors="coerce").fillna(0).sum()
        n10 = pd.to_numeric(payroll.get("nssf_10", 0), errors="coerce").fillna(0).sum()
        nssf_total = n5 + n10
        paye_total = pd.to_numeric(payroll.get("paye", 0), errors="coerce").fillna(0).sum()

    # 3. OTHER DATA SUMS
    # Standardize column headers for math logic to match loans module storage structure
    loans.columns = loans.columns.str.strip().str.lower().str.replace(" ", "_")
    
    if payments is not None and not payments.empty:
        payments.columns = payments.columns.str.strip().str.lower().str.replace(" ", "_")
    if expenses is not None and not expenses.empty:
        expenses.columns = expenses.columns.str.strip().str.lower().str.replace(" ", "_")
    
    # Safe principal and interest Lookup
    l_amt = pd.to_numeric(loans.get("principal", 0), errors="coerce").fillna(0).sum()
    l_int = pd.to_numeric(loans.get("interest", 0), errors="coerce").fillna(0).sum()
    
    p_amt = pd.to_numeric(payments.get("amount", 0), errors="coerce").fillna(0).sum() if (payments is not None and not payments.empty) else 0
    exp_amt = pd.to_numeric(expenses.get("amount", 0), errors="coerce").fillna(0).sum() if (expenses is not None and not expenses.empty) else 0
    
    # 💰 FINANCIAL LOGIC:
    # Total Outflow = Direct expenses + Taxes (PAYE/NSSF) | Petty Cash explicitly removed
    total_outflow = exp_amt + nssf_total + paye_total
    
    # Net Profit = Inflows (payments) - Outflows (expenses/taxes)
    net_profit = p_amt - total_outflow

    # 4. KPI DASHBOARD (Soft Blue Branded)
    st.subheader("🚀 Financial Performance")
    k1, k2, k3, k4 = st.columns(4)
    
    k1.markdown(f"""<div style="background-color:#fff;padding:15px;border-radius:10px;border-left:5px solid #4A90E2;box-shadow:2px 2px 8px rgba(0,0,0,0.05);"><p style="margin:0;font-size:11px;color:#666;font-weight:bold;">CAPITAL ISSUED</p><h4 style="margin:0;color:#4A90E2;">{l_amt:,.0f}</h4></div>""", unsafe_allow_html=True)
    k2.markdown(f"""<div style="background-color:#fff;padding:15px;border-radius:10px;border-left:5px solid #4A90E2;box-shadow:2px 2px 8px rgba(0,0,0,0.05);"><p style="margin:0;font-size:11px;color:#666;font-weight:bold;">INTEREST ACCRUED</p><h4 style="margin:0;color:#4A90E2;">{l_int:,.0f}</h4></div>""", unsafe_allow_html=True)
    k3.markdown(f"""<div style="background-color:#fff;padding:15px;border-radius:10px;border-left:5px solid #2E7D32;box-shadow:2px 2px 8px rgba(0,0,0,0.05);"><p style="margin:0;font-size:11px;color:#666;font-weight:bold;">COLLECTIONS</p><h4 style="margin:0;color:#2E7D32;">{p_amt:,.0f}</h4></div>""", unsafe_allow_html=True)
    
    p_color = "#2E7D32" if net_profit >= 0 else "#FF4B4B"
    k4.markdown(f"""<div style="background-color:#fff;padding:15px;border-radius:10px;border-left:5px solid {p_color};box-shadow:2px 2px 8px rgba(0,0,0,0.05);"><p style="margin:0;font-size:11px;color:#666;font-weight:bold;">NET PROFIT</p><h4 style="margin:0;color:{p_color};">{net_profit:,.0f}</h4></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # 5. VISUAL ANALYTICS
    st.markdown("---")
    col_left, col_right = st.columns(2)

    with col_left:
        st.write("**💰 Income vs. Expenses (Monthly)**")
        if payments is not None and not payments.empty:
            pay_copy = payments.copy()
            pay_copy["date"] = pd.to_datetime(pay_copy.get("date"), errors='coerce')
            inc_trend = pay_copy.groupby(pay_copy["date"].dt.strftime('%Y-%m'))["amount"].sum().reset_index()
            
            if expenses is not None and not expenses.empty:
                exp_copy = expenses.copy()
                exp_copy["date"] = pd.to_datetime(exp_copy.get("date"), errors='coerce')
                exp_trend = exp_copy.groupby(exp_copy["date"].dt.strftime('%Y-%m'))["amount"].sum().reset_index()
            else:
                exp_trend = pd.DataFrame(columns=["date", "amount"])

            # Merge trends for comparison bar chart
            merged = pd.merge(inc_trend, exp_trend, on="date", how="outer").fillna(0)
            merged.columns = ["Month", "Income", "Expenses"]
            
            fig_bar = px.bar(merged, x="Month", y=["Income", "Expenses"], barmode="group",
                             color_discrete_map={"Income": "#2E7D32", "Expenses": "#FF4B4B"})
            fig_bar.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font_color="#2B3F87")
            st.plotly_chart(fig_bar, use_container_width=True, key=f"income_vs_expense_{tenant_id}")
        else:
            st.info("No payment data to chart.")

    with col_right:
        st.write("**🛡️ Portfolio Weight (Top 5)**")
        # Ensure we can isolate borrower groups correctly using standardized keys
        if "borrower" in loans.columns and not loans.empty:
            top_borrowers = loans.groupby("borrower")["principal"].sum().sort_values(ascending=False).head(5).reset_index()
            top_borrowers.columns = ["Borrower", "Total_Loaned"]
            
            fig_pie = px.pie(top_borrowers, names="Borrower", values="Total_Loaned", hole=0.5,
                             color_discrete_sequence=px.colors.sequential.GnBu_r)
            fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', font_color="#2B3F87")
            st.plotly_chart(fig_pie, use_container_width=True, key=f"portfolio_weight_{tenant_id}")
        else:
            st.info("No loan data for portfolio analysis.")

    # 6. RISK INDICATOR (PAR % Calculation matching the normalized status schema)
    st.markdown("---")
    st.subheader("🚨 Risk Assessment")
    
    # Check against upper-case strings to protect against mixed input variations
    overdue_mask = loans["status"].astype(str).str.upper().isin(["OVERDUE", "ROLLED/OVERDUE"])
    overdue_val = pd.to_numeric(loans.loc[overdue_mask, "principal"], errors="coerce").fillna(0).sum()
    
    risk_percent = (overdue_val / l_amt * 100) if l_amt > 0 else 0
    
    r1, r2 = st.columns([2, 1])
    
    with r1:
        st.write(f"Your Portfolio at Risk (PAR) is **{risk_percent:.1f}%**.")
        st.progress(min(float(risk_percent) / 100, 1.0), key=f"risk_progress_{tenant_id}")
        st.write(f"Total Overdue: **{overdue_val:,.0f} UGX**")
        
    with r2:
        if risk_percent < 10: 
            st.success("✅ Healthy Portfolio")
        elif risk_percent < 25: 
            st.warning("⚠️ Moderate Risk")
        else: 
            st.error("🆘 Critical Risk Level")
            
# ==========================================================
# 🚨 OVERDUE TRACKER & ACTIVITY CALENDAR ENGINE (WITH NAMES)
# ==========================================================
import streamlit as st
import pandas as pd
from datetime import datetime

def show_overdue_tracker(tenant_id: str):
    """
    Collections & Overdue Tracker (The Master Engine)
    Multi-tenant version ensuring isolated data fetching, processing, and persistence.
    """
    # Safeguard: Ensure a valid tenant context exists before execution
    if not tenant_id:
        st.error("❌ Access Denied: No valid tenant context detected.")
        return

    st.markdown("### 🚨 Loan Overdue & Rollover Tracker")

    # 1. --- THE AUTO-REFILL GATEKEEPER ---
    if st.button("🔄 Refresh Data from Sheets", use_container_width=True):
        with st.spinner("🧹 Clearing cache and re-syncing..."):
            st.cache_data.clear() 
            st.session_state.loans = get_cached_data("loans", tenant_id=tenant_id)
            st.session_state.ledger = get_cached_data("Ledger", tenant_id=tenant_id)
            st.rerun()

    # --- STEP 1: LOAD ALL NECESSARY DATA UPFRONT (INDENTED) ---
    # Attempt to pull from session state; if empty or missing, explicitly fetch using tenant context
    loans = st.session_state.get("loans", pd.DataFrame())
    if loans.empty:
        loans = get_cached_data("loans", tenant_id=tenant_id)
        if loans is not None and not loans.empty:
            st.session_state.loans = loans
        else:
            loans = pd.DataFrame()

    ledger = st.session_state.get("ledger", pd.DataFrame())
    if ledger.empty:
        ledger = get_cached_data("Ledger", tenant_id=tenant_id)
        if ledger is not None and not ledger.empty:
            st.session_state.ledger = ledger
        else:
            ledger = pd.DataFrame()

    today = datetime.now()

    # We pre-calculate overdue_df so it's ready for the button click
    overdue_df = pd.DataFrame()
    if not loans.empty:
        temp_loans = loans.copy()
        temp_loans.columns = temp_loans.columns.str.strip().str.lower().str.replace(" ", "_")
        temp_loans['end_date'] = pd.to_datetime(temp_loans['end_date'], errors='coerce')
        
        # Checking against uppercase status constants to ensure pipeline sync
        active_overdue_statuses = ["ACTIVE", "OVERDUE", "ROLLED/OVERDUE"]
        overdue_df = temp_loans[
            (temp_loans['status'].astype(str).str.upper().isin(active_overdue_statuses)) & 
            (temp_loans['end_date'] < today)
        ].copy()

    # 2. --- PREP WORKING DATA ---
    if loans.empty:
        st.info("💡 No active loan records found. The system is currently clear!")
        return

    loans_work = loans.copy()
    loans_work.columns = loans_work.columns.str.strip().str.lower().str.replace(" ", "_")

    # 7. --- PREP LEDGER BALANCES ---
    latest_ledger = pd.DataFrame()
    if not ledger.empty:
        ledger_work = ledger.copy()
        ledger_work.columns = ledger_work.columns.str.strip().str.lower().str.replace(" ", "_")
        if "loan_id" in ledger_work.columns:
            ledger_work['date'] = pd.to_datetime(ledger_work.get('date'), errors='coerce')
            latest_ledger = ledger_work.sort_values('date').groupby("loan_id").tail(1)

    # 8. --- ROLLOVER BUTTON (The History-Building Engine) ---
    st.markdown("---") 
    if st.button("🔄 Execute Monthly Rollover (Compound All)", use_container_width=True):
        updated_df = loans_work.copy() 
        new_rows_list = []
        count = 0
        
        try: 
            # FORCE NUMERIC: This kills the "stubborn balance" issue
            money_cols = ['principal', 'interest', 'balance', 'total_repayable', 'amount_paid']
            for col in money_cols:
                if col in updated_df.columns:
                    updated_df[col] = pd.to_numeric(updated_df[col], errors='coerce').fillna(0)

            # Targets: Find active 'PENDING' rows or Fallback to Overdue Dataframe
            targets = updated_df[updated_df['status'].astype(str).str.upper() == "PENDING"].copy() if not updated_df.empty else pd.DataFrame()
            if targets.empty:
                targets = overdue_df.copy()

            if targets.empty:
                st.info("No loans currently require a rollover cycle.")
            else:
                for i, r in targets.iterrows():
                    if i in updated_df.index:
                        # 1. Archive the old row
                        updated_df.at[i, 'status'] = "BCF"

                        # 2. THE ULTIMATE MATH FIX
                        old_p = float(r.get('principal', 0))
                        old_i = float(r.get('interest', 0))
                        
                        # New Basis = (Old P + Old I)
                        new_basis = old_p + old_i
                        # New Interest = 3% of new basis
                        new_month_interest = new_basis * 0.03
                        # Final Balance
                        compounded_balance = new_basis + new_month_interest
                        
                        # Date Math
                        orig_end = pd.to_datetime(r['end_date'], errors='coerce')
                        new_start = orig_end if pd.notna(orig_end) else datetime.now()
                        new_end = new_start + pd.offsets.DateOffset(months=1)

                        # 3. Create New Cycle Row
                        new_row = r.copy()
                        new_row['start_date'] = new_start.strftime('%Y-%m-%d')
                        new_row['end_date'] = new_end.strftime('%Y-%m-%d')
                        new_row['principal'] = new_basis
                        new_row['interest'] = new_month_interest
                        new_row['balance'] = compounded_balance 
                        new_row['total_repayable'] = compounded_balance
                        new_row['amount_paid'] = 0
                        new_row['status'] = "PENDING" 
                        new_row['balance_b/f'] = new_basis 
                        
                        new_rows_list.append(new_row)
                        count += 1

                if new_rows_list:
                    new_entries_df = pd.DataFrame(new_rows_list)
                    combined_df = pd.concat([updated_df, new_entries_df], ignore_index=True)
                    id_col = 'loan_id'
                    updated_df = combined_df.sort_values(by=[id_col, 'start_date'], ascending=[True, True])

                # 6. --- THE CORRECTED SAVE BLOCK ---
                # Re-space column headers to match the physical schema before save executions
                save_ready_df = updated_df.fillna(0).copy()
                save_ready_df.columns = [col.replace("_", " ").title() for col in save_ready_df.columns]
                
                # Dynamic adjustment check to make sure Loan ID column keeps absolute precision naming
                save_ready_df.columns = [col if col != "Loan Id" else "Loan ID" for col in save_ready_df.columns]
                
                # Critical: Save back using the isolated tenant context
                if save_data("loans", save_ready_df, tenant_id=tenant_id):
                    st.success(f"✅ Compounding Successful! Added {count} rows.")
                    st.cache_data.clear() 
                    st.session_state.loans = get_cached_data("loans", tenant_id=tenant_id)
                    st.rerun()
        except Exception as e:
            st.error(f"🚨 Rollover Error: {str(e)}")

    # 9. --- TABLE DISPLAY (Branded & Formatted) ---
    def style_status_colors(s):
        val = str(s).upper()
        if val == "BCF": return "background-color: #FFA500; color: white;"      # Orange
        if val == "PENDING": return "background-color: #D32F2F; color: white;"  # Red
        if val in ["CLOSED", "CLEARED"]: return "background-color: #2E7D32; color: white;" # Green
        return ""

    st.markdown("### 🏦 All Loan Records")
    
    try:
        display_df = st.session_state.get("loans", loans).copy()
        display_df.columns = display_df.columns.str.strip().str.lower().str.replace(" ", "_")

        # Push status to the end for Luxe view
        if 'status' in display_df.columns:
            cols = [c for c in display_df.columns if c != 'status'] + ['status']
            display_df = display_df[cols]

        fmt_dict = {
            "principal": "{:,.0f}", "balance": "{:,.0f}", "interest": "{:,.0f}",
            "total_repayable": "{:,.0f}", "amount_paid": "{:,.0f}", "balance_b/f": "{:,.0f}"
        }
        actual_fmt = {k: v for k, v in fmt_dict.items() if k in display_df.columns}

        styled_df = display_df.style.map(style_status_colors, subset=['status']).format(actual_fmt)
        st.dataframe(styled_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Display Error: {str(e)}")
        st.dataframe(loans, use_container_width=True, hide_index=True)

import streamlit as st
import pandas as pd
from datetime import datetime

# ==============================
# 17. ACTIVITY CALENDAR PAGE
# ==============================
def show_calendar():
    """
    Activity Calendar View.
    Calculates operational workloads and shows visual metrics isolated by tenant_id.
    """
    # Pull the active tenant context directly inside the view function body to satisfy the router
    tenant_id = get_current_tenant() if 'get_current_tenant' in globals() else get_tenant_id()

    # Safeguard: Ensure a valid tenant context exists before loading page resources
    if not tenant_id:
        st.error("❌ Access Denied: No valid tenant context detected.")
        return
    
    st.markdown("<h2 style='color: #2B3F87;'>📅 Activity Calendar</h2>", unsafe_allow_html=True)

    # 1. LOAD DATA SPECIFIC TO ACTIVE TENANT
    loans_df = get_cached_data("loans", tenant_id=tenant_id)

    if loans_df is None or loans_df.empty:
        st.info("📅 Calendar is clear! No active loans to track.")
        return

    # Translate column headers immediately to clean lower snake case
    loans_df.columns = loans_df.columns.str.strip().str.lower().str.replace(" ", "_")

    required_keys = ["end_date", "total_repayable", "status", "borrower", "loan_id", "principal", "interest"]
    for col in required_keys:
        if col not in loans_df.columns:
            loans_df[col] = 0 if col in ["total_repayable", "principal", "interest"] else "Unknown"
            
    # Convert to proper types for logic
    loans_df["end_date"] = pd.to_datetime(loans_df["end_date"], errors="coerce")
    loans_df["total_repayable"] = pd.to_numeric(loans_df["total_repayable"], errors="coerce").fillna(0)
    loans_df["principal"] = pd.to_numeric(loans_df["principal"], errors="coerce").fillna(0)
    loans_df["interest"] = pd.to_numeric(loans_df["interest"], errors="coerce").fillna(0)
    
    # Reference date context
    today = pd.Timestamp.today().normalize()
    
    # Filter for loans that aren't closed, cleared, or archived (BCF) matching core status patterns
    inactive_statuses = ["CLOSED", "CLEARED", "BCF"]
    active_loans = loans_df[~loans_df["status"].astype(str).str.upper().isin(inactive_statuses)].copy()

    # --- VISUAL CALENDAR WIDGET ---
    calendar_events = []
    for _, r in active_loans.iterrows():
        if pd.notna(r['end_date']):
            # Color logic: Red for overdue, Blue for upcoming
            is_overdue = r['end_date'].date() < today.date()
            ev_color = "#FF4B4B" if is_overdue else "#4A90E2"
            
            # Auto-Recovery for display amount if total_repayable is zero
            disp_amt = float(r['total_repayable']) if r['total_repayable'] > 0 else (float(r['principal']) + float(r['interest']))
            
            calendar_events.append({
                "title": f"UGX {disp_amt:,.0f} - {r['borrower']}",
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

    # Render the interactive calendar using the current tenant's distinct tracking state key
    calendar(events=calendar_events, options=calendar_options, key=f"collection_cal_{tenant_id}")
    
    st.markdown("---")

    # 3. DAILY WORKLOAD METRICS (Zoe Branded Cards)
    due_today_df = active_loans[active_loans["end_date"].dt.date == today.date()]
    upcoming_df = active_loans[
        (active_loans["end_date"] > today) & 
        (active_loans["end_date"] <= today + pd.Timedelta(days=7))
    ]
    overdue_count = active_loans[active_loans["end_date"] < today].shape[0]

    # Create the columns
    m1, m2, m3 = st.columns(3)
    
    m1.markdown(f"""
    <div style="background-color: #ffffff; padding: 20px; border-radius: 15px; border-left: 5px solid #2B3F87; box-shadow: 2px 2px 10px rgba(0,0,0,0.05);">
        <p style="margin:0; font-size:12px; color:#666; font-weight:bold;">DUE TODAY |</p>
        <p style="margin:0; font-size:18px; color:#2B3F87; font-weight:bold;">{len(due_today_df)} Accounts</p>
    </div>
    """, unsafe_allow_html=True)

    m2.markdown(f"""
    <div style="background-color: #F0F8FF; padding: 20px; border-radius: 15px; border-left: 5px solid #2B3F87; box-shadow: 2px 2px 10px rgba(0,0,0,0.05);">
        <p style="margin:0; font-size:12px; color:#666; font-weight:bold;">UPCOMING (7 DAYS) |</p>
        <p style="margin:0; font-size:18px; color:#2B3F87; font-weight:bold;">{len(upcoming_df)} Accounts</p>
    </div>
    """, unsafe_allow_html=True)

    m3.markdown(f"""
    <div style="background-color: #FFF5F5; padding: 20px; border-radius: 15px; border-left: 5px solid #D32F2F; box-shadow: 2px 2px 10px rgba(0,0,0,0.05);">
        <p style="margin:0; font-size:12px; color:#D32F2F; font-weight:bold;">TOTAL OVERDUE |</p>
        <p style="margin:0; font-size:18px; color:#D32F2F; font-weight:bold;">{overdue_count} Accounts</p>
    </div>
    """, unsafe_allow_html=True)

    # --- CALENDAR FOOTER: REVENUE PREVIEW ---
    st.markdown("---")
    st.markdown("<h4 style='color: #2B3F87;'>📊 Revenue Forecast (This Month)</h4>", unsafe_allow_html=True)
    
    current_month = today.month
    this_month_df = active_loans[active_loans["end_date"].dt.month == current_month]
    total_expected = this_month_df["total_repayable"].sum()
    
    f1, f2 = st.columns(2)
    f1.metric("Expected Collections", f"{total_expected:,.0f} UGX")
    f2.metric("Remaining Appointments", len(this_month_df))
    
    st.write("💡 *Tip: Click any blue/red bar on the calendar above to see the specific borrower details.*")

    # --- SECTION: DUE TODAY ---
    st.markdown("<h4 style='color: #2B3F87;'>📌 Action Items for Today</h4>", unsafe_allow_html=True)
    if due_today_df.empty:
        st.success("✨ No deadlines for today. Focus on follow-ups!")
    else:
        today_rows = ""
        for i, r in due_today_df.iterrows():
            bg = "#F0F8FF" if i % 2 == 0 else "#FFFFFF"
            today_rows += f"""<tr style="background-color: {bg}; border-bottom: 1px solid #ddd;"><td style="padding:10px;"><b>#{r['loan_id']}</b></td><td style="padding:10px;">{r['borrower']}</td><td style="padding:10px; text-align:right; font-weight:bold; color:#2B3F87;">{r['total_repayable']:,.0f}</td><td style="padding:10px; text-align:center;"><span style="background:#2B3F87; color:white; padding:2px 8px; border-radius:10px; font-size:10px;">💰 COLLECT NOW</span></td></tr>"""
        st.markdown(f"""<div style="border:2px solid #2B3F87; border-radius:10px; overflow:hidden;"><table style="width:100%; border-collapse:collapse; font-family:sans-serif; font-size:12px;"><tr style="background:#2B3F87; color:white;"><th style="padding:10px;">Loan ID</th><th style="padding:10px;">Borrower</th><th style="padding:10px; text-align:right;">Amount Due</th><th style="padding:10px; text-align:center;">Action</th></tr>{today_rows}</table></div>""", unsafe_allow_html=True)

    # --- SECTION: UPCOMING ---
    st.markdown("<br><h4 style='color: #2B3F87;'>⏳ Upcoming Deadlines (Next 7 Days)</h4>", unsafe_allow_html=True)
    if upcoming_df.empty:
        st.info("The next few days look quiet.")
    else:
        upcoming_display = upcoming_df.sort_values("end_date").copy()
        up_rows = ""
        for i, r in upcoming_display.iterrows():
            bg = "#F0F8FF" if i % 2 == 0 else "#FFFFFF"
            display_amt = float(r.get('total_repayable', 0)) or (float(r.get('principal', 0)) + float(r.get('interest', 0)))
            up_rows += f"""<tr style="background-color: {bg};"><td style="padding:10px; color:#2B3F87; font-weight:bold;">{r['end_date'].strftime('%d %b (%a)')}</td><td style="padding:10px;">{r.get('borrower', 'Unknown')}</td><td style="padding:10px; text-align:right; font-weight:bold;">{display_amt:,.0f} UGX</td><td style="padding:10px; text-align:right; color:#666;">ID: #{r.get('loan_id', 'N/A')}</td></tr>"""
        st.markdown(f"""<div style="border:1px solid #2B3F87; border-radius:10px; overflow:hidden;"><table style="width:100%; border-collapse:collapse; font-family:sans-serif; font-size:12px;"><tr style="background:#2B3F87; color:white;"><th style="padding:10px;">Due Date</th><th style="padding:10px;">Borrower</th><th style="padding:10px; text-align:right;">Amount</th><th style="padding:10px; text-align:right;">Ref</th></tr>{up_rows}</table></div>""", unsafe_allow_html=True)

    # --- SECTION: IMMEDIATE FOLLOW-UP ---
    st.markdown("<br><h4 style='color: #FF4B4B;'>🔴 Past Due (Immediate Attention)</h4>", unsafe_allow_html=True)
    overdue_df = active_loans[active_loans["end_date"] < today].copy()
    if overdue_df.empty:
        st.success("Clean Sheet! No overdue loans found. 🎉")
    else:
        overdue_df["days_late"] = (today - overdue_df["end_date"]).dt.days
        overdue_df = overdue_df.sort_values("days_late", ascending=False)
        od_rows = ""
        for i, r in overdue_df.iterrows():
            bg = "#FFF5F5"
            late_color = "#FF4B4B" if r['days_late'] > 7 else "#FFA500"
            od_rows += f"""<tr style="background-color: {bg}; border-bottom: 1px solid #FFDADA;"><td style="padding:10px;"><b>#{r['loan_id']}</b></td><td style="padding:10px;">{r['borrower']}</td><td style="padding:10px; text-align:center; font-weight:bold; color:{late_color};">{r['days_late']} Days</td><td style="padding:10px; text-align:center;"><span style="background:{late_color}; color:white; padding:2px 8px; border-radius:10px; font-size:10px;">{r['status']}</span></td></tr>"""
        st.markdown(f"""<div style="border:2px solid #FF4B4B; border-radius:10px; overflow:hidden;"><table style="width:100%; border-collapse:collapse; font-family:sans-serif; font-size:12px;"><tr style="background:#FF4B4B; color:white;"><th style="padding:10px;">Loan ID</th><th style="padding:10px;">Borrower</th><th style="padding:10px; text-align:center;">Late By</th><th style="padding:10px; text-align:center;">Status</th></tr>{od_rows}</table></div>""", unsafe_allow_html=True)

# ==========================================================
# 🛡️ COLLATERAL MANAGEMENT ENGINE
# ==========================================================
import streamlit as st
import pandas as pd
from datetime import datetime

def show_collateral():
    """
    Manages asset security, verification tracking, and real-time status management
    while respecting multi-tenant boundaries.
    """
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    st.markdown(f"<h2 style='color: {brand_color}; margin-bottom: 20px;'>🛡️ Collateral & Security Assets</h2>", unsafe_allow_html=True)

    # ==============================
    # 📦 DATA FETCH & UNIFICATION
    # ==============================
    collateral_df = get_data("collateral") 
    loans_df = get_data("loans")
    borrowers_df = get_data("borrowers")  # Added to ensure fallback relational mapping works safely

    # Protect against missing data collections
    if loans_df is None: loans_df = pd.DataFrame()
    if collateral_df is None: collateral_df = pd.DataFrame()
    if borrowers_df is None: borrowers_df = pd.DataFrame()

    # Tenant Isolation Filter
    if not loans_df.empty and "tenant_id" in loans_df.columns:
        loans_df = loans_df[loans_df["tenant_id"].astype(str) == str(current_tenant)].copy()
    if not collateral_df.empty and "tenant_id" in collateral_df.columns:
        collateral_df = collateral_df[collateral_df["tenant_id"].astype(str) == str(current_tenant)].copy()

    # --- RELATIONAL BORROWER NAME LOOKUP ---
    # Safely creates name mappings without breaking case conventions of the tables
    name_map = {}
    if not borrowers_df.empty:
        b_id_col = next((c for c in borrowers_df.columns if c.lower() in ['id', 'borrower_id']), None)
        b_nm_col = next((c for c in borrowers_df.columns if c.lower() in ['name', 'borrower_name', 'client_name']), None)
        if b_id_col and b_nm_col:
            name_map = dict(zip(borrowers_df[b_id_col].astype(str), borrowers_df[b_nm_col]))

    # Add descriptive borrower labels inline to loans dataframe safely
    if not loans_df.empty:
        loans_df['id_str'] = loans_df['id'].astype(str)
        if 'borrower_id' in loans_df.columns:
            loans_df['borrower_name_resolved'] = loans_df['borrower_id'].astype(str).map(name_map)
        else:
            # Fallback inline string lookup if tables are denormalized
            match_col = next((c for c in loans_df.columns if c.lower() in ['borrower', 'client', 'borrower_name']), None)
            loans_df['borrower_name_resolved'] = loans_df[match_col] if match_col else "Unknown Borrower"
        
        loans_df['borrower_name_resolved'] = loans_df['borrower_name_resolved'].fillna("Unknown Borrower")
        
        # Filter down to operational loans matching dynamic context
        status_col = next((c for c in loans_df.columns if c.lower() == 'status'), None)
        if status_col:
            active_statuses = ["ACTIVE", "OVERDUE", "PENDING", "BCF"]
            available_loans = loans_df[loans_df[status_col].astype(str).str.upper().isin(active_statuses)].copy()
        else:
            available_loans = loans_df.copy()
    else:
        available_loans = pd.DataFrame()

    # --- TABS CONTAINER ---
    tab_reg, tab_view = st.tabs(["➕ Register Asset", "📋 Inventory & status"])

    # ==============================
    # ➕ TAB 1: REGISTER ASSET
    # ==============================
    with tab_reg:
        if available_loans.empty:
            st.info("ℹ️ No active context loans found to connect security collateral onto.")
        else:
            with st.form("collateral_reg_form", clear_on_submit=True):
                st.write("### Link Asset to Loan File")
                c1, c2 = st.columns(2)

                # Generate clean dropdown tracking options
                loan_map = {}
                for _, row in available_loans.iterrows():
                    loan_id = str(row['id'])
                    b_name = str(row['borrower_name_resolved'])
                    if b_name.lower() in ['nan', 'none', '']: b_name = "Unknown Customer"
                    
                    ref_label = row.get('loan_id_label', loan_id[:8])
                    principal_amt = row.get('principal', row.get('amount', 0))
                    try:
                        amt_fmt = f"UGX {float(principal_amt):,.0f}"
                    except Exception:
                        amt_fmt = f"UGX {principal_amt}"

                    loan_map[loan_id] = f"{b_name} | {amt_fmt} (Ref: {ref_label})"

                selected_loan_id = c1.selectbox(
                    "Select Target Loan Profile",
                    options=list(loan_map.keys()),
                    format_func=lambda x: loan_map.get(x, "Select Profile")
                )

                asset_type = c2.selectbox(
                    "Asset Classification Type",
                    ["Logbook (Car)", "Land Title", "Electronics", "House Deed", "Business Stock", "Other"]
                )

                desc = st.text_input("Detailed Description (Asset Serial ID, Plate Number, Plot Location)")
                est_value = st.number_input("Estimated Asset Market Value (UGX)", min_value=0, step=100000)
                
                st.markdown("<br>", unsafe_allow_html=True)
                uploaded_photo = st.file_uploader("Upload Verification Documentation/Photo Asset", type=["jpg", "png", "jpeg"])

                submit_save = st.form_submit_button("💾 Save Asset Registration Record", use_container_width=True)

            if submit_save:
                if not desc or est_value <= 0:
                    st.error("❌ Form Incomplete. Provide accurate structural evaluations and descriptions.")
                else:
                    try:
                        # Safely compute borrower back-reference assignment details
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
                            st.success(f"✅ Asset registry successfully locked down for {borrower_for_db}!")
                            st.cache_data.clear()
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Security transaction failed to register: {e}")

    # ==============================
    # 📋 TAB 2: INVENTORY & LEDGER
    # ==============================
    with tab_view:
        if collateral_df.empty:
            st.info("💡 No securitized collateral structures found inside this workspace index.")
        else:
            # Clean and normalize asset value matrices safely
            val_col = next((c for c in collateral_df.columns if c.lower() == 'value'), 'value')
            collateral_df[val_col] = pd.to_numeric(collateral_df[val_col], errors="coerce").fillna(0)
            
            stat_col = next((c for c in collateral_df.columns if c.lower() == 'status'), 'status')
            
            # --- KPI DASHBOARD CARDS ---
            total_value = collateral_df[val_col].sum()
            held_count = len(collateral_df[collateral_df[stat_col].astype(str).str.upper() == "IN CUSTODY"])

            m1, m2 = st.columns(2)
            m1.metric("Securitized Inventory Value", f"UGX {total_value:,.0f}")
            m2.metric("Active Assets in Custody", held_count)
            st.markdown("---")

            # --- DYNAMIC FILTERS LAYER ---
            col1, col2 = st.columns(2)
            unique_statuses = sorted(collateral_df[stat_col].dropna().unique().tolist())
            status_filter = col1.selectbox("Filter Inventory status", ["All Asset Records"] + unique_statuses)
            search_query = col2.text_input("🔍 Search Keyword / Borrower Reference").lower()

            df_filtered = collateral_df.copy()
            if status_filter != "All Asset Records":
                df_filtered = df_filtered[df_filtered[stat_col] == status_filter]

            if search_query:
                b_match = next((c for c in df_filtered.columns if c.lower() == 'borrower'), 'borrower')
                d_match = next((c for c in df_filtered.columns if c.lower() == 'description'), 'description')
                
                df_filtered = df_filtered[
                    df_filtered[b_match].astype(str).str.lower().str.contains(search_query, na=False) |
                    df_filtered[d_match].astype(str).str.lower().str.contains(search_query, na=False)
                ]

            # --- RENDER UNIFORM JAVASCRIPT DATAFRAME ---
            st.markdown("### Securitized Assets Ledger")
            
            display_ledger = df_filtered.copy()
            # Remap columns neatly to standard clean headings
            col_renames = {
                "date_added": "date Registered",
                "borrower": "Borrower Profile",
                "type": "Asset Type",
                "description": "Asset Description",
                "value": "Value (UGX)",
                "status": "Tracking status"
            }
            # Handle minor schema variances gracefully
            display_ledger = display_ledger.rename(columns={k: v for k, v in col_renames.items() if k in display_ledger.columns})
            
            # Formats display currencies seamlessly 
            tgt_val_col = "Value (UGX)"
            if tgt_val_col in display_ledger.columns:
                display_ledger[tgt_val_col] = display_ledger[tgt_val_col].apply(lambda x: f"{float(x):,.0f}" if pd.notna(x) else "0")

            cols_to_show = [v for v in col_renames.values() if v in display_ledger.columns]
            
            st.dataframe(
                display_ledger[cols_to_show],
                use_container_width=True,
                hide_index=True
            )
            st.markdown("---")

            # ==============================
            # ⚙️ SECURE LIFECYCLE MANAGEMENT
            # ==============================
            st.markdown("### 🛠️ Strategic Asset Inspection & Audits")
            
            if df_filtered.empty:
                st.warning("No records match configuration queries.")
            else:
                b_lbl = next((c for c in df_filtered.columns if c.lower() == 'borrower'), 'borrower')
                d_lbl = next((c for c in df_filtered.columns if c.lower() == 'description'), 'description')
                
                df_filtered["ui_label"] = df_filtered[b_lbl].astype(str) + " — " + df_filtered[d_lbl].astype(str)
                selected_label = st.selectbox("Choose Target Asset Inventory File", df_filtered["ui_label"].tolist())

                selected_row = df_filtered[df_filtered["ui_label"] == selected_label].iloc[0]
                asset_id = selected_row["id"]

                # --- PHOTO EVIDENCE STORAGE ---
                st.markdown("#### 📸 Physical Asset Evidence")
                asset_photo = selected_row.get("photo", selected_row.get("image_url", None))
                if asset_photo and pd.notna(asset_photo):
                    st.image(asset_photo, caption=str(selected_row[d_lbl]), use_container_width=True)
                else:
                    st.info("No visual photo documentation uploaded against this asset record.")
                
                st.markdown("<br>", unsafe_allow_html=True)

                # --- LIFECYCLE OPERATION PANEL ---
                st.markdown("#### 🔄 Asset State Custody Release")
                status_options = ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"]
                
                # Fixed layout alignment split to prevent text wrapping on operational buttons
                col_stat, col_btn = st.columns([2, 1])
                
                current_status_val = str(selected_row[stat_col])
                default_idx = status_options.index(current_status_val) if current_status_val in status_options else 0

                new_status = col_stat.selectbox(
                    "Transition status State",
                    status_options,
                    index=default_idx,
                    label_visibility="collapsed"
                )

                # Standard button layout prevents regressions and aligns cleanly
                if col_btn.button("Update Asset State", use_container_width=True):
                    update_row = pd.DataFrame([{
                        "id": asset_id,
                        "status": new_status,
                        "tenant_id": str(current_tenant)
                    }])

                    if save_data_saas("collateral", update_row):
                        st.success(f"✅ Custody updated to '{new_status}' successfully!")
                        st.cache_data.clear()
                        st.rerun()
import streamlit as st
import pandas as pd
import uuid
import plotly.express as px
from datetime import datetime

def show_expenses():
    """
    Manages internal operational company costs, budgeting lifecycles, 
    and multi-tenant spending trends.
    """
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    st.markdown(f"<h2 style='color: {brand_color}; margin-bottom: 20px;'>📁 Expense Management</h2>", unsafe_allow_html=True)

    def get_fy_label(date_val):
        try:
            dt = pd.to_datetime(date_val)
            if pd.isna(dt): return "Unknown FY"
            return f"FY{dt.year}-{dt.year+1}" if dt.month >= 7 else f"FY{dt.year-1}-{dt.year}"
        except:
            return "Unknown FY"

    # ==============================
    # 📥 DATA INGESTION & BOUNDARY LAYERS
    # ==============================
    try:
        raw_all_df = get_cached_data("expenses")
    except:
        raw_all_df = pd.DataFrame()

    if raw_all_df is not None and not raw_all_df.empty:
        raw_all_df.columns = raw_all_df.columns.str.lower().str.strip()
        raw_all_df["id"] = raw_all_df["id"].astype(str)
        raw_all_df["amount"] = pd.to_numeric(raw_all_df["amount"], errors="coerce").fillna(0.0)
        
        if "tenant_id" in raw_all_df.columns:
            df = raw_all_df[raw_all_df["tenant_id"].astype(str) == str(current_tenant)].copy()
        else:
            df = raw_all_df.copy()
            
        if not df.empty and "payment_date" in df.columns:
            df["financial_year"] = df["payment_date"].apply(get_fy_label)
        else:
            df["financial_year"] = "Unknown FY"
    else:
        df = pd.DataFrame(columns=["id","category","amount","date","description","payment_date","receipt_no","tenant_id","financial_year"])
        raw_all_df = df.copy()

    EXPENSE_CATS = ["Rent","Insurance","Utilities","Salaries","Licence expenses","Marketing","Office expenses","Operating expenses","Fuel and Motor Vehicle","Taxes","Corporate Social Responsibilities","Other"]

    tab_add, tab_view, tab_manage = st.tabs([
        "➕ Record Expense", "📊 Spending Analysis", "⚙️ Manage Records"
    ])

    # ==============================
    # ➕ TAB 1: RECORD EXPENSE
    # ==============================
    with tab_add:
        with st.form("add_expense_form", clear_on_submit=True):
            st.write("### Log Operational Cost")
            c1, c2 = st.columns(2)

            category = c1.selectbox("Expense Category", EXPENSE_CATS)
            amount = c2.number_input("Amount (UGX)", min_value=0.0, step=10000.0)  # Typed to floats
            desc = st.text_input("Transaction Description / Payee")

            c3, c4 = st.columns(2)
            p_date = c3.date_input("Payment date", value=datetime.now())
            receipt_no = c4.text_input("Receipt / Invoice Number Reference")

            submit_new = st.form_submit_button("🚀 Save Expense Record", use_container_width=True)

            if submit_new:
                if amount > 0 and desc:
                    try:
                        formatted_d = p_date.strftime("%Y-%m-%d")

                        new_record = pd.DataFrame([{
                            "id": str(uuid.uuid4()),
                            "category": category,
                            "amount": float(amount),
                            "date": formatted_d,
                            "description": desc,
                            "payment_date": formatted_d,
                            "receipt_no": receipt_no,
                            "tenant_id": str(current_tenant)
                        }])

                        updated_global_df = pd.concat([raw_all_df, new_record], ignore_index=True)
                        if "financial_year" in updated_global_df.columns:
                            updated_global_df = updated_global_df.drop(columns=["financial_year"], errors="ignore")

                        if save_data("expenses", updated_global_df):
                            st.success(f"✅ Expense logged successfully for {formatted_d}!")
                            st.cache_data.clear()
                            st.rerun()
                    except Exception as e:
                        st.error(f"🚨 Operational save failed: {e}")
                else:
                    st.warning("⚠️ Valid execution amount and asset descriptions are required.")

    # ==============================
    # 📊 TAB 2: SPENDING ANALYSIS
    # ==============================
    with tab_view:
        if df.empty:
            st.info("💡 No transactional outlays registered yet inside this profile.")
        else:
            fys = sorted(df["financial_year"].unique(), reverse=True)
            fy = st.selectbox("📅 Structural Scope Window (Financial Year)", ["All Time Target"] + fys)

            view_df = df if fy == "All Time Target" else df[df["financial_year"] == fy]
            total_outflow = view_df["amount"].sum()

            st.markdown(f"""
            <div style="background-color:white; padding:20px; border-radius:12px; border-left:6px solid #FF4B4B; box-shadow:0 2px 8px rgba(0,0,0,0.04); margin-bottom: 20px;">
                <p style="margin:0; font-size:11px; color:#6B7280; font-weight:600; text-transform:uppercase;">Aggregate Operational Outflow ({fy})</p>
                <h2 style="margin:5px 0 0 0; color:#1F2937; font-weight:700;">UGX {total_outflow:,.0f}</h2>
            </div>
            """, unsafe_allow_html=True)

            col1, col2 = st.columns([2, 1])
            with col1:
                chart_data = view_df.groupby("category")["amount"].sum().reset_index()
                fig = px.pie(
                    chart_data, names="category", values="amount",
                    hole=0.42, title="Cost Contribution Share",
                    color_discrete_sequence=px.colors.qualitative.Safe
                )
                fig.update_layout(margin=dict(t=40, b=10, l=10, r=10), legend=dict(orientation="h", y=-0.1))
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Annual Trajectory")
                fy_summary = df.groupby("financial_year")["amount"].sum().reset_index().rename(
                    columns={"financial_year": "Fin Year", "amount": "Total (UGX)"}
                )
                fy_summary["Total (UGX)"] = fy_summary["Total (UGX)"].apply(lambda x: f"{x:,.0f}")
                st.dataframe(fy_summary, use_container_width=True, hide_index=True)

            st.markdown("### 📋 Expense Ledger Records")
            
            filter_c1, filter_c2 = st.columns(2)
            cat_options = ["All Categories"] + sorted(view_df["category"].dropna().unique().tolist())
            selected_cat = filter_c1.selectbox("Category Filter Selection", cat_options)

            min_val = float(view_df["amount"].min()) if not view_df.empty else 0.0
            max_val = float(view_df["amount"].max()) if not view_df.empty else 100000.0
            if min_val == max_val: max_val += 1.0
            
            amt_range = filter_c2.slider("Transaction Cost Range Threshold", min_val, max_val, (min_val, max_val))

            processed_df = view_df.copy()
            if selected_cat != "All Categories":
                processed_df = processed_df[processed_df["category"] == selected_cat]
            
            processed_df = processed_df[
                (processed_df["amount"] >= amt_range[0]) & 
                (processed_df["amount"] <= amt_range[1])
            ]

            if processed_df.empty:
                st.warning("No records align with current query filters.")
            else:
                processed_df = processed_df.sort_values("payment_date", ascending=False)
                display_ledger = processed_df[["payment_date", "category", "description", "amount", "receipt_no"]].copy()
                display_ledger.columns = ["date Paid", "Category Grouping", "Description Detail", "Amount (UGX)", "Ref / Invoice #"]
                display_ledger["Amount (UGX)"] = display_ledger["Amount (UGX)"].apply(lambda x: f"{x:,.0f}")
                
                st.dataframe(
                    display_ledger,
                    use_container_width=True,
                    hide_index=True
                )

    # ==============================
    # ⚙️ TAB 3: MANAGE RECORDS
    # ==============================
    with tab_manage:
        st.markdown("### 🛠️ Record Maintenance Engine")

        if df.empty:
            st.info("No active expense records discovered to modify.")
        else:
            df["selector_label"] = df.apply(
                lambda r: f"{r['payment_date']} | {r['category']} | UGX {r['amount']:,.0f} — {r['description'][:15]}...", axis=1
            )

            record_map = {row["selector_label"]: row for _, row in df.iterrows()}
            selected_label = st.selectbox("Choose Target Transaction Log Entry", list(record_map.keys()))

            if selected_label:
                target_record = record_map[selected_label]
                
                with st.form("edit_expense_form"):
                    st.write(f"**Modifying Reference Key**: `{target_record['id'][:8]}`")
                    
                    # 🛡️ THE FIX: Match float value type with an explicit float step representation
                    new_amt = st.number_input("Update Value Allocation (UGX)", value=float(target_record['amount']), step=5000.0)
                    new_desc = st.text_input("Modify Description Label Details", value=target_record['description'])
                    
                    mod_c1, mod_c2 = st.columns(2)
                    save_btn = mod_c1.form_submit_button("💾 Save Matrix Modifications", use_container_width=True)
                    delete_btn = mod_c2.form_submit_button("🗑️ Purge Log Record", use_container_width=True)

                    if save_btn:
                        raw_all_df.loc[raw_all_df["id"] == target_record["id"], ["amount", "description"]] = [float(new_amt), new_desc]
                        if "financial_year" in raw_all_df.columns:
                            raw_all_df = raw_all_df.drop(columns=["financial_year"], errors="ignore")

                        if save_data("expenses", raw_all_df):
                            st.success("✅ Log tracking changes updated successfully!")
                            st.cache_data.clear()
                            st.rerun()

                    if delete_btn:
                        clean_global_df = raw_all_df[raw_all_df["id"] != str(target_record["id"])].copy()
                        if "financial_year" in clean_global_df.columns:
                            clean_global_df = clean_global_df.drop(columns=["financial_year"], errors="ignore")

                        if save_data("expenses", clean_global_df):
                            st.warning("🗑️ Expense item purged from system logs.")
                            st.cache_data.clear()
                            st.rerun()


    
import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime
# Ensure proper ReportLab styling imports are active
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

def generate_pdf_statement(client_name, loans_df, payments_df):
    """
    Compiles a clean financial ledger PDF statement safely grouped by unique 
    Loan Ref ID, pulling only the true original disbursement and subsequent actions.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    styles = getSampleStyleSheet()
    
    cell_style = ParagraphStyle('LedgerCell', parent=styles['Normal'], fontSize=8, leading=10)
    cell_bold = ParagraphStyle('LedgerCellBold', parent=styles['Normal'], fontSize=8, leading=10, fontName='Helvetica-Bold')

    elements = []
    company_name = st.session_state.get('company_name', 'ZOE CONSULTS SMC LIMITED').upper()
    elements.append(Paragraph(f"<b>{company_name}</b>", styles["Title"]))
    elements.append(Paragraph(f"<b>Client Statement:</b> {client_name}", styles["Normal"]))
    elements.append(Paragraph(f"<b>Statement date:</b> {datetime.now().strftime('%d %b %Y')}", styles["Normal"]))
    elements.append(Spacer(1, 20))
    
    grand_total = 0.0
    
    def clean_id_str(val):
        if pd.isna(val): return ""
        s = str(val).strip()
        if s.endswith(".0"): s = s[:-2]
        return s

    processed_payments = payments_df.copy() if payments_df is not None else pd.DataFrame()
    if not processed_payments.empty and "loan_id" in processed_payments.columns:
        processed_payments["loan_id"] = processed_payments["loan_id"].apply(clean_id_str)

    working_loans = loans_df.copy()
    working_loans["id_clean"] = working_loans["id"].apply(clean_id_str)
    
    if "loan_id_label" not in working_loans.columns:
        working_loans["loan_id_label"] = working_loans["id_clean"]
    working_loans["loan_id_label"] = working_loans["loan_id_label"].fillna(working_loans["id_clean"]).astype(str)

    grouped_loan_labels = sorted(working_loans["loan_id_label"].unique())

    for display_id in grouped_loan_labels:
        sub_loans = working_loans[working_loans["loan_id_label"] == display_id]
        
        # 🔄 Sort by cycle or sequence key to trace the true initial entry
        seq_col = "cycle" if "cycle" in sub_loans.columns else "id"
        sub_loans = working_loans[
            working_loans["loan_id_label"] == display_id
        ].copy()
        
        date_col = (
            "start_date"
            if "start_date" in sub_loans.columns
            else "created_at"
        )
        
        sub_loans[date_col] = pd.to_datetime(
            sub_loans[date_col],
            errors="coerce"
        )
        
        sub_loans = sub_loans.sort_values(
            by=[date_col]
        )
        
        sub_loans = sub_loans.drop_duplicates(
            subset=[date_col, "balance"],
            keep="last"
        )
        
        origin_loan = sub_loans.iloc[0]
        
        elements.append(Paragraph(f"<b>Loan Account Ref: {display_id}</b>", styles["Heading3"]))
        
        data = [[
            Paragraph("<b>Cycle date</b>", cell_bold),
            Paragraph("<b>Cycle</b>", cell_bold),
            Paragraph("<b>Opening Balance</b>", cell_bold),
            Paragraph("<b>Amount Due</b>", cell_bold),
            Paragraph("<b>Amount Paid</b>", cell_bold),
            Paragraph("<b>Balance</b>", cell_bold),
        ]]
        
        for _, row in sub_loans.iterrows():
        
            cycle_date = str(
                row.get(
                    "start_date",
                    row.get("created_at", "")
                )
            )[:10]
        
            opening_balance = float(
            row.get(
                "opening_balance",
                row.get(
                    "principal",
                    0
                )
            )
        )
        
            amount_due = float(
                row.get(
                    "total_due",
                    row.get("amount_due", 0)
                )
            )
        
            amount_paid = float(
                row.get(
                    "amount_paid",
                    row.get("paid", 0)
                )
            )
        
            balance = float(
                row.get(
                    "balance",
                    0
                )
            )
        
            data.append([
                Paragraph(cycle_date, cell_style),
                Paragraph(str(row.get("cycle", "")), cell_style),
                Paragraph(f"{opening_balance:,.0f}", cell_style),
                Paragraph(f"{amount_due:,.0f}", cell_style),
                Paragraph(f"{amount_paid:,.0f}", cell_style),
                Paragraph(f"{balance:,.0f}", cell_style),
            ])
        
        final_balance = float(sub_loans.iloc[-1].get("balance", 0))
        
        grand_total += abs(final_balance)

        table = Table(
            data,
            repeatRows=1,
            colWidths=[65, 40, 85, 85, 85, 85]
        )
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#89CFF0")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        
            ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#D6EAF8")),
        
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [
                colors.white,
                colors.HexColor("#F8FCFF")
            ]),
        
            ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
        
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ]))
        elements.append(table)
        status_value = str(
            sub_loans.iloc[-1].get(
                "status",
                "OUTSTANDING"
            )
        )
        
        final_balance = abs(
            float(
                sub_loans.iloc[-1].get(
                    "balance",
                    0
                )
            )
        )
        
        if final_balance <= 1:
            status_value = "CLEARED"
        else:
            status_value = "PENDING"
        
        elements.append(
            Spacer(1, 5)
        )
        
        loan_style = ParagraphStyle(
            "LoanHeading",
            parent=styles["Heading3"],
            textColor=colors.HexColor("#89CFF0")
        )
        
        elements.append(
            Paragraph(
                f"<b>Loan Account Ref: {display_id}</b>",
                loan_style
            )
        )
        elements.append(Spacer(1, 15))
        
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(f"<b>Aggregate Outstanding Position: {grand_total:,.0f} UGX</b>", styles["Heading2"]))
    
    doc.build(elements)
    buffer.seek(0)
    return buffer

# ==============================
# MAIN LEDGER FUNCTION
# ==============================
def show_ledger():
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    st.markdown(f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
            .ledger-header {{
                font-family: 'Inter', sans-serif;
                color: {brand_color};
                font-weight: 700;
                margin-bottom: 20px;
            }}
            .snapshot-text {{
                font-family: 'Inter', sans-serif;
                font-weight: 600;
                color: #374151;
                margin-top: 15px;
            }}
        </style>
        <h2 class='ledger-header'>📘 Master Ledger Accounts</h2>
    """, unsafe_allow_html=True)

    raw_loans = get_cached_data("loans")
    raw_payments = get_cached_data("payments")
    raw_borrowers = get_cached_data("borrowers")

    if raw_loans is None or raw_loans.empty:
        st.info("💡 Complete clear! No active loan parameters tracked on database arrays.")
        return

    raw_loans.columns = raw_loans.columns.str.strip().str.lower().str.replace(" ", "_")
    
    if "tenant_id" in raw_loans.columns:
        loans_df = raw_loans[raw_loans["tenant_id"].astype(str) == str(current_tenant)].copy()
    else:
        loans_df = raw_loans.copy()

    if loans_df.empty:
        st.info("💡 No active profile accounts registered to this company portal.")
        return

    if raw_payments is not None and not raw_payments.empty:
        raw_payments.columns = raw_payments.columns.str.strip().str.lower().str.replace(" ", "_")
        payments_df = raw_payments if "tenant_id" not in raw_payments.columns else raw_payments[raw_payments["tenant_id"].astype(str) == str(current_tenant)].copy()
    else:
        payments_df = pd.DataFrame()

    bor_map = {}
    if raw_borrowers is not None and not raw_borrowers.empty:
        raw_borrowers.columns = raw_borrowers.columns.str.strip().str.lower().str.replace(" ", "_")
        borrowers_df = raw_borrowers if "tenant_id" not in raw_borrowers.columns else raw_borrowers[raw_borrowers["tenant_id"].astype(str) == str(current_tenant)]
        bor_map = dict(zip(borrowers_df["id"].astype(str), borrowers_df["name"]))
        
    if "borrower" not in loans_df.columns:
        if "borrower_id" in loans_df.columns:
            loans_df["borrower"] = loans_df["borrower_id"].astype(str).map(bor_map).fillna("Unknown Profile")
        else:
            loans_df["borrower"] = "Unknown Profile"

    def clean_id_str(val):
        if pd.isna(val): return ""
        s = str(val).strip()
        if s.endswith(".0"): return s[:-2]
        return s

    loans_df["id_clean"] = loans_df["id"].apply(clean_id_str)
    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].apply(clean_id_str)

    if "loan_id_label" not in loans_df.columns:
        loans_df["loan_id_label"] = loans_df["id_clean"]
    loans_df["loan_id_label"] = loans_df["loan_id_label"].fillna(loans_df["id_clean"]).astype(str)

    loans_df = loans_df.sort_values(by=["loan_id_label"])

    # ==============================
    # 🎯 SELECTION INTERFACE
    # ==============================
    loan_map = {
        f"Ref: {r['loan_id_label']} — {r['borrower']}": r["loan_id_label"]
        for _, r in loans_df.iterrows()
    }
    
    selected_label = st.selectbox("🎯 Target Loan Account Filter", list(dict.fromkeys(loan_map.keys())))
    target_label = loan_map[selected_label]
    
    filtered_loans = loans_df[loans_df["loan_id_label"] == target_label]
    
    if filtered_loans.empty:
        st.error("Target loan data vector unreadable.")
        return

    # Sort chronological subsets to split initial entries vs rolling items correctly
    seq_col = "cycle" if "cycle" in filtered_loans.columns else "id"
    filtered_loans = filtered_loans.sort_values(by=[seq_col])

    # ==============================
    # 📊 CORRECTED STATEMENT PREVIEW PANEL
    # ==============================
    st.markdown("<h4 class='snapshot-text'>📑 Account Balance Breakdown</h4>", unsafe_allow_html=True)
    
    # 🔄 FIX: Isolate origin and endpoint records to get accurate, non-cumulative numbers
    origin_record = filtered_loans.iloc[0]
    latest_record = filtered_loans.iloc[-1]

    p = float(origin_record.get("principal", 0))

    total_due = float(
        latest_record.get(
            "total_due",
            latest_record.get(
                "amount_due",
                0
            )
        )
    )
    
    paid = filtered_loans["amount_paid"].fillna(0).astype(float).sum()
    
    bal = float(
        latest_record.get(
            "balance",
            0
        )
    )
    
    i = max(
        total_due - p,
        0
    )
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("principal Allocation", f"UGX {p:,.0f}", delta="Disbursement Base", delta_color="off")
    m2.metric("Interest Cost Accrued", f"UGX {i:,.0f}", delta=f"{(i/total_due*100):.1f}% Markup" if total_due > 0 else None, delta_color="normal")
    m3.metric("Total Paid to date", f"UGX {paid:,.0f}", delta=f"{paid/total_due:.1%} Covered" if total_due > 0 else None, delta_color="normal")
    m4.metric("Current Balance Owed", f"UGX {bal:,.0f}", delta=f"Outstanding Position", delta_color="inverse")

    # ==============================
    # 📜 CORRECTED TRANSACTION HISTORY (LEDGER RUNTIME)
    # ==============================
    ledger_data = []

    for _, row in filtered_loans.iterrows():
    
        ledger_data.append({
    
            "Cycle":
                row.get("cycle", ""),
    
            "date":
                str(
                    row.get(
                        "start_date",
                        row.get(
                            "created_at",
                            ""
                        )
                    )
                )[:10],
    
            "Opening Balance":
                float(
                    row.get(
                        "principal",
                        row.get(
                            "opening_balance",
                            0
                        )
                    )
                ),
    
            "Amount Due":
                float(
                    row.get(
                        "total_due",
                        row.get(
                            "amount_due",
                            0
                        )
                    )
                ),
    
            "Amount Paid":
                float(
                    row.get(
                        "amount_paid",
                        0
                    )
                ),
    
            "Balance":
                float(
                    row.get(
                        "balance",
                        0
                    )
                ),
    
            "status":
                row.get(
                    "status",
                    ""
                )
        })
    
    ledger_df = pd.DataFrame(ledger_data)
    
    st.dataframe(
        ledger_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Opening Balance":
                st.column_config.NumberColumn(
                    format="%,.0f"
                ),
    
            "Amount Due":
                st.column_config.NumberColumn(
                    format="%,.0f"
                ),
    
            "Amount Paid":
                st.column_config.NumberColumn(
                    format="%,.0f"
                ),
    
            "Balance":
                st.column_config.NumberColumn(
                    format="%,.0f"
                ),
        }
    )

    st.markdown("---")

    # ==============================
    # 📄 PREMIUM DOWNLOAD SECTION
    # ==============================
    st.markdown(f"""
        <div style="border: 1px solid {brand_color}33; padding: 1.5rem; border-radius: 12px; background-color: {brand_color}08; margin-bottom:15px;">
            <p style="font-family: 'Inter', sans-serif; font-weight: 600; margin-bottom: 5px; color:{brand_color};">Export Premium Statement Artifact</p>
            <p style="font-family: 'Inter', sans-serif; font-size: 0.88rem; color: #4B5563; margin-bottom: 0px;">
                Generates audit-ready customer ledgers complete with formal registration letterhead blocks.
            </p>
        </div>
    """, unsafe_allow_html=True)

    if st.button("✨ Compile Formal PDF Statement", use_container_width=True):
        client_name = filtered_loans.iloc[0].get("borrower", "Unknown Profile")
        client_loans = loans_df[loans_df["borrower"] == client_name]

        with st.spinner("Rendering Document Architecture..."):
            pdf_output = generate_pdf_statement(client_name, client_loans, payments_df)

        st.download_button(
            label=f"⬇️ Download Verified Statement Document (.pdf)",
            data=pdf_output,
            file_name=f"Statement_{client_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf",
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
        # LOGO UPLOAD SAFETY (STORAGE BUCKET FIXED)
        # ==============================
        if logo_file:
            try:
                bucket_name = "company-logos"
                file_path = f"logos/{active_company.get('id')}_logo.png"
                file_bytes = logo_file.getvalue()

                # Robust Upsert Logic: Try updating first; if it doesn't exist, execute an upload.
                try:
                    supabase.storage.from_(bucket_name).update(
                        path=file_path,
                        file=file_bytes,
                        file_options={"content-type": "image/png"}
                    )
                except Exception:
                    # Fallback to fresh file creation if the path didn't exist yet
                    supabase.storage.from_(bucket_name).upload(
                        path=file_path,
                        file=file_bytes,
                        file_options={"content-type": "image/png"}
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
                
            elif page == "payments":
                show_payments(supabase)
                
            elif page == "expenses":
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
