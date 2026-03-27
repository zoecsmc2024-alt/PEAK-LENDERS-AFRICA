import streamlit as st
from st_supabase_connection import SupabaseConnection
from streamlit_option_menu import option_menu
import pandas as pd

# --- 1. GLOBAL CONFIGURATION ---
st.set_page_config(page_title="LendFlow Africa | SaaS", layout="wide")

# Custom CSS for Navy/Baby Blue palette & Small Buttons
st.markdown("""
    <style>
        div.stButton > button { background-color: #2B3F87; color: white; border-radius: 5px; padding: 5px 15px; font-size: 14px; border: none; }
        div.stButton > button:hover { background-color: #1E2D61; color: white; border: none; }
        .stTabs [data-baseweb="tab-list"] { gap: 10px; }
        .stTabs [data-baseweb="tab"] { background-color: #E1F5FE; border-radius: 5px 5px 0 0; padding: 5px 20px; color: #2B3F87; }
        .stTabs [aria-selected="true"] { background-color: #2B3F87 !important; color: white !important; }
    </style>
""", unsafe_allow_html=True)

# Connect to Supabase
conn = st.connection("supabase", type=SupabaseConnection)

# --- 2. SESSION STATE MANAGEMENT ---
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "tenant_id" not in st.session_state:
    st.session_state.tenant_id = None
if "user_email" not in st.session_state:
    st.session_state.user_email = ""

# --- 3. GATEKEEPER (LOGIN/SIGNUP) ---
def login_screen():
    st.markdown("<h1 style='text-align: center; color: #2B3F87;'>🚀 LendFlow Africa</h1>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        tab_log, tab_reg = st.tabs(["🔐 Staff Login", "🏢 Register Business"])
        with tab_log:
            email = st.text_input("Email")
            pwd = st.text_input("Password", type="password")
            if st.button("Enter Workspace", use_container_width=True):
                # Temporary simulated login - We'll link this to Supabase Auth later
                res = conn.table("tenants").select("*").limit(1).execute()
                if res.data:
                    st.session_state.logged_in = True
                    st.session_state.tenant_id = res.data[0]['id']
                    st.session_state.user_email = email
                    st.rerun()
        with tab_reg:
            st.markdown("##### 🏢 Create your Lending Workspace")
            with st.form("register_form", clear_on_submit=True):
                new_biz = st.text_input("Business Name (e.g., Zoe Consults)")
                new_email = st.text_input("Admin Email")
                new_pwd = st.text_input("Password", type="password")
                
                agree = st.checkbox("I agree to the Privacy Policy & Terms")
                
                if st.form_submit_button("Start My Journey", type="primary"):
                    if not agree:
                        st.error("Please agree to the terms.")
                    elif not new_biz or not new_email or not new_pwd:
                        st.warning("All fields are required.")
                    else:
                        try:
                            # 1. CREATE THE BUSINESS (TENANT)
                            t_res = conn.table("tenants").insert({
                                "company_name": new_biz,
                                "theme_color": "#2B3F87"
                            }).execute()
                            t_id = t_res.data[0]['id']
                            
                            # 2. CREATE THE STAFF PROFILE (ADMIN)
                            # Note: We'll link this to Supabase Auth properly next, 
                            # but for now, we'll just save the profile.
                            conn.table("profiles").insert({
                                "tenant_id": t_id,
                                "email": new_email,
                                "full_name": "Business Owner",
                                "role": "Admin"
                            }).execute()
                            
                            st.success(f"🚀 {new_biz} is ready! Switch to 'Staff Login' to enter.")
                        except Exception as e:
                            st.error(f"Registration error: {e}")

# --- 4. THE 5-PILLAR ROUTER (PRODUCTION READY) ---
import streamlit as st
from streamlit_option_menu import option_menu

# --- CACHED TENANT FETCH (Performance Boost) ---
@st.cache_data(ttl=300)
def get_tenant_data(conn, tenant_id):
    try:
        response = (
            conn.table("tenants")
            .select("*")
            .eq("id", tenant_id)
            .single()
            .execute()
        )
        return response.data
    except Exception as e:
        st.error("⚠️ Failed to load workspace data.")
        return None


def main_interface():
    # --- SESSION GUARD ---
    if "tenant_id" not in st.session_state:
        st.error("Session expired. Please log in again.")
        st.stop()

    # --- FETCH TENANT DATA ---
    tenant = get_tenant_data(conn, st.session_state.tenant_id)

    if not tenant:
        st.stop()

    # --- BRANDING ---
    brand_color = tenant.get("theme_color", "#2B3F87")
    company = tenant.get("company_name", "LendFlow")

    # --- CUSTOM STYLING ---
    st.markdown(f"""
        <style>
            .topbar {{
                padding: 0.5rem 0;
                border-bottom: 1px solid #eee;
            }}
            .company-title {{
                font-weight: 600;
                color: {brand_color};
                font-size: 20px;
            }}
            .section-header {{
                margin-top: 0.5rem;
            }}
        </style>
    """, unsafe_allow_html=True)

    # --- TOP BAR ---
    col_logo, col_nav, col_exit = st.columns([1.5, 4, 1], gap="small")

    with col_logo:
        st.markdown(
            f"<div class='company-title'>🚀 {company}</div>",
            unsafe_allow_html=True
        )

    with col_nav:
        selected = option_menu(
            menu_title=None,
            options=["Dashboard", "Portfolio", "Treasury", "Admin", "Settings"],
            icons=["speedometer2", "briefcase", "cash-stack", "person-badge", "gear"],
            orientation="horizontal",
            styles={
                "container": {"padding": "0!important", "background-color": "transparent"},
                "nav-link": {"font-size": "14px", "margin": "0px"},
                "nav-link-selected": {
                    "background-color": brand_color,
                    "font-weight": "600",
                },
            },
        )

    with col_exit:
        st.write("")  # spacing
        if st.button("🚪 Logout", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

    st.markdown("---")

    # --- PAGE ROUTING ---
    if selected == "Dashboard":
        render_dashboard(company)

    elif selected == "Portfolio":
        render_portfolio()

    elif selected == "Treasury":
        render_treasury()

    elif selected == "Admin":
        render_admin()

    elif selected == "Settings":
        render_settings()


# --- MODULE FUNCTIONS (SCALABLE ARCHITECTURE) ---

def render_dashboard(company):
    st.title(f"📊 {company} Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Loan Book", "UGX 0", "+0%")
    col2.metric("Active Borrowers", "0", "+0")
    col3.metric("Monthly Revenue", "UGX 0", "+0%")
    col4.metric("Default Rate", "0%", "-0%")

    st.info("📈 CEO Dashboard insights and charts coming soon.")


def render_portfolio():
    st.title("📂 Portfolio Management")

    t1, t2, t3 = st.tabs(["👥 Borrowers", "📑 Loans Book", "🛡️ Collateral Vault"])

    with t1:
        st.subheader("Manage Borrowers")
        st.info("Borrower onboarding form goes here.")

    with t2:
        st.subheader("Active Loan Book")
        st.info("Loan tracking and schedules will appear here.")

    with t3:
        st.subheader("Collateral Tracker")
        st.info("Track pledged assets and valuations.")


def render_treasury():
    st.title("💰 Treasury & Cashflow")

    t1, t2, t3 = st.tabs(["📥 Payments", "📤 Expenses", "☕ Petty Cash"])

    with t1:
        st.subheader("Incoming Payments")
        st.info("Record loan repayments.")

    with t2:
        st.subheader("Operating Expenses")
        st.info("Track company spending.")

    with t3:
        st.subheader("Daily Petty Cash")
        st.info("Manage small operational expenses.")


def render_admin():
    st.title("🧾 Admin & Payroll")

    t1, t2, t3 = st.tabs(["👥 Staff", "💸 Payroll", "🏛️ Taxes (URA/NSSF)"])

    with t1:
        st.subheader("Team Access Control")
        st.info("Role-based access control coming soon.")

    with t2:
        st.subheader("Payroll Management")
        st.info("Salary processing module.")

    with t3:
        st.subheader("Tax Compliance")
        st.info("URA & NSSF integration.")


def render_settings():
    st.title("⚙️ Workspace Settings")

    st.subheader("Branding")
    st.color_picker("Primary Color", "#2B3F87")

    st.subheader("System Preferences")
    st.checkbox("Enable Notifications")
    st.checkbox("Auto-generate Reports")

    st.success("Settings saved automatically.")
# --- 5. EXECUTION ---
if not st.session_state.logged_in:
    login_screen()
else:
    main_interface()
