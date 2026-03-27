import streamlit as st
from st_supabase_connection import SupabaseConnection
from streamlit_option_menu import option_menu
import pandas as pd

# --- 1. INITIALIZE CONNECTION ---
conn = st.connection("supabase", type=SupabaseConnection)

# --- 2. CACHED TENANT FETCH (Fixed) ---
@st.cache_data(ttl=300)
def get_tenant_data(tenant_id): # Removed 'conn' as a parameter to prevent hashing error
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
        return None

# --- 3. MODULE FUNCTIONS ---

def render_dashboard(tenant):
    company = tenant.get("company_name", "LendFlow")
    currency = tenant.get("currency", "UGX")
    st.title(f"📊 {company} Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Loan Book", f"{currency} 0", "+0%")
    col2.metric("Active Borrowers", "0", "+0")
    col3.metric("Monthly Revenue", f"{currency} 0", "+0%")
    col4.metric("Default Rate", "0%", "-0%")
    st.info("📈 CEO Dashboard insights and charts coming soon.")

def render_portfolio(tenant_id):
    st.title("📂 Portfolio Management")
    t1, t2, t3 = st.tabs(["👥 Borrowers", "📑 Loans Book", "🛡️ Collateral Vault"])

    with t1:
        st.subheader("Manage Borrowers")
        col_form, col_list = st.columns([1, 2])
        with col_form:
            with st.form("add_borrower_form", clear_on_submit=True):
                name = st.text_input("Full Name")
                phone = st.text_input("Phone Number")
                nin = st.text_input("National ID (NIN)")
                if st.form_submit_button("Save Borrower", type="primary"):
                    if name and phone:
                        conn.table("borrowers").insert({
                            "tenant_id": tenant_id, "name": name, 
                            "phone": phone, "national_id": nin
                        }).execute()
                        st.toast(f"Saved {name}!", icon="👤")
                    else:
                        st.error("Name and Phone are required.")
        with col_list:
            data = conn.table("borrowers").select("*").eq("tenant_id", tenant_id).execute()
            if data.data:
                st.dataframe(pd.DataFrame(data.data)[['name', 'phone', 'national_id']], use_container_width=True)

    with t2:
        st.subheader("Active Loan Book")
        st.info("Loan tracking and schedules will appear here.")

def render_treasury():
    st.title("💰 Treasury & Cashflow")
    t1, t2, t3 = st.tabs(["📥 Payments", "📤 Expenses", "☕ Petty Cash"])
    with t1: st.info("Record loan repayments.")

def render_admin():
    st.title("🧾 Admin & Payroll")
    t1, t2, t3 = st.tabs(["👥 Staff", "💸 Payroll", "🏛️ Taxes (URA/NSSF)"])
    with t1: st.info("Role-based access control coming soon.")

def render_settings(tenant):
    st.title("⚙️ Workspace Settings")
    with st.container(border=True):
        st.subheader("Branding")
        new_name = st.text_input("Primary Name", value=tenant.get("company_name"))
        new_color = st.color_picker("Primary Color", value=tenant.get("theme_color", "#2B3F87"))
        if st.button("Save Settings", type="primary"):
            conn.table("tenants").update({"company_name": new_name, "theme_color": new_color}).eq("id", tenant['id']).execute()
            st.cache_data.clear() # Clears cache so branding updates immediately
            st.rerun()

# --- 4. MAIN INTERFACE ---
def main_interface():
    if "tenant_id" not in st.session_state:
        st.error("Session expired. Please log in again.")
        st.stop()

    tenant = get_tenant_data(st.session_state.tenant_id)
    if not tenant: st.stop()

    brand_color = tenant.get("theme_color", "#2B3F87")
    company = tenant.get("company_name", "LendFlow")

    # CUSTOM STYLING
    st.markdown(f"<style>.company-title {{ color: {brand_color}; font-weight: 600; font-size: 20px; }}</style>", unsafe_allow_html=True)

    # TOP BAR
    col_logo, col_nav, col_exit = st.columns([1.5, 4, 1], gap="small")
    with col_logo:
        st.markdown(f"<div class='company-title'>🚀 {company}</div>", unsafe_allow_html=True)
    with col_nav:
        selected = option_menu(
            menu_title=None,
            options=["Dashboard", "Portfolio", "Treasury", "Admin", "Settings"],
            icons=["speedometer2", "briefcase", "cash-stack", "person-badge", "gear"],
            orientation="horizontal",
            styles={"nav-link-selected": {"background-color": brand_color}}
        )
    with col_exit:
        if st.button("🚪 Logout", use_container_width=True):
            for key in list(st.session_state.keys()): del st.session_state[key]
            st.rerun()

    st.markdown("---")

    # PAGE ROUTING
    if selected == "Dashboard": render_dashboard(tenant)
    elif selected == "Portfolio": render_portfolio(st.session_state.tenant_id)
    elif selected == "Treasury": render_treasury()
    elif selected == "Admin": render_admin()
    elif selected == "Settings": render_settings(tenant)

# --- 5. EXECUTION ---
if "logged_in" not in st.session_state or not st.session_state.logged_in:
    # Minimal login for development - add your login screen here
    st.title("🔐 LendFlow Login")
    email_in = st.text_input("Email")
    if st.button("Enter Workspace"):
        profile = conn.table("profiles").select("tenant_id").eq("email", email_in).execute()
        if profile.data:
            st.session_state.logged_in = True
            st.session_state.tenant_id = profile.data[0]['tenant_id']
            st.rerun()
else:
    main_interface()
