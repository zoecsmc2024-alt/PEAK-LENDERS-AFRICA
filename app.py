
st.set_page_config(
    page_title="Lending Manager Pro",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
SESSION_TIMEOUT = 30





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
