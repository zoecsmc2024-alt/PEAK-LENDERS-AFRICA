# ==========================================
# FINAL APP ROUTER (REACTIVE & STABLE)
# ==========================================

if __name__ == "__main__":

    # 1. Initialize Default State
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if "view" not in st.session_state:
        st.session_state["view"] = "login"

    # 2. Auth Flow (Gatekeeper)
    if not st.session_state["authenticated"]:
        st.session_state['theme_color'] = "#1E3A8A"
        apply_master_theme()
        run_auth_ui(supabase)
        st.stop()  # Stops execution here if not logged in

    # 3. Main App (Only runs if authenticated)
    try:
        check_session_timeout()
        
        # Get the selection from your sidebar function
        raw_page = render_sidebar()
        apply_master_theme()

        # --- BULLETPROOF ROUTING LOGIC ---
        # We convert to string and lowercase to avoid emoji/casing mismatches
        page = str(raw_page).lower().strip()

        if "overview" in page:
            show_dashboard_view()
        
        elif "loans" in page:
            show_loans()
            
        elif "borrowers" in page:
            show_borrowers()
            
        elif "collateral" in page:
            show_collateral()
            
        elif "calendar" in page:
            show_calendar()
            
        elif "ledger" in page:
            show_ledger()
            
        elif "payments" in page:
            show_payments()
            
        elif "expenses" in page:
            show_expenses()
            
        elif "petty cash" in page:
            show_petty_cash()
            
        elif "overdue" in page:
            show_overdue_tracker()
            
        elif "payroll" in page:
            show_payroll()
            
        elif "reports" in page:
            show_reports()
            
        elif "settings" in page:
            show_settings()
            
        else:
            # This helps you see EXACTLY what is being passed if it fails
            st.warning(f"Recognized page: '{raw_page}' but no module matches.")
            st.info("Ensure your render_sidebar() returns the selected value.")

    except Exception as e:
        st.error(f"⚠️ Application Error: {e}")
