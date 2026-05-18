# ==========================================
# VIEW BORROWERS PAGE (STREAMLIT VERSION)
# ==========================================
import streamlit as st
import pandas as pd
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from core.database import supabase
import io

def show_borrowers():
    # ==========================================
    # STYLING & MARKUP
    # ==========================================
    st.markdown("""
    <style>
    .borrower-title {
        font-size: 32px;
        font-weight: 700;
        color: #0A192F;
        margin-bottom: 10px;
    }

    .metric-card {
        background: white;
        padding: 15px;
        border-radius: 12px;
        border: 1px solid #E5E7EB;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }

    .search-box input {
        border-radius: 10px !important;
    }

    .stButton button {
        border-radius: 8px !important;
        font-weight: 600;
    }

    .borrower-container {
        background: white;
        padding: 15px;
        border-radius: 14px;
        border: 1px solid #E5E7EB;
    }

    /* Target the AgGrid component tree specifically to change table typography */
    .borrower-container .ag-theme-streamlit,
    .borrower-container .ag-cell,
    .borrower-container .ag-header-cell-text,
    .borrower-container .ag-paging-panel {
        font-family: "Segoe UI", -apple-system, BlinkMacSystemFont, Roboto, sans-serif !important;
        font-size: 14px !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # ==========================================
    # FETCH BORROWERS
    # ==========================================
    @st.cache_data(ttl=60)
    def load_borrowers():
        try:
            response = (
                supabase
                .table("borrowers")
                .select("*")
                .execute()
            )

            data = response.data if response.data else []

            if not data:
                return pd.DataFrame()

            df_local = pd.DataFrame(data)

            # Fill missing columns safely
            expected_cols = [
                "id",
                "full_name",
                "business_name",
                "unique_number",
                "mobile",
                "email",
                "total_paid",
                "open_loans_balance",
                "status",
                "gender",
                "working_status",
                "city",
                "province",
                "loan_officer",
                "created_at"
            ]

            for col in expected_cols:
                if col not in df_local.columns:
                    df_local[col] = ""

            return df_local

        except Exception as e:
            st.error(f"Error loading borrowers: {e}")
            return pd.DataFrame()

    # ==========================================
    # LOAD DATA
    # ==========================================
    df = load_borrowers()

    st.markdown(
        '<div class="borrower-title">👥 View Borrowers</div>',
        unsafe_allow_html=True
    )

    # ==========================================
    # 🧭 TOP ACTION BAR (CLEAN SAAS LAYOUT)
    # ==========================================
    
    # =========================
    # ROW 1: SEARCH (FULL WIDTH)
    # =========================
    search = st.text_input(
        "🔍 Search Borrowers",
        placeholder="Search name, phone, email, business...",
        key="borrower_search_input"
    )
    
    st.markdown("---")
    
    # =========================
    # ROW 2: ACTIONS
    # =========================
    col1, col2, col3, col4 = st.columns([1.2, 1, 1, 1])
    
    with col1:
        show_filters = st.toggle("⚙ Advanced Filters", key="toggle_adv_filters")
    
    with col2:
        if not df.empty:
            st.download_button(
                "⬇ Export CSV",
                data=df.to_csv(index=False),
                file_name="borrowers.csv",
                mime="text/csv",
                use_container_width=True,
                key="btn_export_csv"
            )
        else:
            st.button("⬇ Export CSV", disabled=True, use_container_width=True, key="btn_export_csv_disabled")
    
    with col3:
        if st.button("🔄 Refresh", use_container_width=True, key="btn_refresh_data"):
            st.cache_data.clear()
            st.rerun()
    
    with col4:
        st.markdown(" ")  # spacing placeholder

    # ==========================================
    # ADVANCED SEARCH SECTION
    # ==========================================
    if show_filters and not df.empty:
        st.markdown("### 🔎 Advanced Search")
        f1, f2, f3 = st.columns(3)

        with f1:
            borrower_status = st.multiselect(
                "Loan Status",
                ["Current", "Due Today", "Missed Repayment", "Arrears", "Past Maturity", "Fully Paid", "Defaulted"],
                key="filter_status"
            )
            gender = st.multiselect(
                "Gender",
                ["Male", "Female", "Nonbinary", "Other"],
                key="filter_gender"
            )

        with f2:
            working_status = st.multiselect(
                "Working Status",
                ["Employee", "Government Employee", "Private Sector Employee", "Owner", "Student", "Pensioner", "Unemployed"],
                key="filter_working_status"
            )
            city = st.text_input("City", key="filter_city")

        with f3:
            province = st.text_input("Province / State", key="filter_province")
            officer = st.text_input("Loan Officer", key="filter_officer")

        d1, d2 = st.columns(2)
        with d1:
            from_date = st.date_input("Created From", value=None, key="filter_from_date")
        with d2:
            to_date = st.date_input("Created To", value=None, key="filter_to_date")

        # Apply Advanced Filtering Logic
        if borrower_status:
            df = df[df["status"].isin(borrower_status)]
        if gender:
            df = df[df["gender"].isin(gender)]
        if working_status:
            df = df[df["working_status"].isin(working_status)]
        if city:
            df = df[df["city"].astype(str).str.lower().str.contains(city.lower())]
        if province:
            df = df[df["province"].astype(str).str.lower().str.contains(province.lower())]
        if officer:
            df = df[df["loan_officer"].astype(str).str.lower().str.contains(officer.lower())]
        if from_date:
            df = df[pd.to_datetime(df["created_at"]).dt.date >= from_date]
        if to_date:
            df = df[pd.to_datetime(df["created_at"]).dt.date <= to_date]

    # ==========================================
    # SEARCH FILTER (KEYWORD STRING MATCH)
    # ==========================================
    if not df.empty and search:
        search_lower = search.lower()
        df = df[
            df.astype(str)
            .apply(
                lambda row: row.str.lower().str.contains(search_lower).any(),
                axis=1
            )
        ]

    # ==========================================
    # METRICS DISPLAY
    # ==========================================
    if not df.empty:
        total_paid = pd.to_numeric(df["total_paid"], errors="coerce").fillna(0).sum()
        open_balance = pd.to_numeric(df["open_loans_balance"], errors="coerce").fillna(0).sum()

        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Total Borrowers", f"{len(df):,}")
        with m2:
            st.metric("Total Paid", f"{total_paid:,.2f}")
        with m3:
            st.metric("Open Loan Balance", f"{open_balance:,.2f}")
    else:
        total_paid = 0.0
        open_balance = 0.0

    # ==========================================
    # DATA AG-GRID TABLE
    # ==========================================
    st.markdown('<div class="borrower-container">', unsafe_allow_html=True)

    if df.empty:
        st.warning("No borrowers found.")
    else:
        # Rename columns safely for end-user display grid representation
        display_df = df.rename(columns={
            "full_name": "Full Name",
            "business_name": "Business",
            "unique_number": "Unique#",
            "mobile": "Mobile",
            "email": "Email",
            "total_paid": "Total Paid",
            "open_loans_balance": "Open Loans Balance",
            "status": "Status"
        })

        visible_columns = [
            "Full Name",
            "Business",
            "Unique#",
            "Mobile",
            "Email",
            "Total Paid",
            "Open Loans Balance",
            "Status"
        ]
        display_df = display_df[visible_columns]

        gb = GridOptionsBuilder.from_dataframe(display_df)
        gb.configure_pagination(enabled=True, paginationPageSize=20)
        gb.configure_default_column(sortable=True, filter=True, resizable=True)

        gb.configure_column(
            "Total Paid",
            type=["numericColumn"],
            valueFormatter="x.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})"
        )
        gb.configure_column(
            "Open Loans Balance",
            type=["numericColumn"],
            valueFormatter="x.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})"
        )
        gb.configure_selection(selection_mode="single", use_checkbox=True)

        grid_options = gb.build()

        grid_response = AgGrid(
            display_df,
            gridOptions=grid_options,
            height=600,
            width="100%",
            fit_columns_on_grid_load=True,
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            allow_unsafe_jscode=True,
            theme="streamlit"
        )
        
        # Track selected row state globally for upcoming app pages
        if grid_response and "selected_rows" in grid_response and grid_response["selected_rows"] is not None:
            selected_rows = grid_response["selected_rows"]
            if isinstance(selected_rows, pd.DataFrame):
                st.session_state["selected_borrower"] = selected_rows.to_dict(orient="records")[0] if not selected_rows.empty else None
            elif isinstance(selected_rows, list) and len(selected_rows) > 0:
                st.session_state["selected_borrower"] = selected_rows[0]
            else:
                st.session_state["selected_borrower"] = None

    st.markdown("</div>", unsafe_allow_html=True)

    # ==========================================
    # FOOTER SUMMARY TOTALS
    # ==========================================
    if not df.empty:
        st.markdown("---")
        f1, f2 = st.columns(2)
        with f1:
            st.success(f"💰 Total Paid: {total_paid:,.2f}")
        with f2:
            st.error(f"📉 Open Loans Balance: {open_balance:,.2f}")

    # ==========================================
    # ACTION SECTION HANDLERS
    # ==========================================
    st.markdown("### ⚡ Borrower Actions")
    a1, a2, a3, a4 = st.columns(4)

    # Initialize view state if it doesn't exist
    if "show_add_form" not in st.session_state:
        st.session_state.show_add_form = False

    with a1:
        if st.button("➕ Add Borrower", key="btn_toggle_add_form"):
            st.session_state.show_add_form = not st.session_state.show_add_form

    # Render the input form right here if toggled on
    if st.session_state.show_add_form:
        st.markdown("---")
        st.markdown("### 📝 Create New Borrower Profile")
        with st.form("new_borrower_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("Full Name *", key="input_new_name")
                new_phone = st.text_input("Mobile Number *", key="input_new_phone")
                new_email = st.text_input("Email Address", key="input_new_email")
            with col2:
                new_biz = st.text_input("Business Name", key="input_new_biz")
                new_id = st.text_input("Unique Number / ID", key="input_new_id")
            
            submit_btn = st.form_submit_button("Save Profile")
            if submit_btn:
                if not new_name or not new_phone:
                    st.error("Name and Mobile Number are required fields.")
                else:
                    try:
                        supabase.table("borrowers").insert({
                            "full_name": new_name,
                            "mobile": new_phone,
                            "email": new_email,
                            "business_name": new_biz,
                            "unique_number": new_id,
                            "status": "Active"
                        }).execute()
                        st.success(f"Successfully added {new_name}!")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as ex:
                        st.error(f"Failed to save profile: {ex}")

    with a2:
        # Fixed Excel Download Logic pattern preventing download button disappearances
        if not df.empty:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="Borrowers")
            excel_data = output.getvalue()
            
            st.download_button(
                label="📄 Export Excel",
                data=excel_data,
                file_name="borrowers.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="btn_download_excel"
            )
        else:
            st.button("📄 Export Excel", disabled=True, use_container_width=True, key="btn_download_excel_disabled")
    
    with a3:
        if st.button("🖨 Print Report", use_container_width=True, key="btn_print_report"):
            st.components.v1.html("""
            <script>
                window.print();
            </script>
            """, height=0, width=0)
            st.info("Print dialog opened")
    
    with a4:
        if st.button("👁 Show / Hide Columns", use_container_width=True, key="btn_toggle_columns"):
            if "show_columns" not in st.session_state:
                st.session_state["show_columns"] = True
            else:
                st.session_state["show_columns"] = not st.session_state["show_columns"]
    
        if st.session_state.get("show_columns", True):
            st.success("Columns Visible Mode Enabled")
        else:
            st.warning("Column Compact Mode Enabled")
