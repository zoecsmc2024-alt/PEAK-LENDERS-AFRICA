# ==========================================
# VIEW BORROWERS PAGE (STREAMLIT VERSION)
# ==========================================
import streamlit as st
import pandas as pd
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from core.database import supabase

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
    # TOP ACTIONS
    # ==========================================
    top1, top2, top3, top4 = st.columns([2, 1, 1, 1])

    with top1:
        search = st.text_input(
            "Search Borrowers",
            placeholder="Search name, phone, email, business...",
            key="borrower_search_input"
        )

    with top2:
        if not df.empty:
            st.download_button(
                "⬇ Export CSV",
                data=df.to_csv(index=False),
                file_name="borrowers.csv",
                mime="text/csv"
            )
        else:
            st.button("⬇ Export CSV", disabled=True)

    with top3:
        if st.button("🔄 Refresh"):
            st.cache_data.clear()
            st.rerun()

    with top4:
        show_filters = st.toggle("Advanced Search")

    # ==========================================
    # ADVANCED SEARCH SECTION
    # ==========================================
    if show_filters and not df.empty:
        st.markdown("### 🔎 Advanced Search")
        f1, f2, f3 = st.columns(3)

        with f1:
            borrower_status = st.multiselect(
                "Loan Status",
                ["Current", "Due Today", "Missed Repayment", "Arrears", "Past Maturity", "Fully Paid", "Defaulted"]
            )
            gender = st.multiselect(
                "Gender",
                ["Male", "Female", "Nonbinary", "Other"]
            )

        with f2:
            working_status = st.multiselect(
                "Working Status",
                ["Employee", "Government Employee", "Private Sector Employee", "Owner", "Student", "Pensioner", "Unemployed"]
            )
            city = st.text_input("City")

        with f3:
            province = st.text_input("Province / State")
            officer = st.text_input("Loan Officer")

        d1, d2 = st.columns(2)
        with d1:
            from_date = st.date_input("Created From", value=None)
        with d2:
            to_date = st.date_input("Created To", value=None)

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

        AgGrid(
            display_df,
            gridOptions=grid_options,
            height=600,
            width="100%",
            fit_columns_on_grid_load=True,
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            allow_unsafe_jscode=True,
            theme="streamlit"
        )

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
        if st.button("➕ Add Borrower"):
            st.session_state.show_add_form = not st.session_state.show_add_form

    # Render the input form right here if toggled on
    if st.session_state.show_add_form:
        st.markdown("---")
        st.markdown("### 📝 Create New Borrower Profile")
        with st.form("new_borrower_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("Full Name *")
                new_phone = st.text_input("Mobile Number *")
                new_email = st.text_input("Email Address")
            with col2:
                new_biz = st.text_input("Business Name")
                new_id = st.text_input("Unique Number / ID")
            
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
        if st.button("📄 Export Excel", use_container_width=True):
    
            if "df" in globals() and not df.empty:
                import io
    
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False, sheet_name="Borrowers")
    
                st.download_button(
                    label="⬇ Download Excel File",
                    data=output.getvalue(),
                    file_name="borrowers.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.warning("No data to export")
    
    with a3:
        if st.button("🖨 Print Report", use_container_width=True):
            st.markdown("""
            <script>
                window.print();
            </script>
            """, unsafe_allow_html=True)
            st.info("Print dialog opened")
    
    with a4:
        if st.button("👁 Show / Hide Columns", use_container_width=True):
    
            if "show_columns" not in st.session_state:
                st.session_state["show_columns"] = True
            else:
                st.session_state["show_columns"] = not st.session_state["show_columns"]
    
            if st.session_state["show_columns"]:
                st.success("Columns Visible Mode Enabled")
            else:
                st.warning("Column Compact Mode Enabled")
