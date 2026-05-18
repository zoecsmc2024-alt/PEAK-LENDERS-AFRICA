# ==========================================
# VIEW BORROWERS PAGE (STREAMLIT VERSION)
# ==========================================

import streamlit as st
import pandas as pd
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
# Core DB utilities
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas

import streamlit as st
import pandas as pd
from core.database import supabase


# ==========================================
# MAIN FUNCTION (IMPORT SAFE)
# ==========================================

def show_borrowers():

    st.markdown(
        "<h1 style='color:#0A192F;'>👥 View Borrowers</h1>",
        unsafe_allow_html=True
    )

    # ==========================================
    # FETCH DATA
    # ==========================================

    @st.cache_data(ttl=60)
    def get_borrowers():
        try:
            res = supabase.table("borrowers").select("*").execute()
            return pd.DataFrame(res.data) if res.data else pd.DataFrame()
        except Exception as e:
            st.error(f"Database error: {e}")
            return pd.DataFrame()

    df = get_borrowers()

    # ==========================================
    # TOP CONTROLS
    # ==========================================

    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        search = st.text_input("🔎 Search Borrowers", "")

    with col2:
        refresh = st.button("🔄 Refresh")
        if refresh:
            st.cache_data.clear()
            st.rerun()

    with col3:
        if not df.empty:
            st.download_button(
                "⬇ Export CSV",
                df.to_csv(index=False),
                file_name="borrowers.csv",
                mime="text/csv"
            )

    # ==========================================
    # FILTER SEARCH
    # ==========================================

    if not df.empty and search:
        search = search.lower()
        df = df[df.astype(str).apply(
            lambda row: row.str.lower().str.contains(search).any(),
            axis=1
        )]

    # ==========================================
    # METRICS
    # ==========================================

    if not df.empty:

        df["total_paid"] = pd.to_numeric(df.get("total_paid", 0), errors="coerce").fillna(0)
        df["open_loans_balance"] = pd.to_numeric(df.get("open_loans_balance", 0), errors="coerce").fillna(0)

        c1, c2, c3 = st.columns(3)

        with c1:
            st.metric("Total Borrowers", len(df))

        with c2:
            st.metric("Total Paid", f"{df['total_paid'].sum():,.2f}")

        with c3:
            st.metric("Open Loans Balance", f"{df['open_loans_balance'].sum():,.2f}")

    # ==========================================
    # TABLE DISPLAY
    # ==========================================

    if df.empty:
        st.warning("No borrowers found.")
        return

    # Safe column mapping
    display_df = df.copy()

    column_map = {
        "full_name": "Full Name",
        "business_name": "Business",
        "unique_number": "Unique #",
        "mobile": "Mobile",
        "email": "Email",
        "total_paid": "Total Paid",
        "open_loans_balance": "Open Balance",
        "status": "Status"
    }

    display_df.rename(columns=column_map, inplace=True)

    # Ensure required columns exist
    required_cols = [
        "Full Name",
        "Business",
        "Unique #",
        "Mobile",
        "Email",
        "Total Paid",
        "Open Balance",
        "Status"
    ]

    for col in required_cols:
        if col not in display_df.columns:
            display_df[col] = ""

    display_df = display_df[required_cols]

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

    # ==========================================
    # ACTIONS
    # ==========================================

    st.markdown("---")
    st.subheader("⚡ Actions")

    a1, a2, a3 = st.columns(3)

    with a1:
        if st.button("➕ Add Borrower"):
            st.info("Navigate to Add Borrower page")

    with a2:
        if st.button("📄 Export Excel"):
            st.info("Excel export can be added next")

    with a3:
        if st.button("🖨 Print"):
            st.info("Print feature can be added next")datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from core.database import supabase


# ==========================================
# PAGE CONFIG
# ==========================================
def show_borrowers:

st.set_page_config(
    page_title="View Borrowers",
    layout="wide"
)

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

        df = pd.DataFrame(data)

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
            "status"
        ]

        for col in expected_cols:
            if col not in df.columns:
                df[col] = ""

        return df

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

top1, top2, top3, top4 = st.columns([2,1,1,1])

with top1:
    search = st.text_input(
        "Search Borrowers",
        placeholder="Search name, phone, email, business..."
    )

with top2:
    export_csv = st.download_button(
        "⬇ Export CSV",
        data=df.to_csv(index=False),
        file_name="borrowers.csv",
        mime="text/csv"
    )

with top3:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()

with top4:
    show_filters = st.toggle("Advanced Search")


# ==========================================
# ADVANCED SEARCH SIDEBAR
# ==========================================

if show_filters:

    st.markdown("### 🔎 Advanced Search")

    f1, f2, f3 = st.columns(3)

    with f1:
        borrower_status = st.multiselect(
            "Loan Status",
            [
                "Current",
                "Due Today",
                "Missed Repayment",
                "Arrears",
                "Past Maturity",
                "Fully Paid",
                "Defaulted"
            ]
        )

        gender = st.multiselect(
            "Gender",
            [
                "Male",
                "Female",
                "Nonbinary",
                "Other"
            ]
        )

    with f2:
        working_status = st.multiselect(
            "Working Status",
            [
                "Employee",
                "Government Employee",
                "Private Sector Employee",
                "Owner",
                "Student",
                "Pensioner",
                "Unemployed"
            ]
        )

        city = st.text_input("City")

    with f3:
        province = st.text_input("Province / State")

        officer = st.text_input("Loan Officer")

    d1, d2 = st.columns(2)

    with d1:
        from_date = st.date_input(
            "Created From",
            value=None
        )

    with d2:
        to_date = st.date_input(
            "Created To",
            value=None
        )


# ==========================================
# SEARCH FILTER
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
# METRICS
# ==========================================

if not df.empty:

    total_paid = pd.to_numeric(
        df["total_paid"],
        errors="coerce"
    ).fillna(0).sum()

    open_balance = pd.to_numeric(
        df["open_loans_balance"],
        errors="coerce"
    ).fillna(0).sum()

    m1, m2, m3 = st.columns(3)

    with m1:
        st.metric(
            "Total Borrowers",
            len(df)
        )

    with m2:
        st.metric(
            "Total Paid",
            f"{total_paid:,.2f}"
        )

    with m3:
        st.metric(
            "Open Loan Balance",
            f"{open_balance:,.2f}"
        )


# ==========================================
# TABLE
# ==========================================

st.markdown('<div class="borrower-container">', unsafe_allow_html=True)

if df.empty:

    st.warning("No borrowers found.")

else:

    # Rename columns for display
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

    # Select visible columns
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

    gb.configure_pagination(
        enabled=True,
        paginationPageSize=20
    )

    gb.configure_default_column(
        sortable=True,
        filter=True,
        resizable=True
    )

    gb.configure_column(
        "Total Paid",
        type=["numericColumn"],
        valueFormatter="x.toLocaleString()"
    )

    gb.configure_column(
        "Open Loans Balance",
        type=["numericColumn"],
        valueFormatter="x.toLocaleString()"
    )

    gb.configure_selection(
        selection_mode="single",
        use_checkbox=True
    )

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
# FOOTER TOTALS
# ==========================================

if not df.empty:

    st.markdown("---")

    f1, f2 = st.columns(2)

    with f1:
        st.success(
            f"💰 Total Paid: {total_paid:,.2f}"
        )

    with f2:
        st.error(
            f"📉 Open Loans Balance: {open_balance:,.2f}"
        )


# ==========================================
# ACTION SECTION
# ==========================================

st.markdown("### ⚡ Borrower Actions")

a1, a2, a3, a4 = st.columns(4)

with a1:
    if st.button("➕ Add Borrower"):
        st.switch_page("pages/add_borrower.py")

with a2:
    if st.button("📄 Export Excel"):
        st.info("Excel export coming soon")

with a3:
    if st.button("🖨 Print"):
        st.info("Print feature coming soon")

with a4:
    if st.button("👁 Show / Hide Columns"):
        st.info("Column customization coming soon")
