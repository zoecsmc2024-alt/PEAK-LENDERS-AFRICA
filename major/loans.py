import streamlit as st
import pandas as pd
from datetime import datetime
from core.database import supabase


# =========================================================
# 🎨 LOANS PAGE STYLING (LOANDISK INSPIRED)
# =========================================================
def loans_styles():
    st.markdown("""
    <style>

    /* Page background */
    .loans-header {
        background: linear-gradient(90deg, #0A192F, #112240);
        padding: 14px 18px;
        border-radius: 10px;
        color: white;
        margin-bottom: 15px;
    }

    .loans-header h1 {
        margin: 0;
        font-size: 22px;
    }

    /* Table card */
    .loans-card {
        background: white;
        border-radius: 12px;
        padding: 15px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
    }

    /* Sidebar filters */
    .filter-box {
        background: #F8FAFC;
        padding: 12px;
        border-radius: 10px;
        border: 1px solid #E2E8F0;
        margin-bottom: 10px;
    }

    /* Buttons */
    .stButton > button {
        background: #2563EB;
        color: white;
        border-radius: 8px;
        height: 38px;
        font-weight: 600;
    }

    .stButton > button:hover {
        background: #1E40AF;
    }

    </style>
    """, unsafe_allow_html=True)


# =========================================================
# 📊 FETCH LOANS DATA (SUPABASE)
# =========================================================
def get_loans():
    try:
        res = supabase.table("loans").select("*").execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# =========================================================
# 🧭 VERTICAL SIDEBAR MENU (LOANS MODULE)
# =========================================================
def loans_sidebar():
    st.sidebar.markdown("## 💰 Loans")

    menu = st.sidebar.radio(
        "Navigate",
        [
            "View All Loans",
            "Add Loan",
            "Due Loans",
            "Missed Repayments",
            "Loans in Arrears",
            "No Repayments",
            "Past Maturity Date",
            "Principal Outstanding",
            "1 Month Late Loans",
            "3 Months Late Loans",
            "Loan Calculator",
            "Guarantors",
            "Loan Comments",
            "Approve Loans"
        ]
    )

    return menu


# =========================================================
# 📁 MAIN LOANS PAGE (RENAMED TO MATCH APP.PY IMPORT)
# =========================================================
def show_loans():

    loans_styles()
    menu = loans_sidebar()

    # Initialize filter states in session state so they persist across rerenders
    if "search_name" not in st.session_state:
        st.session_state.search_name = ""
    if "search_mobile" not in st.session_state:
        st.session_state.search_mobile = ""
    if "search_status" not in st.session_state:
        st.session_state.search_status = "All"

    # =====================================================
    # HEADER (LIKE HTML <h1>View Loans</h1>)
    # =====================================================
    st.markdown("""
    <div class="loans-header">
        <h1>📊 Loans Management</h1>
    </div>
    """, unsafe_allow_html=True)

    df = get_loans()

    # =====================================================
    # FILTER PANEL (LIKE LEFT SEARCH SIDEBAR IN HTML)
    # =====================================================
    with st.sidebar.expander("🔍 Advanced Search Filters", expanded=False):
        name = st.text_input("Borrower Name", value=st.session_state.search_name)
        mobile = st.text_input("Mobile", value=st.session_state.search_mobile)
        status = st.selectbox(
            "Loan Status",
            ["All", "Open", "Closed", "Pending", "Defaulted"],
            index=["All", "Open", "Closed", "Pending", "Defaulted"].index(st.session_state.search_status)
        )

        date_from = st.date_input("From Date")
        date_to = st.date_input("To Date")

        apply = st.button("Search Loans")
        
        if apply:
            st.session_state.search_name = name
            st.session_state.search_mobile = mobile
            st.session_state.search_status = status

    # Process and execute advanced filters on the dataframe
    if not df.empty:
        if st.session_state.search_name:
            if "borrower_name" in df.columns:
                df = df[df["borrower_name"].str.contains(st.session_state.search_name, case=False, na=False)]
            elif "name" in df.columns:
                df = df[df["name"].str.contains(st.session_state.search_name, case=False, na=False)]

        if st.session_state.search_mobile and "mobile" in df.columns:
            df = df[df["mobile"].astype(str).str.contains(st.session_state.search_mobile)]

        if st.session_state.search_status != "All" and "status" in df.columns:
            df = df[df["status"] == st.session_state.search_status]


    # =====================================================
    # PAGE ROUTING (LIKE LOANDISK MENU)
    # =====================================================
    if menu == "View All Loans":
        st.markdown("### 📁 All Loans")
        if df.empty:
            st.warning("No loan data found.")
        else:
            st.dataframe(df, use_container_width=True)

    elif menu == "Add Loan":
        st.markdown("### ➕ Add Loan")

        with st.form("add_loan"):
            borrower = st.text_input("Borrower Name")
            amount = st.number_input("Loan Amount", min_value=0.0, format="%.2f")
            interest = st.number_input("Interest Rate", min_value=0.0, format="%.2f")
            submit = st.form_submit_button("Create Loan")

            if submit:
                if not borrower:
                    st.error("Please enter a Borrower Name.")
                else:
                    try:
                        supabase.table("loans").insert({
                            "borrower_name": borrower,
                            "amount": amount,
                            "interest_rate": interest,
                            "created_at": str(datetime.now())
                        }).execute()
                        st.success("Loan created successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error saving data: {e}")

    elif menu == "Due Loans":
        st.markdown("### ⏰ Due Loans")
        if not df.empty and "status" in df.columns:
            st.dataframe(df[df["status"] == "Due"], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

    elif menu == "Missed Repayments":
        st.markdown("### ❌ Missed Repayments")
        if not df.empty and "status" in df.columns:
            st.dataframe(df[df["status"] == "Missed"], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

    elif menu == "Loans in Arrears":
        st.markdown("### ⚠ Loans in Arrears")
        if not df.empty and "status" in df.columns:
            st.dataframe(df[df["status"] == "Arrears"], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

    elif menu == "No Repayments":
        st.markdown("### 🚫 No Repayments")
        if not df.empty and "repayments_count" in df.columns:
            st.dataframe(df[df["repayments_count"] == 0], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

    elif menu == "Past Maturity Date":
        st.markdown("### 📅 Past Maturity Loans")
        if not df.empty and "status" in df.columns:
            st.dataframe(df[df["status"] == "Matured"], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

    elif menu == "Principal Outstanding":
        st.markdown("### 💵 Principal Outstanding")
        st.dataframe(df, use_container_width=True)

    elif menu == "1 Month Late Loans":
        st.markdown("### 📉 1 Month Late Loans")
        if not df.empty and "late_months" in df.columns:
            st.dataframe(df[df["late_months"] == 1], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

    elif menu == "3 Months Late Loans":
        st.markdown("### 📉 3 Months Late Loans")
        if not df.empty and "late_months" in df.columns:
            st.dataframe(df[df["late_months"] == 3], use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

    elif menu == "Loan Calculator":
        st.markdown("### 🧮 Loan Calculator")

        p = st.number_input("Principal", min_value=0.0, format="%.2f")
        r = st.number_input("Interest Rate (%)", min_value=0.0, format="%.2f")
        t = st.number_input("Time (months)", min_value=0.0, format="%.1f")

        if st.button("Calculate"):
            total = p + (p * r/100 * t/12)
            st.success(f"Total Payable: {total:,.2f}")

    elif menu == "Guarantors":
        st.markdown("### 🤝 Guarantors")
        st.info("Guarantor module coming soon")

    elif menu == "Loan Comments":
        st.markdown("### 💬 Loan Comments")
        st.info("Comments system coming soon")

    elif menu == "Approve Loans":
        st.markdown("### ✅ Approve Loans")
        st.info("Approval workflow coming soon")
