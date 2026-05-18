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
# 📁 MAIN LOANS PAGE
# =========================================================
def show_loans():

    loans_styles()

    menu = loans_sidebar()

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

        name = st.text_input("Borrower Name")
        mobile = st.text_input("Mobile")
        status = st.selectbox(
            "Loan Status",
            ["All", "Open", "Closed", "Pending", "Defaulted"]
        )

        date_from = st.date_input("From Date")
        date_to = st.date_input("To Date")

        apply = st.button("Search Loans")

    # Apply filters (simple demo logic)
    if apply and not df.empty:

        if name:
            df = df[df["borrower_name"].str.contains(name, case=False, na=False)]

        if mobile:
            df = df[df["mobile"].astype(str).str.contains(mobile)]

        if status != "All":
            df = df[df["status"] == status]


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
            amount = st.number_input("Loan Amount")
            interest = st.number_input("Interest Rate")
            submit = st.form_submit_button("Create Loan")

            if submit:
                try:
                    supabase.table("loans").insert({
                        "borrower_name": borrower,
                        "amount": amount,
                        "interest_rate": interest,
                        "created_at": str(datetime.now())
                    }).execute()
                    st.success("Loan created successfully")
                except Exception as e:
                    st.error(e)

    elif menu == "Due Loans":
        st.markdown("### ⏰ Due Loans")
        st.dataframe(df[df.get("status") == "Due"] if not df.empty else df)

    elif menu == "Missed Repayments":
        st.markdown("### ❌ Missed Repayments")
        st.dataframe(df[df.get("status") == "Missed"] if not df.empty else df)

    elif menu == "Loans in Arrears":
        st.markdown("### ⚠ Loans in Arrears")
        st.dataframe(df[df.get("status") == "Arrears"] if not df.empty else df)

    elif menu == "No Repayments":
        st.markdown("### 🚫 No Repayments")
        st.dataframe(df[df.get("repayments_count") == 0] if not df.empty else df)

    elif menu == "Past Maturity Date":
        st.markdown("### 📅 Past Maturity Loans")
        st.dataframe(df[df.get("status") == "Matured"] if not df.empty else df)

    elif menu == "Principal Outstanding":
        st.markdown("### 💵 Principal Outstanding")
        st.dataframe(df)

    elif menu == "1 Month Late Loans":
        st.markdown("### 📉 1 Month Late Loans")
        st.dataframe(df[df.get("late_months") == 1] if not df.empty else df)

    elif menu == "3 Months Late Loans":
        st.markdown("### 📉 3 Months Late Loans")
        st.dataframe(df[df.get("late_months") == 3] if not df.empty else df)

    elif menu == "Loan Calculator":
        st.markdown("### 🧮 Loan Calculator")

        p = st.number_input("Principal")
        r = st.number_input("Interest Rate (%)")
        t = st.number_input("Time (months)")

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
