import streamlit as st
import pandas as pd
from datetime import datetime
from core.database import supabase


# =========================================================
# 🎨 LOANS PAGE STYLING (ENTERPRISE SAAS)
# =========================================================
def loans_styles():
    st.markdown("""
    <style>

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
# 📊 FETCH LOANS DATA
# =========================================================
def get_loans():
    try:
        res = supabase.table("loans").select("*").execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# =========================================================
# 🧠 SAFE DATA FORMATTER
# =========================================================
def format_loans_df(df):

    if df.empty:
        return df

    safe_cols = {
        "sn": "📌 SN",
        "loan_id_label": "🏷 Loan ID",
        "loan_type": "📂 Type",
        "principal": "💰 Principal",
        "interest": "📊 Interest",
        "total_repayable": "🧾 Total Payable",
        "amount_paid": "💵 Paid",
    }

    available = [c for c in safe_cols.keys() if c in df.columns]
    df = df[available].copy()

    df.rename(columns={k: v for k, v in safe_cols.items() if k in df.columns}, inplace=True)

    if "🧾 Total Payable" in df.columns and "💵 Paid" in df.columns:
        df["📉 Balance"] = df["🧾 Total Payable"] - df["💵 Paid"]

    return df


# =========================================================
# 📁 MAIN PAGE
# =========================================================
def show_loans():

    loans_styles()

    st.markdown("""
    <div class="loans-header">
        <h1>📊 Loans Management</h1>
    </div>
    """, unsafe_allow_html=True)

    df = get_loans()

    # =====================================================
    # 🧭 HORIZONTAL NAVIGATION (FIXED)
    # =====================================================
    menu = st.radio(
        "",
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
        ],
        horizontal=True
    )

    # =====================================================
    # 🔍 FILTERS (SIDEBAR ONLY)
    # =====================================================
    with st.sidebar.expander("🔍 Advanced Filters", expanded=False):

        name = st.text_input("Borrower Name")
        mobile = st.text_input("Mobile")
        status = st.selectbox("Status", ["All", "Open", "Closed", "Pending", "Defaulted"])

        apply = st.button("Apply Filters")

    if apply and not df.empty:

        if name and "borrower_name" in df.columns:
            df = df[df["borrower_name"].str.contains(name, case=False, na=False)]

        if mobile and "mobile" in df.columns:
            df = df[df["mobile"].astype(str).str.contains(mobile)]

        if status != "All" and "status" in df.columns:
            df = df[df["status"] == status]

    clean_df = format_loans_df(df)

    # =====================================================
    # 📊 ROUTING
    # =====================================================
    if menu == "View All Loans":
        st.markdown("### 📁 All Loans")
        st.dataframe(clean_df, use_container_width=True, hide_index=True)

    elif menu == "Add Loan":
        st.markdown("### ➕ Add Loan")

        with st.form("add_loan"):
            borrower = st.text_input("Borrower Name")
            principal = st.number_input("Principal", min_value=0.0)
            interest = st.number_input("Interest (%)", min_value=0.0)

            submit = st.form_submit_button("Create Loan")

            if submit:
                if not borrower:
                    st.error("Borrower required")
                else:
                    try:
                        supabase.table("loans").insert({
                            "borrower_name": borrower,
                            "principal": principal,
                            "interest": interest,
                            "created_at": str(datetime.now())
                        }).execute()

                        st.success("Loan created successfully")
                        st.rerun()

                    except Exception as e:
                        st.error(f"Error: {e}")

    elif menu == "Due Loans":
        st.markdown("### ⏰ Due Loans")
        st.dataframe(df[df.get("status") == "Due"], use_container_width=True, hide_index=True)

    elif menu == "Missed Repayments":
        st.markdown("### ❌ Missed Repayments")
        st.dataframe(df[df.get("status") == "Missed"], use_container_width=True, hide_index=True)

    elif menu == "Loans in Arrears":
        st.markdown("### ⚠ Loans in Arrears")
        st.dataframe(df[df.get("status") == "Arrears"], use_container_width=True, hide_index=True)

    elif menu == "No Repayments":
        st.markdown("### 🚫 No Repayments")
        st.dataframe(df[df.get("repayments_count") == 0], use_container_width=True, hide_index=True)

    elif menu == "Past Maturity Date":
        st.markdown("### 📅 Past Maturity Loans")
        st.dataframe(df[df.get("status") == "Matured"], use_container_width=True, hide_index=True)

    elif menu == "Principal Outstanding":
        st.markdown("### 💵 Principal Outstanding")
        st.dataframe(clean_df, use_container_width=True, hide_index=True)

    elif menu == "1 Month Late Loans":
        st.markdown("### 📉 1 Month Late Loans")
        st.dataframe(df[df.get("late_months") == 1], use_container_width=True, hide_index=True)

    elif menu == "3 Months Late Loans":
        st.markdown("### 📉 3 Months Late Loans")
        st.dataframe(df[df.get("late_months") == 3], use_container_width=True, hide_index=True)

    elif menu == "Loan Calculator":
        st.markdown("### 🧮 Loan Calculator")

        p = st.number_input("Principal", 0.0)
        r = st.number_input("Interest %", 0.0)
        t = st.number_input("Months", 0.0)

        if st.button("Calculate"):
            total = p + (p * r/100 * t/12)
            st.success(f"Total Payable: {total:,.2f}")

    else:
        st.info("Coming soon...")
