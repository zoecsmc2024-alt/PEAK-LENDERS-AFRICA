import streamlit as st
import pandas as pd
from datetime import datetime
from core.database import supabase


# =========================================================
# 🎨 LOANS PAGE STYLING (ENTERPRISE SAAS - VISIBILITY FIX)
# =========================================================
def loans_styles():
    st.markdown("""
    <style>

    /* Header Panel styling */
    .loans-header {
        background: linear-gradient(90deg, #0A192F, #112240);
        padding: 14px 18px;
        border-radius: 10px;
        color: white;
        margin-bottom: 25px;
    }

    .loans-header h1 {
        margin: 0;
        font-size: 22px;
        color: #FFFFFF !important;
    }

    /* Force visibility on selection box labels & components */
    div[data-testid="stSelectbox"] label p {
        color: #1E293B !important;
        font-weight: 600 !important;
        font-size: 15px;
    }

    /* Primary buttons styling */
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
    # 🧭 CLEAN SUB-HEADER NAVIGATION DROPDOWN
    # =====================================================
    # Replaced st.radio(horizontal=True) to fix text cutoff/wrapping bugs
    menu = st.selectbox(
        "📂 Select Loans Module Category",
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

    st.markdown("---")

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
    
        # =====================================================
        # 📥 FETCH BORROWERS (FOR DROPDOWN)
        # =====================================================
        try:
            res = supabase.table("borrowers").select("id, borrower_name").execute()
            borrowers = res.data if res.data else []
        except Exception as e:
            st.error(f"Error loading borrowers: {e}")
            borrowers = []
    
        borrower_map = {b["borrower_name"]: b["id"] for b in borrowers}
    
        with st.form("add_loan"):
    
            # =========================
            # 👤 BORROWER DROPDOWN
            # =========================
            borrower_name = st.selectbox(
                "Borrower",
                list(borrower_map.keys()) if borrower_map else []
            )
    
            principal = st.number_input("Principal", min_value=0.0)
            interest = st.number_input("Interest (%)", min_value=0.0)
    
            # =========================
            # 📅 DATES
            # =========================
            start_date = st.date_input("Start Date", value=datetime.today())
            end_date = st.date_input("End Date")
    
            submit = st.form_submit_button("Create Loan")
    
            if submit:
    
                if not borrower_name:
                    st.error("Borrower required")
    
                elif not end_date:
                    st.error("End date required")
    
                else:
                    try:
                        supabase.table("loans").insert({
                            "borrower_id": borrower_map[borrower_name],
                            "borrower_name": borrower_name,  # optional redundancy (safe for reporting)
                            "principal": principal,
                            "interest": interest,
                            "start_date": str(start_date),
                            "end_date": str(end_date),
                            "created_at": str(datetime.now())
                        }).execute()
    
                        st.success("Loan created successfully")
                        st.rerun()
    
                    except Exception as e:
                        st.error(f"Error: {e}")

    elif menu == "Due Loans":
        st.markdown("### ⏰ Due Loans")
    
        if df.empty:
            st.warning("No loan data available.")
        else:
            filtered_df = df.copy()
    
            # =====================================================
            # 🎯 STATUS FILTER (PRIMARY)
            # =====================================================
            if "status" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df["status"] == "Due"]
    
            # =====================================================
            # 📅 OPTIONAL: DUE DATE INTELLIGENCE (SMART UPGRADE)
            # =====================================================
            if "end_date" in filtered_df.columns:
                try:
                    filtered_df["end_date"] = pd.to_datetime(filtered_df["end_date"], errors="coerce")
                    filtered_df = filtered_df.sort_values(by="end_date", ascending=True)
                except Exception:
                    pass
    
            # =====================================================
            # 📊 DISPLAY LOGIC
            # =====================================================
            if filtered_df.empty:
                st.info("🎉 No due loans at the moment.")
            else:
                st.dataframe(
                    format_loans_df(filtered_df),
                    use_container_width=True,
                    hide_index=True
                )

    elif menu == "Missed Repayments":
        st.markdown("### ❌ Missed Repayments")
    
        if df.empty:
            st.warning("No loan data available.")
        else:
            filtered_df = df.copy()
    
            # =====================================================
            # 🎯 FILTER MISSED STATUS
            # =====================================================
            if "status" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df["status"] == "Missed"]
    
            # =====================================================
            # 💰 FORMAT AMOUNTS WITH COMMAS
            # =====================================================
            money_cols = ["principal", "total_repayable", "amount_paid"]
    
            for col in money_cols:
                if col in filtered_df.columns:
                    filtered_df[col] = pd.to_numeric(filtered_df[col], errors="coerce")
                    filtered_df[col] = filtered_df[col].apply(
                        lambda x: f"{x:,.2f}" if pd.notnull(x) else x
                    )
    
            # =====================================================
            # 📊 DISPLAY
            # =====================================================
            if filtered_df.empty:
                st.info("🎉 No missed repayments.")
            else:
                st.dataframe(
                    format_loans_df(filtered_df),
                    use_container_width=True,
                    hide_index=True
                )
    elif menu == "Loans in Arrears":
        st.markdown("### ⚠ Loans in Arrears")
    
        if df.empty:
            st.warning("No loan data available.")
        else:
            filtered_df = df.copy()
    
            # =====================================================
            # 🎯 FILTER ARREARS
            # =====================================================
            if "status" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df["status"] == "Arrears"]
    
            # =====================================================
            # 📅 OPTIONAL: SORT BY MOST CRITICAL FIRST
            # =====================================================
            if "end_date" in filtered_df.columns:
                try:
                    filtered_df["end_date"] = pd.to_datetime(filtered_df["end_date"], errors="coerce")
                    filtered_df = filtered_df.sort_values(by="end_date", ascending=True)
                except Exception:
                    pass
    
            # =====================================================
            # 📊 DISPLAY
            # =====================================================
            if filtered_df.empty:
                st.info("🎉 No loans currently in arrears.")
            else:
                st.dataframe(
                    format_loans_df(filtered_df),
                    use_container_width=True,
                    hide_index=True
                )

    elif menu == "No Repayments":
        st.markdown("### 🚫 No Repayments")
    
        if df.empty:
            st.warning("No loan data available.")
        else:
            filtered_df = df.copy()
    
            # =====================================================
            # 🎯 FILTER: NO REPAYMENTS
            # =====================================================
            if "repayments_count" in filtered_df.columns:
                filtered_df["repayments_count"] = pd.to_numeric(
                    filtered_df["repayments_count"], errors="coerce"
                ).fillna(0)
    
                filtered_df = filtered_df[filtered_df["repayments_count"] == 0]
            else:
                st.info("Repayments data not available.")
                filtered_df = pd.DataFrame()
    
            # =====================================================
            # 📊 DISPLAY
            # =====================================================
            if filtered_df.empty:
                st.info("🎉 All loans have at least one repayment.")
            else:
                st.dataframe(
                    format_loans_df(filtered_df),
                    use_container_width=True,
                    hide_index=True
                )
    elif menu == "Past Maturity Date":
        st.markdown("### 📅 Past Maturity Loans")
        st.dataframe(df[df.get("status") == "Matured"] if not df.empty else df, use_container_width=True, hide_index=True)

    elif menu == "Principal Outstanding":
        st.markdown("### 💵 Principal Outstanding")
        st.dataframe(clean_df, use_container_width=True, hide_index=True)

    elif menu == "1 Month Late Loans":
        st.markdown("### 📉 1 Month Late Loans")
        st.dataframe(df[df.get("late_months") == 1] if not df.empty else df, use_container_width=True, hide_index=True)

    elif menu == "3 Months Late Loans":
        st.markdown("### 📉 3 Months Late Loans")
        st.dataframe(df[df.get("late_months") == 3] if not df.empty else df, use_container_width=True, hide_index=True)

    elif menu == "Loan Calculator":
        st.markdown("### 🧮 Loan Calculator")

        p = st.number_input("Principal", 0.0)
        r = st.number_input("Interest %", 0.0)
        t = st.number_input("Months", 0.0)

        if st.button("Calculate"):
            total = p + (p * r/100 * t/12)
            st.success(f"Total Payable: {total:,.2f}")

    else:
        st.info(f"{menu} module coming soon...")
