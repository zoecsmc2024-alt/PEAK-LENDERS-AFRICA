import streamlit as st
import pandas as pd
from datetime import datetime
from core.database import supabase


# =========================================================
# 🎨 PAYROLL PAGE STYLING
# =========================================================
def payroll_styles():
    st.markdown("""
    <style>
    .payroll-header {
        background: linear-gradient(90deg, #0A192F, #112240);
        padding: 14px 18px;
        border-radius: 12px;
        color: white;
        margin-bottom: 15px;
    }
    .payroll-header h1 {
        margin: 0;
        font-size: 24px;
    }
    .stButton > button {
        background: #2563EB;
        color: white;
        border-radius: 8px;
        height: 40px;
        font-weight: 600;
        border: none;
    }
    .stButton > button:hover {
        background: #1D4ED8;
        color: white;
    }
    div[data-baseweb="select"] > div {
        border-radius: 10px;
    }
    </style>
    """, unsafe_allow_html=True)


# =========================================================
# 📊 DATA FETCHERS
# =========================================================
def get_payrolls():
    try:
        res = supabase.table("payroll").select("*").execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception as e:
        st.error(f"Payroll fetch error: {e}")
        return pd.DataFrame()


def get_staff():
    try:
        res = supabase.table("staff").select("id, full_name").execute()
        return res.data if res.data else []
    except Exception as e:
        st.error(f"Staff fetch error: {e}")
        return []


# =========================================================
# 🧠 FORMAT PAYROLL DATAFRAME
# =========================================================
def format_payroll_df(df):
    if df.empty:
        return df

    safe_cols = {
        "staff_name": "👤 Staff",
        "salary_month": "📅 Month",
        "basic_salary": "💰 Basic Salary",
        "allowances": "➕ Allowances",
        "deductions": "➖ Deductions",
        "net_salary": "🧾 Net Salary",
        "payment_status": "📌 Status",
        "created_at": "🕒 Created"
    }

    available = [c for c in safe_cols.keys() if c in df.columns]
    df_clean = df[available].copy()

    df_clean.rename(columns={k: v for k, v in safe_cols.items() if k in df_clean.columns}, inplace=True)

    money_cols = ["💰 Basic Salary", "➕ Allowances", "➖ Deductions", "🧾 Net Salary"]
    for col in money_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").fillna(0)

    return df_clean


# =========================================================
# 📁 MAIN PAYROLL PAGE
# =========================================================
def show_payroll():
    payroll_styles()

    st.markdown("""
    <div class="payroll-header">
        <h1>💰 Payroll Management</h1>
    </div>
    """, unsafe_allow_html=True)

    # Initial Data Load
    raw_df = get_payrolls()
    working_df = raw_df.copy() if not raw_df.empty else pd.DataFrame()

    # =====================================================
    # 🧭 MODULE NAVIGATION
    # =====================================================
    menu = st.selectbox(
        "📂 Payroll Module",
        [
            "View Payroll",
            "Add Payroll",
            "Pending Payroll",
            "Paid Payroll",
            "Payroll Report",
            "Payroll Calculator"
        ]
    )

    st.markdown("---")

    # =====================================================
    # 🔍 FILTER LOGIC BY MENU VIEW
    # =====================================================
    if not working_df.empty and "payment_status" in working_df.columns:
        if menu == "Pending Payroll":
            working_df = working_df[working_df["payment_status"] == "Pending"]
        elif menu == "Paid Payroll":
            working_df = working_df[working_df["payment_status"] == "Paid"]

    # =====================================================
    # 🔍 SEARCH + GLOBAL ACTIONS BLOCK
    # =====================================================
    search = st.text_input("🔍 Search Payroll", placeholder="Search staff name or payroll month...")

    if search and not working_df.empty:
        search_filter = pd.Series(False, index=working_df.index)
        if "staff_name" in working_df.columns:
            search_filter |= working_df["staff_name"].astype(str).str.contains(search, case=False, na=False)
        if "salary_month" in working_df.columns:
            search_filter |= working_df["salary_month"].astype(str).str.contains(search, case=False, na=False)
        working_df = working_df[search_filter]

    # Cleaned DataFrame layout representation for UI rendering
    clean_df = format_payroll_df(working_df)

    # UI Action Grid
    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        if not clean_df.empty:
            st.download_button(
                "⬇ Export CSV",
                data=clean_df.to_csv(index=False),
                file_name=f"payroll_{menu.lower().replace(' ', '_')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.button("⬇ Export CSV", disabled=True, use_container_width=True)

    with col2:
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.rerun()

    with col3:
        total_payroll = 0
        if "net_salary" in working_df.columns:
            total_payroll = pd.to_numeric(working_df["net_salary"], errors="coerce").fillna(0).sum()
        st.metric("💵 Subtotal/View Payroll", f"UGX {total_payroll:,.0f}")

    st.markdown("---")

    # =====================================================
    # 📊 VIEW PAYROLL
    # =====================================================
    if menu == "View Payroll":
        st.markdown("### 📋 Payroll Records")
        if clean_df.empty:
            st.info("No payroll records found.")
        else:
            st.dataframe(clean_df, use_container_width=True, hide_index=True)

            st.markdown("---")
            c1, c2, c3 = st.columns(3)
            if "💰 Basic Salary" in clean_df.columns:
                c1.metric("💰 Total Base Salaries", f"UGX {clean_df['💰 Basic Salary'].sum():,.0f}")
            if "➕ Allowances" in clean_df.columns:
                c2.metric("➕ Total Allowances", f"UGX {clean_df['➕ Allowances'].sum():,.0f}")
            if "🧾 Net Salary" in clean_df.columns:
                c3.metric("🧾 Total Net Payroll", f"UGX {clean_df['🧾 Net Salary'].sum():,.0f}")

    # =====================================================
    # ➕ ADD PAYROLL
    # =====================================================
    elif menu == "Add Payroll":
        st.markdown("### ➕ Add New Payroll Entry")
        staff_data = get_staff()
        staff_map = {s["full_name"]: s["id"] for s in staff_data}

        with st.form("add_payroll_form", clear_on_submit=True):
            staff_name = st.selectbox("👤 Select Staff member", list(staff_map.keys()) if staff_map else ["No staff found"])
            salary_month = st.text_input("📅 Salary Month", placeholder="e.g., May 2026")

            col_a, col_b, col_c = st.columns(3)
            with col_a:
                basic_salary = st.number_input("💰 Basic Salary", min_value=0.0, step=50000.0)
            with col_b:
                allowances = st.number_input("➕ Allowances", min_value=0.0, step=10000.0)
            with col_c:
                deductions = st.number_input("➖ Deductions", min_value=0.0, step=10000.0)

            payment_status = st.selectbox("📌 Payment Status", ["Pending", "Paid"])
            payment_date = st.date_input("📅 Payment Date", value=datetime.today())

            submit = st.form_submit_button("💾 Save Payroll Record")

            if submit:
                if not staff_map:
                    st.error("Cannot save. Staff list is missing.")
                elif not salary_month:
                    st.error("Salary month input is required.")
                else:
                    try:
                        net_salary = basic_salary + allowances - deductions
                        payload = {
                            "staff_id": staff_map[staff_name],
                            "staff_name": staff_name,
                            "salary_month": salary_month,
                            "basic_salary": basic_salary,
                            "allowances": allowances,
                            "deductions": deductions,
                            "net_salary": net_salary,
                            "payment_status": payment_status,
                            "payment_date": str(payment_date),
                            "created_at": datetime.now().isoformat()
                        }
                        supabase.table("payroll").insert(payload).execute()
                        st.success(f"Successfully tracked payroll entry for {staff_name}!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Payroll database transaction error: {e}")

    # =====================================================
    # ⏳ PENDING PAYROLL VIEW
    # =====================================================
    elif menu == "Pending Payroll":
        st.markdown("### ⏳ Outstanding Payroll Entries")
        if clean_df.empty:
            st.success("🎉 No pending items to process!")
        else:
            st.dataframe(clean_df, use_container_width=True, hide_index=True)

    # =====================================================
    # ✅ PAID PAYROLL VIEW
    # =====================================================
    elif menu == "Paid Payroll":
        st.markdown("### ✅ Disbursed Records")
        if clean_df.empty:
            st.info("No disbursed records matched this view.")
        else:
            st.dataframe(clean_df, use_container_width=True, hide_index=True)

    # =====================================================
    # 📊 PAYROLL REPORT
    # =====================================================
    elif menu == "Payroll Report":
        st.markdown("### 📊 Summary Statistics & Analytics")
        if clean_df.empty:
            st.warning("No records evaluated for report analytics.")
        else:
            net_col = clean_df["🧾 Net Salary"] if "🧾 Net Salary" in clean_df.columns else pd.Series([0])
            total_net = net_col.sum()
            avg_net = net_col.mean()

            rep_1, rep_2 = st.columns(2)
            rep_1.metric("📊 Total Aggregate Cost", f"UGX {total_net:,.0f}")
            rep_2.metric("📈 Mean Average Net Payout", f"UGX {avg_net:,.0f}")

            st.markdown("---")
            st.dataframe(clean_df, use_container_width=True, hide_index=True)

    # =====================================================
    # 🧮 PAYROLL CALCULATOR
    # =====================================================
    elif menu == "Payroll Calculator":
        st.markdown("### 🧮 Fast Sandbox Calculator")
        calc_b = st.number_input("💰 Sandbox Basic Salary", min_value=0.0, step=50000.0)
        calc_a = st.number_input("➕ Sandbox Allowances", min_value=0.0, step=10000.0)
        calc_d = st.number_input("➖ Sandbox Deductions", min_value=0.0, step=10000.0)

        if st.button("Run Model Calculations"):
            gross = calc_b + calc_a
            net = gross - calc_d

            rc1, rc2, rc3 = st.columns(3)
            rc1.metric("💰 Gross Income", f"UGX {gross:,.0f}")
            rc2.metric("➖ Gross Deductions", f"UGX {calc_d:,.0f}")
            rc3.metric("🧾 Net Liquid Distribution", f"UGX {net:,.0f}")
