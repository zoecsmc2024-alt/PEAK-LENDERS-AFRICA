# Complete Streamlit Payroll Page

```python
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
# 📊 FETCH PAYROLL DATA
# =========================================================
def get_payrolls():
    try:
        res = supabase.table("payroll").select("*").execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception as e:
        st.error(f"Payroll fetch error: {e}")
        return pd.DataFrame()


# =========================================================
# 👥 FETCH STAFF
# =========================================================
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

    df = df[available].copy()

    df.rename(columns={
        k: v for k, v in safe_cols.items() if k in df.columns
    }, inplace=True)

    money_cols = [
        "💰 Basic Salary",
        "➕ Allowances",
        "➖ Deductions",
        "🧾 Net Salary"
    ]

    for col in money_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# =========================================================
# 📁 PAYROLL PAGE
# =========================================================
def show_payroll():

    payroll_styles()

    st.markdown("""
    <div class="payroll-header">
        <h1>💰 Payroll Management</h1>
    </div>
    """, unsafe_allow_html=True)

    payroll_df = get_payrolls()

    # =====================================================
    # 🧭 NAVIGATION
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
    # 🔍 SEARCH + ACTIONS
    # =====================================================
    search = st.text_input(
        "🔍 Search Payroll",
        placeholder="Search staff name or payroll month..."
    )

    if search and not payroll_df.empty:

        if "staff_name" in payroll_df.columns:
            payroll_df = payroll_df[
                payroll_df["staff_name"].astype(str).str.contains(
                    search,
                    case=False,
                    na=False
                )
            ]

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        if not payroll_df.empty:
            st.download_button(
                "⬇ Export CSV",
                data=payroll_df.to_csv(index=False),
                file_name="payroll.csv",
                mime="text/csv",
                use_container_width=True
            )

    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()

    with col3:
        total_payroll = 0

        if "net_salary" in payroll_df.columns:
            total_payroll = pd.to_numeric(
                payroll_df["net_salary"],
                errors="coerce"
            ).fillna(0).sum()

        st.metric("💵 Total Payroll", f"{total_payroll:,.2f}")

    st.markdown("---")

    clean_df = format_payroll_df(payroll_df)

    # =====================================================
    # 📊 VIEW PAYROLL
    # =====================================================
    if menu == "View Payroll":

        st.markdown("### 📋 Payroll Records")

        if clean_df.empty:
            st.info("No payroll records found.")
        else:
            st.dataframe(
                clean_df,
                use_container_width=True,
                hide_index=True
            )

            st.markdown("---")

            col1, col2, col3 = st.columns(3)

            if "💰 Basic Salary" in clean_df.columns:
                col1.metric(
                    "💰 Total Salaries",
                    f"{clean_df['💰 Basic Salary'].sum():,.2f}"
                )

            if "➕ Allowances" in clean_df.columns:
                col2.metric(
                    "➕ Total Allowances",
                    f"{clean_df['➕ Allowances'].sum():,.2f}"
                )

            if "🧾 Net Salary" in clean_df.columns:
                col3.metric(
                    "🧾 Total Net Payroll",
                    f"{clean_df['🧾 Net Salary'].sum():,.2f}"
                )

    # =====================================================
    # ➕ ADD PAYROLL
    # =====================================================
    elif menu == "Add Payroll":

        st.markdown("### ➕ Add Payroll")

        staff_data = get_staff()

        staff_map = {
            s["full_name"]: s["id"]
            for s in staff_data
        }

        with st.form("add_payroll_form"):

            staff_name = st.selectbox(
                "👤 Select Staff",
                list(staff_map.keys()) if staff_map else []
            )

            salary_month = st.text_input(
                "📅 Salary Month",
                placeholder="Example: May 2026"
            )

            basic_salary = st.number_input(
                "💰 Basic Salary",
                min_value=0.0,
                step=1000.0
            )

            allowances = st.number_input(
                "➕ Allowances",
                min_value=0.0,
                step=1000.0
            )

            deductions = st.number_input(
                "➖ Deductions",
                min_value=0.0,
                step=1000.0
            )

            payment_status = st.selectbox(
                "📌 Payment Status",
                ["Pending", "Paid"]
            )

            payment_date = st.date_input(
                "📅 Payment Date",
                value=datetime.today()
            )

            submit = st.form_submit_button("💾 Save Payroll")

            if submit:

                if not staff_name:
                    st.error("Please select staff")

                elif not salary_month:
                    st.error("Salary month required")

                else:
                    try:

                        net_salary = (
                            basic_salary + allowances - deductions
                        )

                        supabase.table("payroll").insert({
                            "staff_id": staff_map[staff_name],
                            "staff_name": staff_name,
                            "salary_month": salary_month,
                            "basic_salary": basic_salary,
                            "allowances": allowances,
                            "deductions": deductions,
                            "net_salary": net_salary,
                            "payment_status": payment_status,
                            "payment_date": str(payment_date),
                            "created_at": str(datetime.now())
                        }).execute()

                        st.success("Payroll added successfully")
                        st.rerun()

                    except Exception as e:
                        st.error(f"Payroll save error: {e}")

    # =====================================================
    # ⏳ PENDING PAYROLL
    # =====================================================
    elif menu == "Pending Payroll":

        st.markdown("### ⏳ Pending Payroll")

        if payroll_df.empty:
            st.info("No payroll records available")
        else:

            pending_df = payroll_df.copy()

            if "payment_status" in pending_df.columns:
                pending_df = pending_df[
                    pending_df["payment_status"] == "Pending"
                ]

            if pending_df.empty:
                st.success("🎉 No pending payroll")
            else:
                st.dataframe(
                    format_payroll_df(pending_df),
                    use_container_width=True,
                    hide_index=True
                )

    # =====================================================
    # ✅ PAID PAYROLL
    # =====================================================
    elif menu == "Paid Payroll":

        st.markdown("### ✅ Paid Payroll")

        if payroll_df.empty:
            st.info("No payroll records available")
        else:

            paid_df = payroll_df.copy()

            if "payment_status" in paid_df.columns:
                paid_df = paid_df[
                    paid_df["payment_status"] == "Paid"
                ]

            if paid_df.empty:
                st.info("No paid payroll found")
            else:
                st.dataframe(
                    format_payroll_df(paid_df),
                    use_container_width=True,
                    hide_index=True
                )

    # =====================================================
    # 📊 PAYROLL REPORT
    # =====================================================
    elif menu == "Payroll Report":

        st.markdown("### 📊 Payroll Report")

        if payroll_df.empty:
            st.warning("No payroll data available")
        else:

            report_df = payroll_df.copy()

            if "net_salary" in report_df.columns:
                report_df["net_salary"] = pd.to_numeric(
                    report_df["net_salary"],
                    errors="coerce"
                ).fillna(0)

            total_net = report_df["net_salary"].sum() if "net_salary" in report_df.columns else 0
            avg_net = report_df["net_salary"].mean() if "net_salary" in report_df.columns else 0

            col1, col2 = st.columns(2)

            col1.metric("💵 Total Payroll", f"{total_net:,.2f}")
            col2.metric("📈 Average Salary", f"{avg_net:,.2f}")

            st.markdown("---")

            st.dataframe(
                format_payroll_df(report_df),
                use_container_width=True,
                hide_index=True
            )

    # =====================================================
    # 🧮 PAYROLL CALCULATOR
    # =====================================================
    elif menu == "Payroll Calculator":

        st.markdown("### 🧮 Payroll Calculator")

        basic = st.number_input(
            "💰 Basic Salary",
            min_value=0.0
        )

        allowance = st.number_input(
            "➕ Allowances",
            min_value=0.0
        )

        deduction = st.number_input(
            "➖ Deductions",
            min_value=0.0
        )

        if st.button("Calculate Payroll"):

            gross = basic + allowance
            net = gross - deduction

            c1, c2, c3 = st.columns(3)

            c1.metric("💰 Gross Salary", f"{gross:,.2f}")
            c2.metric("➖ Deductions", f"{deduction:,.2f}")
            c3.metric("🧾 Net Salary", f"{net:,.2f}")

    else:
        st.info("Payroll module coming soon...")

```
