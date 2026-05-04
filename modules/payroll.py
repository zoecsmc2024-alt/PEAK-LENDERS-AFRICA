# =================================
# 🏢 Enterprise Payroll Engine (Fixed Truth-Value Errors)
# =================================
import streamlit as st
import pandas as pd
from datetime import datetime
import uuid

def show_payroll_enterprise():
    tenant = st.session_state.get("tenant_id")
    role = st.session_state.get("role")
    if not tenant or role != "Admin":
        st.error("🔒 Restricted: Only Admins with valid session can access payroll")
        return

    st.markdown("<h2 style='color:#4A90E2;'>🧾 Enterprise Payroll</h2>", unsafe_allow_html=True)

    # -----------------------------
    # 1. Load Payroll & Employees
    # -----------------------------
    payroll_df = get_cached_data("payroll")
    if payroll_df is None:
        payroll_df = pd.DataFrame()
    emp_df = get_cached_data("employees")
    if emp_df is None:
        emp_df = pd.DataFrame()

    # Standardize column names
    payroll_df.columns = payroll_df.columns.astype(str).str.strip().str.replace(" ", "_")
    emp_df.columns = emp_df.columns.astype(str).str.strip().str.replace(" ", "_")
    # Filter by tenant
    payroll_df = payroll_df[payroll_df.get("tenant_id", "") == str(tenant)].copy() if not payroll_df.empty else pd.DataFrame()
    emp_df = emp_df[emp_df.get("tenant_id", "") == str(tenant)].copy() if not emp_df.empty else pd.DataFrame()

    # -----------------------------
    # 2. Payroll Calculations
    # -----------------------------
    def calculate_paye(gross):
        if gross <= 235000: return 0
        elif gross <= 335000: return (gross - 235000) * 0.10
        elif gross <= 410000: return 10000 + (gross - 335000) * 0.20
        else: return 25000 + (gross - 410000) * 0.30

    def calculate_nssf(gross):
        n5 = gross * 0.05
        n10 = gross * 0.10
        return round(n5), round(n10), round(n5+n10)

    def calculate_lst(gross):
        return round(100000/12) if gross*12 > 1200000 else 0

    def compute_payroll(basic, arrears, absent, advance, other):
        gross = round(float(basic) + float(arrears) - float(absent))
        lst = calculate_lst(gross)
        n5, n10, n15 = calculate_nssf(gross)
        paye = round(calculate_paye(gross))
        net = gross - (paye + lst + n5 + advance + other)
        return {"gross": gross, "lst": lst, "n5": n5, "n10": n10, "n15": n15, "paye": paye, "net": net}

    # -----------------------------
    # 3. Process Payroll Form
    # -----------------------------
    tab_employees, tab_process, tab_history = st.tabs([
        "🧑‍💼 Employees",
        "💳 Process Payroll",
        "📜 Payroll History"
    ])

    # -----------------------------
    # Tab 1: Employees
    # -----------------------------
    with tab_employees:
        st.subheader("Manage Employees")
        
        # Add new employee form
        with st.form("add_employee_form"):
            name = st.text_input("Employee Name")
            tin = st.text_input("TIN")
            desig = st.text_input("Designation")
            mob = st.text_input("Mobile No.")
            acc = st.text_input("Account No.")
            nssf = st.text_input("NSSF No.")
            
            if st.form_submit_button("Add Employee"):
                if not name:
                    st.error("Employee name is required")
                else:
                    emp_row = pd.DataFrame([{
                        "employee_id": str(uuid.uuid4()),
                        "employee_name": name,
                        "tin": tin,
                        "designation": desig,
                        "mob_no": mob,
                        "account_no": acc,
                        "nssf_no": nssf,
                        "tenant_id": str(st.session_state.get("tenant_id"))
                    }])
                    save_data_saas("employees", emp_row)
                    st.success(f"✅ Employee {name} added")
        
        # Show existing employees
        emp_df = get_data("employees")
        if not emp_df.empty:
            st.dataframe(emp_df[["employee_name", "designation", "tin", "mob_no"]])
        else:
            st.info("No employees added yet.")

    with tab_process:
        with st.form("payroll_form", clear_on_submit=True):
            st.subheader("👤 Employee Info")
            emp_options = emp_df.get("employee_name", []).tolist() if not emp_df.empty else []
            selected_emp = st.selectbox("Select Employee", emp_options)
            emp_record = {}
            if not emp_df.empty:
                temp = emp_df[emp_df["employee_name"] == selected_emp]
                if not temp.empty:
                    emp_record = temp.iloc[0].to_dict()

            c1, c2, c3 = st.columns(3)
            f_tin = c1.text_input("TIN", emp_record.get("tin",""))
            f_desig = c2.text_input("Designation", emp_record.get("designation",""))
            f_mob = c3.text_input("Mobile No.", emp_record.get("mob_no",""))

            c4, c5 = st.columns(2)
            f_acc = c4.text_input("Account No.", emp_record.get("account_no",""))
            f_nssf_no = c5.text_input("NSSF No.", emp_record.get("nssf_no",""))

            st.subheader("💰 Earnings & Deductions")
            c6, c7, c8 = st.columns(3)
            f_arrears = c6.number_input("Arrears", min_value=0.0)
            f_basic = c7.number_input("Basic Salary", min_value=0.0)
            f_absent = c8.number_input("Absent Deduction", min_value=0.0)

            c9, c10 = st.columns(2)
            f_adv = c9.number_input("Advance / DRS", min_value=0.0)
            f_other = c10.number_input("Other Deductions", min_value=0.0)

            if st.form_submit_button("💳 Preview & Save"):
                if not selected_emp or f_basic <= 0:
                    st.error("Enter employee and valid salary.")
                else:
                    # Check duplicate payroll for same month
                    month_str = datetime.now().strftime("%Y-%m")
                    duplicate_check = payroll_df[
                        (payroll_df.get("employee","") == selected_emp) &
                        (payroll_df.get("month","") == month_str)
                    ] if not payroll_df.empty else pd.DataFrame()

                    if not duplicate_check.empty:
                        st.warning("⚠️ Payroll for this employee already exists this month.")
                    else:
                        calc = compute_payroll(f_basic, f_arrears, f_absent, f_adv, f_other)
                        new_row = pd.DataFrame([{
                            "payroll_id": str(uuid.uuid4()),
                            "employee": selected_emp,
                            "tin": f_tin,
                            "designation": f_desig,
                            "mob_no": f_mob,
                            "account_no": f_acc,
                            "nssf_no": f_nssf_no,
                            "arrears": f_arrears,
                            "basic_salary": f_basic,
                            "absent_deduction": f_absent,
                            "gross_salary": calc['gross'],
                            "lst": calc['lst'],
                            "paye": calc['paye'],
                            "nssf_5": calc['n5'],
                            "nssf_10": calc['n10'],
                            "nssf_15": calc['n15'],
                            "advance_drs": f_adv,
                            "other_deductions": f_other,
                            "net_pay": calc['net'],
                            "date": datetime.now(),
                            "month": month_str,
                            "tenant_id": str(tenant)
                        }])
                        if save_data_saas("payroll", new_row):
                            st.success(f"✅ Payroll for {selected_emp} saved successfully!")

    # -----------------------------
    # 4. Payroll History & Reporting
    # -----------------------------
    with tab_history:
        if payroll_df.empty:
            st.info("No payroll records found.")
        else:
            # Sort latest first
            payroll_df = payroll_df.sort_values(by=["date"], ascending=False)
            st.dataframe(payroll_df, use_container_width=True)

            # Export & Printable
            csv = payroll_df.to_csv(index=False).encode("utf-8")
            st.download_button("📄 Download CSV", data=csv, file_name=f"Payroll_{datetime.now().strftime('%Y%m')}.csv")
