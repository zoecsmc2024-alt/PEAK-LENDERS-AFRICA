import streamlit as st
import pandas as pd
from datetime import datetime
from core.database import supabase, get_cached_data

# =========================================================
# 🎨 LOANS PAGE STYLING
# =========================================================
def loans_styles():
    st.markdown("""
    <style>
    .loans-header {
        background: linear-gradient(90deg, #0A192F, #112240);
        padding: 16px 20px;
        border-radius: 12px;
        margin-bottom: 20px;
    }
    .loans-header h1 {
        color: white;
        margin: 0;
        font-size: 24px;
        font-weight: 700;
    }
    .loan-top-card {
        background: white;
        padding: 14px;
        border-radius: 12px;
        border: 1px solid #E2E8F0;
        margin-bottom: 15px;
    }
    .stButton > button {
        background: #2563EB;
        color: white;
        border: none;
        border-radius: 8px;
        height: 40px;
        font-weight: 600;
    }
    .stButton > button:hover {
        background: #1D4ED8;
        color: white;
    }
    div[data-baseweb="select"] > div {
        border-radius: 10px;
    }
    .metric-card {
        background: white;
        border: 1px solid #E2E8F0;
        padding: 12px;
        border-radius: 12px;
    }
    </style>
    """, unsafe_allow_html=True)


# =========================================================
# 📥 FETCH LOANS
# =========================================================
def get_loans():
    try:
        res = supabase.table("loans").select("*").execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception as e:
        st.error(f"Loan fetch error: {e}")
        return pd.DataFrame()


# =========================================================
# 📥 FETCH BORROWERS
# =========================================================
def get_borrowers():
    try:
        res = supabase.table("borrowers").select("*").execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception as e:
        st.error(f"Borrower fetch error: {e}")
        return pd.DataFrame()


# =========================================================
# 📥 FETCH PAYMENTS
# =========================================================
def get_payments():
    try:
        res = supabase.table("payments").select("*").execute()
        return pd.DataFrame(res.data) if res.data else pd.DataFrame()
    except Exception as e:
        st.error(f"Payment fetch error: {e}")
        return pd.DataFrame()


# =========================================================
# 🧠 FORMAT DATAFRAME
# =========================================================
def format_loans_df(df):
    if df.empty:
        return df

    # CRITICAL: Exclude any pre-existing string totals row to prevent data corruption loops
    working_df = df[df["status"] != "TOTAL"].copy() if "status" in df.columns else df.copy()
    if working_df.empty:
        return working_df

    base_cols = {
        "sn": "📌 SN",
        "loan_id_label": "🏷 Loan ID",
        "borrower": "👤 Borrower",
        "loan_type": "📂 Type",
        "principal": "💰 Principal",
        "interest": "📊 Interest %",
        "total_repayable": "🧾 Total Payable",
        "amount_paid": "💵 Paid",
        "balance": "📉 Balance",
        "status": "📌 Status",
        "cycle_no": "🔁 Cycle",
        "principal_outstanding": "💵 Principal Outstanding"
    }

    # Keep and map safely
    available = [c for c in base_cols.keys() if c in working_df.columns]
    working_df = working_df[available].copy()

    money_cols_mapped = [
        "💰 Principal",
        "🧾 Total Payable",
        "💵 Paid",
        "📉 Balance",
        "💵 Principal Outstanding"
    ]

    working_df.rename(columns={k: v for k, v in base_cols.items() if k in working_df.columns}, inplace=True)

    # Convert mapping to clean numeric formats
    for col in money_cols_mapped:
        if col in working_df.columns:
            working_df[col] = pd.to_numeric(working_df[col], errors="coerce").fillna(0)

    # Compile the final native mathematical totals row before casting to strings
    total_row = {}
    for col in working_df.columns:
        if col in money_cols_mapped:
            total_row[col] = f"{working_df[col].sum():,.0f}"
        else:
            total_row[col] = ""

    if "📌 SN" in total_row:
        total_row["📌 SN"] = "TOTAL"
    elif "🏷 Loan ID" in total_row:
        total_row["🏷 Loan ID"] = "TOTAL"

    # Turn numeric rows into localized string formats safely
    for col in working_df.columns:
        if col in money_cols_mapped:
            working_df[col] = working_df[col].apply(lambda x: f"{x:,.0f}")

    # Append safely without breaking types
    formatted_df = pd.concat([working_df, pd.DataFrame([total_row])], ignore_index=True)
    return formatted_df


# =========================================================
# 📁 MAIN LOANS PAGE
# =========================================================
def show_loans():
    loans_styles()

    st.markdown("""
    <div class="loans-header">
        <h1>💵 Loans Management</h1>
    </div>
    """, unsafe_allow_html=True)

    # =====================================================
    # 📥 LOAD DATA
    # =====================================================
    loans_df = get_loans()
    borrowers_df = get_borrowers()
    payments_df = get_payments()

    # =====================================================
    # 🛡 SAFETY
    # =====================================================
    if loans_df.empty:
        loans_df = pd.DataFrame(columns=[
            "id", "sn", "loan_id_label", "borrower_id", "borrower",
            "loan_type", "principal", "interest", "total_repayable",
            "amount_paid", "balance", "status", "cycle_no",
            "start_date", "end_date"
        ])

    # =====================================================
    # 🔧 CLEANUP
    # =====================================================
    for frame in [loans_df, borrowers_df, payments_df]:
        if not frame.empty:
            frame.columns = (
                frame.columns
                .str.lower()
                .str.strip()
                .str.replace(" ", "_")
            )

    # =====================================================
    # 🔗 BORROWER SYNC
    # =====================================================
    if not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)

        if "full_name" in borrowers_df.columns:
            borrower_map = dict(zip(
                borrowers_df["id"],
                borrowers_df["full_name"]
            ))

            if "borrower_id" in loans_df.columns:
                loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)
                loans_df["borrower"] = (
                    loans_df["borrower_id"]
                    .map(borrower_map)
                    .fillna("Unknown")
                )

    # =====================================================
    # 💰 NUMERIC CLEANUP
    # =====================================================
    numeric_cols = [
        "principal", "interest", "total_repayable", "amount_paid", "balance"
    ]

    for col in numeric_cols:
        if col not in loans_df.columns:
            loans_df[col] = 0.0
        loans_df[col] = pd.to_numeric(loans_df[col], errors="coerce").fillna(0.0)

    # =====================================================
    # 🔗 PAYMENT SYNC
    # =====================================================
    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].astype(str)
        payments_df["amount"] = pd.to_numeric(payments_df["amount"], errors="coerce").fillna(0.0)

        payment_sums = payments_df.groupby("loan_id")["amount"].sum()

        loans_df["amount_paid"] = (
            loans_df["id"]
            .astype(str)
            .map(payment_sums)
            .fillna(0.0)
        )

    loans_df["balance"] = (loans_df["total_repayable"] - loans_df["amount_paid"]).clip(lower=0.0)

    # =====================================================
    # 📌 STATUS ENGINE
    # =====================================================
    today = pd.Timestamp.today().normalize()

    if "end_date" in loans_df.columns:
        loans_df["end_date"] = pd.to_datetime(loans_df["end_date"], errors="coerce")

    if "status" not in loans_df.columns:
        loans_df["status"] = "ACTIVE"

    # User Correction Rule applied: Ensure calculations filter only items with an active balance > 0
    loans_df.loc[(loans_df["balance"] <= 0), "status"] = "CLEARED"
    loans_df.loc[(loans_df["balance"] > 0) & (loans_df["end_date"] < today), "status"] = "ARREARS"
    loans_df.loc[(loans_df["balance"] > 0) & (loans_df["end_date"] >= today), "status"] = "ACTIVE"

    # =====================================================
    # 🧾 SERIAL LABELS
    # =====================================================
    if "sn" in loans_df.columns:
        loans_df["loan_id_label"] = (
            loans_df["sn"]
            .astype(str)
            .str.replace("LN-", "", regex=False)
            .str.zfill(4)
        )

    # =====================================================
    # 📊 METRICS
    # =====================================================
    # Apply Correction Rule: Active metrics require remaining balance > 0
    active_balance_mask = loans_df["balance"] > 0
    total_loans = len(loans_df[active_balance_mask])
    total_principal = loans_df.sidebar_filtered_total_p = loans_df[active_balance_mask]["principal"].sum()
    total_paid = loans_df[active_balance_mask]["amount_paid"].sum()
    total_balance = loans_df[active_balance_mask]["balance"].sum()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("📁 Active/Pending Loans", f"{total_loans:,}")
    m2.metric("💰 Portfolio Principal", f"{total_principal:,.0f}")
    m3.metric("💵 Total Repayments", f"{total_paid:,.0f}")
    m4.metric("📉 Portfolio Balance", f"{total_balance:,.0f}")

    st.markdown("---")

    # =====================================================
    # 🧭 NAVIGATION
    # =====================================================
    menu = st.selectbox(
        "📂 Select Module",
        [
            "View All Loans",
            "Add Loan",
            "Due Loans",
            "Loans in Arrears",
            "Past Maturity Date",
            "Principal Outstanding",
            "Loan Calculator"
        ],
        key="loans_module_select"
    )

    # =====================================================
    # 🔍 FILTERS (SIDEBAR)
    # =====================================================
    # Cross-sync setup: Read row ticked from Page 1 to auto-populate filter context
    p1_selected = st.session_state.get("selected_borrower", None)
    default_search_name = p1_selected.get("Full Name", "") if p1_selected else ""

    with st.sidebar.expander("🔍 Advanced Filters", expanded=True if default_search_name else False):
        search_name = st.text_input("Borrower Name", value=default_search_name, key="loans_filter_name")
        search_status = st.selectbox(
            "Status Dropdown",
            ["All", "ACTIVE", "ARREARS", "CLEARED"],
            key="loans_filter_status"
        )
        apply_filters = st.button("Apply Filters", key="loans_filter_trigger")

    filtered_df = loans_df.copy()

    # Run background matching automatically if default_search_name exists, or if button is clicked
    if default_search_name or apply_filters:
        if search_name and "borrower" in filtered_df.columns:
            filtered_df = filtered_df[
                filtered_df["borrower"]
                .astype(str)
                .str.contains(search_name, case=False, na=False)
            ]
        if search_status != "All" and "status" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["status"] == search_status]

    # =====================================================
    # 📁 VIEW ALL LOANS
    # =====================================================
    if menu == "View All Loans":
        st.markdown("### 📁 All System Loans")
        clean_df = format_loans_df(filtered_df)
        st.dataframe(
            clean_df,
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # ➕ ADD LOAN
    # =====================================================
    elif menu == "Add Loan":
        st.markdown("### ➕ Issue New Loan Contract")

        borrower_options = {}
        if not advertisers_df_empty := borrowers_df.empty and "full_name" in borrowers_df.columns:
            borrower_options = dict(zip(
                borrowers_df["full_name"],
                borrowers_df["id"]
            ))

        # Synchronize drop-down selection index to fallback to Page 1 selection context seamlessly
        default_index = 0
        if default_search_name and default_search_name in borrower_options:
            default_index = list(borrower_options.keys()).index(default_search_name)

        with st.form("add_loan_form"):
            col1, col2 = st.columns(2)

            with col1:
                borrower_name = st.selectbox(
                    "👤 Target Borrower *",
                    options=list(borrower_options.keys()) if borrower_options else ["No Borrowers Found"],
                    index=default_index
                )
                loan_type = st.selectbox(
                    "📂 Loan Type Allocation",
                    ["Business", "Personal", "School Fees", "Emergency"]
                )
                principal = st.number_input(
                    "💰 Principal Disbursed (UGX)",
                    min_value=0.0,
                    step=10000.0,
                    key="add_loan_p"
                )

            with col2:
                interest = st.number_input(
                    "📊 Interest Percentage Rate (%)",
                    min_value=0.0,
                    step=1.0,
                    key="add_loan_i"
                )
                start_date = st.date_input(
                    "📅 Issuance Start Date",
                    value=datetime.today(),
                    key="add_loan_sd"
                )
                end_date = st.date_input(
                    "📅 Contractual Maturity End Date",
                    value=datetime.today(),
                    key="add_loan_ed"
                )

            submit = st.form_submit_button("💾 Commit Contract to Ledger")

            if submit:
                if not borrower_options:
                    st.error("Cannot create a loan without a valid borrower selection.")
                elif principal <= 0:
                    st.error("Principal amount must be greater than zero.")
                elif end_date < start_date:
                    st.error("Maturity End Date cannot be earlier than Issuance Start Date.")
                else:
                    try:
                        total_payable = principal + (principal * (interest / 100))

                        supabase.table("loans").insert({
                            "borrower_id": borrower_options[borrower_name],
                            "loan_type": loan_type,
                            "principal": float(principal),
                            "interest": float(interest),
                            "total_repayable": float(total_payable),
                            "amount_paid": 0.0,
                            "balance": float(total_payable),
                            "status": "ACTIVE",
                            "start_date": str(start_date),
                            "end_date": str(end_date),
                            "created_at": datetime.now().isoformat()
                        }).execute()

                        st.success(f"✅ Loan Contract created successfully for {borrower_name}!")
                        st.rerun()

                    except Exception as e:
                        st.error(f"Loan ledger creation crash: {e}")

    # =====================================================
    # ⏰ DUE LOANS
    # =====================================================
    elif menu == "Due Loans":
        st.markdown("### ⏰ Current Active Portfolio Due")
        due_df = filtered_df[(filtered_df["status"] == "ACTIVE") & (filtered_df["balance"] > 0)]
        st.dataframe(
            format_loans_df(due_df),
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # ⚠ ARREARS
    # =====================================================
    elif menu == "Loans in Arrears":
        st.markdown("### ⚠ Defaulted Portfolio / Loans in Arrears")
        arrears_df = filtered_df[(filtered_df["status"] == "ARREARS") & (filtered_df["balance"] > 0)]
        st.dataframe(
            format_loans_df(arrears_df),
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # 📅 PAST MATURITY
    # =====================================================
    elif menu == "Past Maturity Date":
        st.markdown("### 📅 Matured Contracts Past Maturity Date")
        matured_df = filtered_df[(filtered_df["end_date"] < today) & (filtered_df["balance"] > 0)]
        st.dataframe(
            format_loans_df(matured_df),
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # 💵 PRINCIPAL OUTSTANDING
    # =====================================================
    elif menu == "Principal Outstanding":
        st.markdown("### 💵 Principal Amortization Outstanding Table")
        outstanding_df = filtered_df.copy()
        
        if not outstanding_df.empty:
            # FIX: Mathematical Valuation Amortization Formula
            # Protect from division by zero bugs if a loan has 0 total_repayable
            denom = outstanding_df["total_repayable"].where(outstanding_df["total_repayable"] > 0, 1)
            outstanding_df["principal_outstanding"] = (
                outstanding_df["principal"] - (outstanding_df["amount_paid"] * (outstanding_df["principal"] / denom))
            ).clip(lower=0.0)
        else:
            outstanding_df["principal_outstanding"] = pd.Series(dtype=float)

        st.dataframe(
            format_loans_df(outstanding_df),
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # 🧮 LOAN CALCULATOR
    # =====================================================
    elif menu == "Loan Calculator":
        st.markdown("### 🧮 Amortization & Interest Calculator")

        c1, c2, c3 = st.columns(3)
        with c1:
            calc_principal = st.number_input("Principal Amount (UGX)", min_value=0.0, key="calc_p")
        with c2:
            calc_rate = st.number_input("Flat Interest % (per period)", min_value=0.0, key="calc_r")
        with c3:
            calc_months = st.number_input("Tenor Period Duration (Months)", min_value=1, key="calc_m")

        if st.button("Calculate Loan", key="btn_execute_calc"):
            interest_amount = calc_principal * (calc_rate / 100) * (calc_months / 12)
            total = calc_principal + interest_amount

            r1, r2, r3 = st.columns(3)
            r1.metric("💰 Principal Base", f"{calc_principal:,.0f}")
            r2.metric("📊 Interest Content", f"{interest_amount:,.0f}")
            r3.metric("🧾 Total Repayable Target", f"{total:,.0f}")
