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

    .loans-header{
        background: linear-gradient(90deg,#0A192F,#112240);
        padding:16px 20px;
        border-radius:12px;
        margin-bottom:20px;
    }

    .loans-header h1{
        color:white;
        margin:0;
        font-size:24px;
        font-weight:700;
    }

    .loan-top-card{
        background:white;
        padding:14px;
        border-radius:12px;
        border:1px solid #E2E8F0;
        margin-bottom:15px;
    }

    .stButton > button{
        background:#2563EB;
        color:white;
        border:none;
        border-radius:8px;
        height:40px;
        font-weight:600;
    }

    .stButton > button:hover{
        background:#1D4ED8;
        color:white;
    }

    div[data-baseweb="select"] > div{
        border-radius:10px;
    }

    .metric-card{
        background:white;
        border:1px solid #E2E8F0;
        padding:12px;
        border-radius:12px;
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

    display_df = df.copy()

    safe_cols = {
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
        "cycle_no": "🔁 Cycle"
    }

    available = [c for c in safe_cols.keys() if c in display_df.columns]
    display_df = display_df[available].copy()

    display_df.rename(columns={
        k: v for k, v in safe_cols.items()
        if k in display_df.columns
    }, inplace=True)

    money_cols = [
        "💰 Principal",
        "🧾 Total Payable",
        "💵 Paid",
        "📉 Balance"
    ]

    for col in money_cols:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(
                display_df[col],
                errors="coerce"
            ).fillna(0)

    raw_df = display_df.copy()

    for col in money_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:,.0f}"
            )

    total_row = {}

    for col in display_df.columns:

        if col in money_cols:
            total_row[col] = f"{raw_df[col].sum():,.0f}"

        else:
            total_row[col] = ""

    if "📌 SN" in total_row:
        total_row["📌 SN"] = "TOTAL"

    display_df = pd.concat(
        [display_df, pd.DataFrame([total_row])],
        ignore_index=True
    )

    return display_df


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
            "id",
            "sn",
            "loan_id_label",
            "borrower_id",
            "borrower",
            "loan_type",
            "principal",
            "interest",
            "total_repayable",
            "amount_paid",
            "balance",
            "status",
            "cycle_no",
            "start_date",
            "end_date"
        ])

    # =====================================================
    # 🔧 CLEANUP
    # =====================================================
    for df in [loans_df, borrowers_df, payments_df]:

        if not df.empty:
            df.columns = (
                df.columns
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
        "principal",
        "interest",
        "total_repayable",
        "amount_paid",
        "balance"
    ]

    for col in numeric_cols:

        if col not in loans_df.columns:
            loans_df[col] = 0

        loans_df[col] = pd.to_numeric(
            loans_df[col],
            errors="coerce"
        ).fillna(0)

    # =====================================================
    # 🔗 PAYMENT SYNC
    # =====================================================
    if not payments_df.empty and "loan_id" in payments_df.columns:

        payments_df["loan_id"] = payments_df["loan_id"].astype(str)

        payments_df["amount"] = pd.to_numeric(
            payments_df["amount"],
            errors="coerce"
        ).fillna(0)

        payment_sums = (
            payments_df
            .groupby("loan_id")["amount"]
            .sum()
        )

        loans_df["amount_paid"] = (
            loans_df["id"]
            .astype(str)
            .map(payment_sums)
            .fillna(0)
        )

    loans_df["balance"] = (
        loans_df["total_repayable"]
        - loans_df["amount_paid"]
    ).clip(lower=0)

    # =====================================================
    # 📌 STATUS ENGINE
    # =====================================================
    today = pd.Timestamp.today()

    if "end_date" in loans_df.columns:
        loans_df["end_date"] = pd.to_datetime(
            loans_df["end_date"],
            errors="coerce"
        )

    if "status" not in loans_df.columns:
        loans_df["status"] = "ACTIVE"

    loans_df.loc[
        (loans_df["balance"] <= 0),
        "status"
    ] = "CLEARED"

    loans_df.loc[
        (
            loans_df["balance"] > 0
        ) &
        (
            loans_df["end_date"] < today
        ),
        "status"
    ] = "ARREARS"

    loans_df.loc[
        (
            loans_df["balance"] > 0
        ) &
        (
            loans_df["end_date"] >= today
        ),
        "status"
    ] = "ACTIVE"

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
    total_loans = len(loans_df)

    total_principal = loans_df["principal"].sum()

    total_paid = loans_df["amount_paid"].sum()

    total_balance = loans_df["balance"].sum()

    m1, m2, m3, m4 = st.columns(4)

    m1.metric("📁 Loans", f"{total_loans:,}")
    m2.metric("💰 Principal", f"{total_principal:,.0f}")
    m3.metric("💵 Paid", f"{total_paid:,.0f}")
    m4.metric("📉 Balance", f"{total_balance:,.0f}")

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
        ]
    )

    # =====================================================
    # 🔍 FILTERS
    # =====================================================
    with st.sidebar.expander("🔍 Advanced Filters"):

        search_name = st.text_input("Borrower")

        search_status = st.selectbox(
            "Status",
            ["All", "ACTIVE", "ARREARS", "CLEARED"]
        )

        apply_filters = st.button("Apply Filters")

    filtered_df = loans_df.copy()

    if apply_filters:

        if search_name:
            filtered_df = filtered_df[
                filtered_df["borrower"]
                .astype(str)
                .str.contains(
                    search_name,
                    case=False,
                    na=False
                )
            ]

        if search_status != "All":
            filtered_df = filtered_df[
                filtered_df["status"] == search_status
            ]

    clean_df = format_loans_df(filtered_df)

    # =====================================================
    # 📁 VIEW ALL LOANS
    # =====================================================
    if menu == "View All Loans":

        st.markdown("### 📁 All Loans")

        st.dataframe(
            clean_df,
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # ➕ ADD LOAN
    # =====================================================
    elif menu == "Add Loan":

        st.markdown("### ➕ Add Loan")

        borrower_options = {}

        if not borrowers_df.empty and "full_name" in borrowers_df.columns:

            borrower_options = dict(zip(
                borrowers_df["full_name"],
                borrowers_df["id"]
            ))

        with st.form("add_loan_form"):

            col1, col2 = st.columns(2)

            with col1:

                borrower_name = st.selectbox(
                    "👤 Borrower",
                    list(borrower_options.keys())
                )

                loan_type = st.selectbox(
                    "📂 Loan Type",
                    [
                        "Business",
                        "Personal",
                        "School Fees",
                        "Emergency"
                    ]
                )

                principal = st.number_input(
                    "💰 Principal",
                    min_value=0.0,
                    step=10000.0
                )

            with col2:

                interest = st.number_input(
                    "📊 Interest %",
                    min_value=0.0,
                    step=1.0
                )

                start_date = st.date_input(
                    "📅 Start Date",
                    value=datetime.today()
                )

                end_date = st.date_input(
                    "📅 End Date"
                )

            submit = st.form_submit_button(
                "💾 Create Loan"
            )

            if submit:

                try:

                    total_payable = (
                        principal
                        + (
                            principal
                            * (interest / 100)
                        )
                    )

                    supabase.table("loans").insert({

                        "borrower_id":
                            borrower_options[borrower_name],

                        "borrower":
                            borrower_name,

                        "loan_type":
                            loan_type,

                        "principal":
                            float(principal),

                        "interest":
                            float(interest),

                        "total_repayable":
                            float(total_payable),

                        "amount_paid":
                            0,

                        "balance":
                            float(total_payable),

                        "status":
                            "ACTIVE",

                        "start_date":
                            str(start_date),

                        "end_date":
                            str(end_date),

                        "created_at":
                            str(datetime.now())

                    }).execute()

                    st.success("✅ Loan created successfully")
                    st.rerun()

                except Exception as e:
                    st.error(f"Loan creation error: {e}")

    # =====================================================
    # ⏰ DUE LOANS
    # =====================================================
    elif menu == "Due Loans":

        st.markdown("### ⏰ Due Loans")

        due_df = loans_df[
            loans_df["status"] == "ACTIVE"
        ]

        st.dataframe(
            format_loans_df(due_df),
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # ⚠ ARREARS
    # =====================================================
    elif menu == "Loans in Arrears":

        st.markdown("### ⚠ Loans in Arrears")

        arrears_df = loans_df[
            loans_df["status"] == "ARREARS"
        ]

        st.dataframe(
            format_loans_df(arrears_df),
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # 📅 PAST MATURITY
    # =====================================================
    elif menu == "Past Maturity Date":

        st.markdown("### 📅 Past Maturity Loans")

        matured_df = loans_df[
            loans_df["end_date"] < today
        ]

        st.dataframe(
            format_loans_df(matured_df),
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # 💵 PRINCIPAL OUTSTANDING
    # =====================================================
    elif menu == "Principal Outstanding":

        st.markdown("### 💵 Principal Outstanding")

        outstanding_df = loans_df.copy()

        outstanding_df["principal_outstanding"] = (
            outstanding_df["principal"]
            - outstanding_df["amount_paid"]
        ).clip(lower=0)

        st.dataframe(
            format_loans_df(outstanding_df),
            use_container_width=True,
            hide_index=True
        )

    # =====================================================
    # 🧮 LOAN CALCULATOR
    # =====================================================
    elif menu == "Loan Calculator":

        st.markdown("### 🧮 Loan Calculator")

        c1, c2, c3 = st.columns(3)

        with c1:
            principal = st.number_input(
                "Principal",
                min_value=0.0
            )

        with c2:
            rate = st.number_input(
                "Interest %",
                min_value=0.0
            )

        with c3:
            months = st.number_input(
                "Months",
                min_value=1
            )

        if st.button("Calculate Loan"):

            interest_amount = (
                principal
                * (rate / 100)
                * (months / 12)
            )

            total = principal + interest_amount

            r1, r2, r3 = st.columns(3)

            r1.metric(
                "💰 Principal",
                f"{principal:,.0f}"
            )

            r2.metric(
                "📊 Interest",
                f"{interest_amount:,.0f}"
            )

            r3.metric(
                "🧾 Total Payable",
                f"{total:,.0f}"
            )
