# ==============================
# 🧾 RECEIPT GENERATION
# ==============================
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer
)

from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4

from io import BytesIO
from datetime import datetime

import streamlit as st
import pandas as pd

# Core DB utilities
from core.database import (
    supabase,
    get_cached_data,
    save_data_saas,
    delete_data_saas
)


# ==============================
# 🎨 PAYMENTS PAGE STYLING
# ==============================
def payments_styles():

    st.markdown("""
    <style>

    .payments-header {
        background: linear-gradient(90deg, #0A192F, #112240);
        padding: 14px 18px;
        border-radius: 12px;
        color: white;
        margin-bottom: 18px;
    }

    .payments-header h1 {
        margin: 0;
        font-size: 24px;
    }

    .payments-tabs {
        display: flex;
        gap: 10px;
        margin-bottom: 20px;
        flex-wrap: wrap;
    }

    .payments-tab {
        background: #F1F5F9;
        padding: 10px 18px;
        border-radius: 10px;
        font-weight: 600;
        border: 1px solid #E2E8F0;
        cursor: pointer;
    }

    .payments-tab-active {
        background: #2563EB;
        color: white;
        border: 1px solid #2563EB;
    }

    .metric-card {
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 14px;
    }

    .stButton > button {
        border-radius: 10px;
        height: 40px;
        font-weight: 600;
    }

    </style>
    """, unsafe_allow_html=True)


# ==============================
# 🧾 PDF RECEIPT
# ==============================
def generate_receipt_pdf(data, filename):

    doc = SimpleDocTemplate(
        filename,
        pagesize=A4
    )

    styles = getSampleStyleSheet()

    content = []

    content.append(
        Paragraph(
            "<b>PAYMENT RECEIPT</b>",
            styles["Title"]
        )
    )

    content.append(Spacer(1, 12))

    for k, v in data.items():

        content.append(
            Paragraph(
                f"<b>{k}:</b> {v}",
                styles["Normal"]
            )
        )

        content.append(Spacer(1, 8))

    doc.build(content)


# ==============================
# 🔢 RECEIPT NUMBER
# ==============================
def generate_receipt_no(supabase, tenant_id):

    try:

        res = supabase.rpc(
            "get_next_receipt",
            {"p_tenant": tenant_id}
        ).execute()

        return res.data

    except Exception as e:

        st.error(f"Receipt generation failed: {e}")

        return f"RCPT-{datetime.now().strftime('%Y%m%d%H%M%S')}"


# ==============================
# 💵 PAYMENTS MODULE
# ==============================
def show_payments():

    payments_styles()

    st.markdown("""
    <div class="payments-header">
        <h1>💵 Payments Management</h1>
    </div>
    """, unsafe_allow_html=True)

    # ==============================
    # 📥 LOAD DATA
    # ==============================
    try:

        loans_raw = get_cached_data("loans")
        payments_raw = get_cached_data("payments")
        borrowers_raw = get_cached_data("borrowers")

    except Exception as e:

        st.error(f"❌ Data load error: {e}")
        return

    loans_df = pd.DataFrame(loans_raw) if loans_raw is not None else pd.DataFrame()
    payments_df = pd.DataFrame(payments_raw) if payments_raw is not None else pd.DataFrame()
    borrowers_df = pd.DataFrame(borrowers_raw) if borrowers_raw is not None else pd.DataFrame()

    if loans_df.empty:
        st.info("ℹ️ No loans available.")
        return

    # ==============================
    # 🧹 NORMALIZE COLUMNS
    # ==============================
    for df in [loans_df, payments_df, borrowers_df]:

        if not df.empty:

            df.columns = (
                df.columns
                .str.lower()
                .str.strip()
                .str.replace(" ", "_")
            )

    # ==============================
    # 🔑 ENSURE STRING IDS
    # ==============================
    id_pairs = [
        (borrowers_df, "id"),
        (loans_df, "id"),
        (loans_df, "borrower_id"),
        (payments_df, "loan_id")
    ]

    for df, col in id_pairs:

        if col in df.columns:
            df[col] = df[col].astype(str)

    # ==============================
    # 👤 BORROWER SYNC
    # ==============================
    borrower_name_col = None

    for possible_col in [
        "borrower_name",
        "name",
        "full_name"
    ]:

        if possible_col in borrowers_df.columns:
            borrower_name_col = possible_col
            break

    if (
        borrower_name_col
        and "borrower_id" in loans_df.columns
    ):

        borrower_map = dict(
            zip(
                borrowers_df["id"],
                borrowers_df[borrower_name_col]
            )
        )

        loans_df["borrower"] = (
            loans_df["borrower_id"]
            .map(borrower_map)
            .fillna("Unknown")
        )

    else:
        loans_df["borrower"] = "Unknown"

    # ==============================
    # 💰 NUMERIC CLEANUP
    # ==============================
    loan_numeric_cols = [
        "principal",
        "interest",
        "total_repayable",
        "amount_paid",
        "balance"
    ]

    for col in loan_numeric_cols:

        if col in loans_df.columns:

            loans_df[col] = pd.to_numeric(
                loans_df[col],
                errors="coerce"
            ).fillna(0)

    if (
        not payments_df.empty
        and "amount" in payments_df.columns
    ):

        payments_df["amount"] = pd.to_numeric(
            payments_df["amount"],
            errors="coerce"
        ).fillna(0)

    # ==============================
    # 🔄 PAYMENT SYNC TO LOANS
    # ==============================
    if not payments_df.empty:

        payment_sums = (
            payments_df
            .groupby("loan_id")["amount"]
            .sum()
        )

        loans_df["amount_paid"] = (
            loans_df["id"]
            .map(payment_sums)
            .fillna(0)
        )

    else:
        loans_df["amount_paid"] = 0

    # ==============================
    # 📉 BALANCE RECALCULATION
    # ==============================
    if "total_repayable" in loans_df.columns:

        loans_df["balance"] = (
            loans_df["total_repayable"]
            - loans_df["amount_paid"]
        )

    # ==============================
    # ✨ SHOW LATEST CYCLE ONLY
    # ==============================
    if (
        "sn" in loans_df.columns
        and "cycle_no" in loans_df.columns
    ):

        loans_df = loans_df.sort_values(
            ["sn", "cycle_no"],
            ascending=True
        )

        display_df = loans_df.drop_duplicates(
            subset=["sn"],
            keep="last"
        )

    else:
        display_df = loans_df.copy()

    # ==============================
    # 🔁 CASCADE FUNCTION
    # ==============================
    def cascade_payment(loans_df, sn, changed_cycle_no):

        cycles = (
            loans_df[loans_df["sn"] == sn]
            .sort_values("cycle_no")
            .reset_index()
        )

        for idx, row in cycles.iterrows():

            if row["cycle_no"] <= changed_cycle_no:
                continue

            prev_balance = cycles.loc[idx - 1, "balance"]

            prev_interest = row["interest"]

            loans_df.loc[
                row["index"],
                "principal"
            ] = prev_balance

            loans_df.loc[
                row["index"],
                "total_repayable"
            ] = prev_balance + prev_interest

            loans_df.loc[
                row["index"],
                "balance"
            ] = (
                loans_df.loc[row["index"], "total_repayable"]
                - loans_df.loc[row["index"], "amount_paid"]
            )

    # ==============================
    # 🔥 ACTIVE LOAN FINDER
    # ==============================
    def get_active_loan(loans_df, loan_row):

        current = loan_row

        visited = set()

        while True:

            if current["id"] in visited:
                break

            visited.add(current["id"])

            child = loans_df[
                loans_df["parent_loan_id"] == current["id"]
            ]

            if child.empty:
                return current

            current = child.iloc[0]

        return current

    # ==============================
    # 📊 TOP METRICS
    # ==============================
    total_collected = (
        payments_df["amount"].sum()
        if not payments_df.empty
        else 0
    )

    total_balance = (
        loans_df["balance"].sum()
        if "balance" in loans_df.columns
        else 0
    )

    total_loans = len(loans_df)

    mc1, mc2, mc3 = st.columns(3)

    mc1.metric(
        "💵 Total Collected",
        f"UGX {total_collected:,.0f}"
    )

    mc2.metric(
        "📉 Outstanding Balance",
        f"UGX {total_balance:,.0f}"
    )

    mc3.metric(
        "📂 Total Loans",
        f"{total_loans:,}"
    )

    st.markdown("---")

    # ==============================
    # 🧭 BEAUTIFUL HORIZONTAL MENU
    # ==============================
    menu = st.selectbox(
        "📂 Payments Module",
        [
            "➕ Record Payment",
            "📜 Payment History",
            "📊 Loan Balances",
            "🧾 Receipts"
        ]
    )

    st.markdown("---")

    # ==============================
    # ➕ RECORD PAYMENT
    # ==============================
    if menu == "➕ Record Payment":

        active_loans = display_df.copy()

        def format_loan(row):

            balance = (
                row["total_repayable"]
                - row["amount_paid"]
            )

            sn = (
                row.get("loan_id_label")
                or row.get("sn")
                or "N/A"
            )

            return (
                f"{row['borrower']} "
                f"| SN: {sn} "
                f"| BAL: UGX {balance:,.0f}"
            )

        active_loans["label"] = (
            active_loans
            .apply(format_loan, axis=1)
        )

        selected_index = st.selectbox(
            "Select Loan",
            active_loans.index,
            format_func=lambda i: active_loans.loc[i, "label"]
        )

        loan = active_loans.loc[selected_index]

        active_loan = get_active_loan(
            loans_df,
            loan
        )

        loan_id = active_loan["id"]

        borrower_name = active_loan["borrower"]

        balance = (
            active_loan["total_repayable"]
            - active_loan["amount_paid"]
        )

        # ==============================
        # 📋 LOAN SUMMARY
        # ==============================
        s1, s2, s3, s4 = st.columns(4)

        s1.metric(
            "👤 Borrower",
            borrower_name
        )

        s2.metric(
            "💰 Principal",
            f"UGX {active_loan['principal']:,.0f}"
        )

        s3.metric(
            "🧾 Total Payable",
            f"UGX {active_loan['total_repayable']:,.0f}"
        )

        s4.metric(
            "📉 Balance",
            f"UGX {balance:,.0f}"
        )

        st.markdown("---")

        # ==============================
        # 💳 PAYMENT FORM
        # ==============================
        with st.form("payment_form"):

            amount = st.number_input(
                "Payment Amount",
                min_value=0.0,
                step=1000.0
            )

            method = st.selectbox(
                "Payment Method",
                [
                    "Cash",
                    "Mobile Money",
                    "Bank"
                ]
            )

            date = st.date_input(
                "Payment Date",
                datetime.now()
            )

            submit = st.form_submit_button(
                "✅ Post Payment"
            )

        # ==============================
        # 🚀 POST PAYMENT
        # ==============================
        if submit:

            if amount <= 0:

                st.warning("Enter valid amount")
                return

            try:

                tenant_id = st.session_state.get(
                    "tenant_id"
                )

                receipt_no = generate_receipt_no(
                    supabase,
                    tenant_id
                )

                # ==============================
                # 1️⃣ INSERT PAYMENT
                # ==============================
                supabase.table("payments").insert({

                    "receipt_no": receipt_no,

                    "loan_id": loan_id,

                    "borrower_id": active_loan["borrower_id"],

                    "borrower": borrower_name,

                    "amount": float(amount),

                    "date": date.strftime("%Y-%m-%d"),

                    "method": method,

                    "tenant_id": tenant_id,

                    "created_at": str(datetime.now())

                }).execute()

                # ==============================
                # 2️⃣ UPDATE LOAN
                # ==============================
                new_paid = (
                    active_loan["amount_paid"]
                    + amount
                )

                new_balance = (
                    active_loan["total_repayable"]
                    - new_paid
                )

                supabase.table("loans").update({

                    "amount_paid": float(new_paid),

                    "balance": float(new_balance)

                }).eq("id", loan_id).execute()

                # ==============================
                # 3️⃣ UPDATE BORROWER
                # ==============================
                if "borrower_id" in active_loan:

                    borrower_id = active_loan["borrower_id"]

                    borrower_loans = loans_df[
                        loans_df["borrower_id"]
                        == borrower_id
                    ]

                    borrower_total_balance = (
                        borrower_loans["balance"]
                        .sum()
                    )

                    borrower_total_paid = (
                        borrower_loans["amount_paid"]
                        .sum()
                    )

                    supabase.table("borrowers").update({

                        "total_paid":
                        float(borrower_total_paid),

                        "total_balance":
                        float(borrower_total_balance)

                    }).eq("id", borrower_id).execute()

                # ==============================
                # 4️⃣ CASCADE CYCLES
                # ==============================
                sn = active_loan["sn"]

                changed_cycle_no = int(
                    active_loan["cycle_no"]
                )

                cascade_payment(
                    loans_df,
                    sn,
                    changed_cycle_no
                )

                save_data_saas(
                    "loans",
                    loans_df
                )

                # ==============================
                # 5️⃣ GENERATE RECEIPT
                # ==============================
                file_path = (
                    f"/tmp/{receipt_no}.pdf"
                )

                generate_receipt_pdf({

                    "Receipt No": receipt_no,

                    "Borrower": borrower_name,

                    "Amount":
                    f"UGX {amount:,.0f}",

                    "Method": method,

                    "Date":
                    date.strftime("%Y-%m-%d"),

                }, file_path)

                with open(file_path, "rb") as f:

                    st.download_button(
                        "📥 Download Receipt",
                        f,
                        file_name=f"{receipt_no}.pdf"
                    )

                st.success(
                    f"✅ Payment posted successfully."
                )

                st.cache_data.clear()

                st.rerun()

            except Exception as e:

                st.error(f"❌ Error: {e}")

    # ==============================
    # 📜 PAYMENT HISTORY
    # ==============================
    elif menu == "📜 Payment History":

        st.markdown("### 📜 Payment History")

        if payments_df.empty:

            st.info("No payment history")

        else:

            payments_df["amount_display"] = (
                payments_df["amount"]
                .apply(lambda x: f"UGX {x:,.0f}")
            )

            payments_df["receipt_no"] = (
                payments_df["receipt_no"]
                .fillna("No Receipt")
            )

            display_cols = [
                "date",
                "borrower",
                "amount_display",
                "method",
                "receipt_no"
            ]

            st.dataframe(
                payments_df[display_cols],
                use_container_width=True,
                hide_index=True
            )

    # ==============================
    # 📊 LOAN BALANCES
    # ==============================
    elif menu == "📊 Loan Balances":

        st.markdown("### 📊 Loan Balances")

        balance_df = display_df.copy()

        balance_df["principal_display"] = (
            balance_df["principal"]
            .apply(lambda x: f"UGX {x:,.0f}")
        )

        balance_df["paid_display"] = (
            balance_df["amount_paid"]
            .apply(lambda x: f"UGX {x:,.0f}")
        )

        balance_df["balance_display"] = (
            balance_df["balance"]
            .apply(lambda x: f"UGX {x:,.0f}")
        )

        show_cols = [
            "borrower",
            "principal_display",
            "paid_display",
            "balance_display"
        ]

        st.dataframe(
            balance_df[show_cols],
            use_container_width=True,
            hide_index=True
        )

    # ==============================
    # 🧾 RECEIPTS
    # ==============================
    elif menu == "🧾 Receipts":

        st.markdown("### 🧾 Receipts")

        if payments_df.empty:

            st.info("No receipts available")

        else:

            receipt_df = payments_df.copy()

            receipt_df["amount_display"] = (
                receipt_df["amount"]
                .apply(lambda x: f"UGX {x:,.0f}")
            )

            st.dataframe(

                receipt_df[[
                    "receipt_no",
                    "borrower",
                    "amount_display",
                    "date"
                ]],

                use_container_width=True,
                hide_index=True
            )
