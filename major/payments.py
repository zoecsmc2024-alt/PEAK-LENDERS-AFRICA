import os
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
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
# 🧾 PDF RECEIPT GENERATOR
# ==============================
def generate_receipt_pdf(data, filename):
    doc = SimpleDocTemplate(filename, pagesize=A4)
    styles = getSampleStyleSheet()
    content = []

    content.append(Paragraph("<b>PAYMENT RECEIPT</b>", styles["Title"]))
    content.append(Spacer(1, 12))

    for k, v in data.items():
        # CRITICAL FIX: Explicitly string-cast values to protect ReportLab from typing crashes
        content.append(Paragraph(f"<b>{str(k)}:</b> {str(v)}", styles["Normal"]))
        content.append(Spacer(1, 8))

    doc.build(content)


# ==============================
# 🔢 RECEIPT NUMBER ENGINE
# ==============================
def generate_receipt_no(supabase, tenant_id):
    try:
        res = supabase.rpc(
            "get_next_receipt",
            {"p_tenant": tenant_id}
        ).execute()
        
        if res.data:
            return res.data
        return f"RCPT-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    except Exception as e:
        return f"RCPT-{datetime.now().strftime('%Y%m%d%H%M%S')}"


# ==============================
# 🔁 CASCADE BALANCE MANAGEMENT
# ==============================
def cascade_payment(loans_df, sn, changed_cycle_no):
    cycles = (
        loans_df[loans_df["sn"] == sn]
        .sort_values("cycle_no")
        .copy()
    )
    
    # Track changes sequentially across rollovers
    for i in range(len(cycles)):
        current_row = cycles.iloc[i]
        if current_row["cycle_no"] <= changed_cycle_no:
            continue
            
        prev_row = cycles.iloc[i - 1]
        prev_balance = prev_row["balance"]
        
        # Real-time calculations
        new_principal = prev_balance
        new_interest_amount = new_principal * (current_row["interest"] / 100)
        new_total_repayable = new_principal + new_interest_amount
        new_balance = max(0.0, new_total_repayable - current_row["amount_paid"])
        
        # Update state Dataframe
        loan_id = current_row["id"]
        loans_df.loc[loans_df["id"] == loan_id, "principal"] = new_principal
        loans_df.loc[loans_df["id"] == loan_id, "total_repayable"] = new_total_repayable
        loans_df.loc[loans_df["id"] == loan_id, "balance"] = new_balance
        
        # Push update down to database layer
        try:
            supabase.table("loans").update({
                "principal": float(new_principal),
                "total_repayable": float(new_total_repayable),
                "balance": float(new_balance)
            }).eq("id", str(loan_id)).execute()
        except Exception as e:
            st.error(f"Failed cascading sequence data on Loan ID {loan_id}: {e}")


# ==============================
# 🔥 ACTIVE LOAN TRACKING FACTOR
# ==============================
def get_active_loan(loans_df, loan_row):
    current = loan_row
    visited = set()

    while True:
        if current["id"] in visited:
            break
        visited.add(current["id"])

        if "parent_loan_id" in loans_df.columns:
            child = loans_df[loans_df["parent_loan_id"] == current["id"]]
            if child.empty:
                return current
            current = child.iloc[0]
        else:
            return current
    return current


# ==============================
# 💵 MAIN PAYMENTS MODULE
# ==============================
def show_payments():
    payments_styles()

    st.markdown("""
    <div class="payments-header">
        <h1>💵 Payments Management</h1>
    </div>
    """, unsafe_allow_html=True)

    # Clean execution wrapper to show receipt download modals without breaking UI workflow
    if "pending_receipt" in st.session_state:
        receipt = st.session_state["pending_receipt"]
        st.success(f"🎉 Payment Record Successfully Saved! Receipt Reference: **{receipt['receipt_no']}**")
        
        if os.path.exists(receipt["file_path"]):
            with open(receipt["file_path"], "rb") as f:
                st.download_button(
                    label="📥 Download Official Payment Receipt (PDF)",
                    data=f.read(),
                    file_name=f"Receipt_{receipt['receipt_no']}.pdf",
                    mime="application/pdf",
                    key="btn_download_processed_rcpt"
                )
        if st.button("Clear Reference & Continue", key="btn_clear_rcpt_state"):
            if os.path.exists(receipt["file_path"]):
                try:
                    os.remove(receipt["file_path"])
                except:
                    pass
            del st.session_state["pending_receipt"]
            st.rerun()
        st.markdown("---")

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
    # 👤 BORROWER NAME SYNCHRONIZER
    # ==============================
    borrower_name_col = None
    for possible_col in ["borrower_name", "name", "full_name"]:
        if possible_col in borrowers_df.columns:
            borrower_name_col = possible_col
            break

    if borrower_name_col and "borrower_id" in loans_df.columns:
        borrower_map = dict(zip(borrowers_df["id"], borrowers_df[borrower_name_col]))
        loans_df["borrower"] = loans_df["borrower_id"].map(borrower_map).fillna("Unknown")
    else:
        loans_df["borrower"] = "Unknown"

    # ==============================
    # 💰 NUMERIC DATA CLEANUP
    # ==============================
    loan_numeric_cols = ["principal", "interest", "total_repayable", "amount_paid", "balance"]
    for col in loan_numeric_cols:
        if col in loans_df.columns:
            loans_df[col] = pd.to_numeric(loans_df[col], errors="coerce").fillna(0.0)

    if not payments_df.empty and "amount" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(payments_df["amount"], errors="coerce").fillna(0.0)

    # ==============================
    # 🔄 RECALCULATE REPAYMENTS
    # ==============================
    if not payments_df.empty and "loan_id" in payments_df.columns:
        payment_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(payment_sums).fillna(0.0)
    else:
        loans_df["amount_paid"] = 0.0

    if "total_repayable" in loans_df.columns:
        loans_df["balance"] = (loans_df["total_repayable"] - loans_df["amount_paid"]).clip(lower=0.0)

    # Filter handling for active cycle views
    if "sn" in loans_df.columns and "cycle_no" in loans_df.columns:
        loans_df["cycle_no"] = pd.to_numeric(loans_df["cycle_no"], errors="coerce").fillna(1).astype(int)
        loans_df = loans_df.sort_values(["sn", "cycle_no"], ascending=True)
        display_df = loans_df.drop_duplicates(subset=["sn"], keep="last")
    else:
        display_df = loans_df.copy()

    # ==============================
    # 📊 HEADER DASHBOARD METRICS
    # ==============================
    total_collected = payments_df["amount"].sum() if not payments_df.empty else 0.0
    total_balance = loans_df["balance"].sum() if "balance" in loans_df.columns else 0.0
    total_loans = len(loans_df)

    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("💵 Total Collected", f"UGX {total_collected:,.0f}")
    mc2.metric("📉 Outstanding Balance", f"UGX {total_balance:,.0f}")
    mc3.metric("📂 Total Loans", f"{total_loans:,}")

    st.markdown("---")
    menu = st.selectbox(
        "📂 Payments Module Options",
        ["➕ Record Payment", "📜 Payment History", "📊 Loan Balances", "🧾 Receipts"],
        key="payment_module_navigation_node"
    )
    st.markdown("---")

    # ==============================
    # ➕ RECORD PAYMENT MODULE
    # ==============================
    if menu == "➕ Record Payment":
        active_loans = display_df.copy()
        if active_loans.empty:
            st.warning("No dynamic loan assets structural profiles detected.")
            return

        def format_loan(row):
            bal = row["total_repayable"] - row["amount_paid"]
            sn = row.get("loan_id_label") or row.get("sn") or "N/A"
            return f"{row['borrower']} | SN: {sn} | BAL: UGX {bal:,.0f}"

        active_loans["label"] = active_loans.apply(format_loan, axis=1)
        
        # Link state across page 2 selections seamlessly
        selected_index = st.selectbox(
            "Select Target Loan Contract",
            active_loans.index,
            format_func=lambda i: active_loans.loc[i, "label"]
        )

        loan = active_loans.loc[selected_index]
        active_loan = get_active_loan(loans_df, loan)
        loan_id = active_loan["id"]
        borrower_name = active_loan["borrower"]
        balance = max(0.0, active_loan["total_repayable"] - active_loan["amount_paid"])

        # Display Summary
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("👤 Borrower", str(borrower_name))
        s2.metric("💰 Principal Base", f"UGX {active_loan['principal']:,.0f}")
        s3.metric("🧾 Total Payable", f"UGX {active_loan['total_repayable']:,.0f}")
        s4.metric("📉 Active Balance", f"UGX {balance:,.0f}")

        st.markdown("---")

        with st.form("payment_form"):
            amount = st.number_input("Payment Amount Received (UGX)", min_value=0.0, step=5000.0)
            method = st.selectbox("Payment Channel Method", ["Cash", "Mobile Money", "Bank"])
            date = st.date_input("Payment Settlement Date", datetime.now())
            submit = st.form_submit_button("✅ Post Payment Ledger Entry")

        if submit:
            if amount <= 0:
                st.warning("Please enter a valid transactional payment amount.")
                return
            if amount > (balance + 0.01):
                st.error(f"Payment amount exceeds total remaining loan balance of UGX {balance:,.0f}")
                return

            try:
                tenant_id = st.session_state.get("tenant_id", "default_tenant")
                receipt_no = generate_receipt_no(supabase, tenant_id)

                # 1️⃣ Commit Payment Entry to Database Table
                supabase.table("payments").insert({
                    "receipt_no": receipt_no,
                    "loan_id": str(loan_id),
                    "borrower_id": str(active_loan["borrower_id"]),
                    "borrower": str(borrower_name),
                    "amount": float(amount),
                    "date": date.strftime("%Y-%m-%d"),
                    "method": method,
                    "tenant_id": str(tenant_id),
                    "created_at": datetime.now().isoformat()
                }).execute()

                # 2️⃣ Update Individual Loan Totals
                new_paid = active_loan["amount_paid"] + amount
                new_balance = max(0.0, active_loan["total_repayable"] - new_paid)

                supabase.table("loans").update({
                    "amount_paid": float(new_paid),
                    "balance": float(new_balance)
                }).eq("id", str(loan_id)).execute()

                # Local update to keep tracking array in memory identical
                loans_df.loc[loans_df["id"] == loan_id, "amount_paid"] = new_paid
                loans_df.loc[loans_df["id"] == loan_id, "balance"] = new_balance

                # 3️⃣ Synchronize Master Borrower Profile Balances
                if "borrower_id" in active_loan:
                    b_id = str(active_loan["borrower_id"])
                    b_loans = loans_df[loans_df["borrower_id"] == b_id]
                    b_total_bal = b_loans["balance"].sum()
                    b_total_paid = b_loans["amount_paid"].sum()

                    supabase.table("borrowers").update({
                        "total_paid": float(b_total_paid),
                        "total_balance": float(b_total_bal)
                    }).eq("id", b_id).execute()

                # 4️⃣ Cascade Balance Matrix Changes Across Structural Loan Cycles
                if "sn" in active_loan and "cycle_no" in active_loan:
                    cascade_payment(loans_df, active_loan["sn"], int(active_loan["cycle_no"]))

                # Save session cache
                save_data_saas("loans", loans_df)

                # 5️⃣ Generate & Stage Receipt Document Download safely
                file_path = f"/tmp/{receipt_no}.pdf"
                generate_receipt_pdf({
                    "Receipt Number": receipt_no,
                    "Client Name": borrower_name,
                    "Settlement Amount": f"UGX {amount:,.0f}",
                    "Payment Channel": method,
                    "Transaction Date": date.strftime("%Y-%m-%d"),
                    "Remaining Balance": f"UGX {new_balance:,.0f}"
                }, file_path)

                # Stage details into memory container to completely avoid Streamlit UI thread crashes
                st.session_state["pending_receipt"] = {
                    "receipt_no": receipt_no,
                    "file_path": file_path
                }
                
                st.cache_data.clear()
                st.rerun()

            except Exception as e:
                st.error(f"Critical ledger balancing update transaction failed: {e}")

    # ==============================
    # 📜 PAYMENT HISTORY MODULE
    # ==============================
    elif menu == "📜 Payment History":
        st.markdown("### 📜 System-Wide Repayment History Ledger")
        if payments_df.empty:
            st.info("No compiled payment histories currently detected on this ledger database.")
        else:
            hist_df = payments_df.copy()
            hist_df["amount_display"] = hist_df["amount"].apply(lambda x: f"UGX {x:,.0f}")
            hist_df["receipt_no"] = hist_df["receipt_no"].fillna("No Receipt")

            display_cols = ["date", "borrower", "amount_display", "method", "receipt_no"]
            st.dataframe(
                hist_df[[c for c in display_cols if c in hist_df.columns]],
                use_container_width=True,
                hide_index=True
            )

    # ==============================
    # 📊 LOAN BALANCES MODULE
    # ==============================
    elif menu == "📊 Loan Balances":
        st.markdown("### 📊 Active Loan Balances Portfolio")
        balance_df = display_df.copy()
        
        balance_df["principal_display"] = balance_df["principal"].apply(lambda x: f"UGX {x:,.0f}")
        balance_df["paid_display"] = balance_df["amount_paid"].apply(lambda x: f"UGX {x:,.0f}")
        balance_df["balance_display"] = balance_df["balance"].apply(lambda x: f"UGX {x:,.0f}")

        show_cols = ["borrower", "principal_display", "paid_display", "balance_display"]
        st.dataframe(
            balance_df[show_cols],
            use_container_width=True,
            hide_index=True
        )

    # ==============================
    # 🧾 RECEIPTS MANIFEST MODULE
    # ==============================
    elif menu == "🧾 Receipts":
        st.markdown("### 🧾 Historical Receipt Catalog Manifest")
        if payments_df.empty:
            st.info("No generated receipt manifest entries found.")
        else:
            receipt_df = payments_df.copy()
            receipt_df["amount_display"] = receipt_df["amount"].apply(lambda x: f"UGX {x:,.0f}")

            st.dataframe(
                receipt_df[["receipt_no", "borrower", "amount_display", "date"]],
                use_container_width=True,
                hide_index=True
            )
