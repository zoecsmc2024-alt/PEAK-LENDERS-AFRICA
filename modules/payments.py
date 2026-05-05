import pandas as pd
from datetime import datetime
import uuid
import streamlit as st
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
import io
import base64
import json
import os
import re
from datetime import datetime, timedelta
from fpdf import FPDF
from streamlit_calendar import calendar
import bcrypt
from twilio.rest import Client as TwilioClient
import time
import uuid
import extra_streamlit_components as stx
from database import supabase, get_cached_data
# ==============================
# 🧾 RECEIPT GENERATION
# ==============================
def generate_receipt_pdf(data, filename):
    doc = SimpleDocTemplate(filename)
    styles = getSampleStyleSheet()
    content = []
    content.append(Paragraph("<b>PAYMENT RECEIPT</b>", styles["Title"]))
    content.append(Spacer(1, 12))
    for k, v in data.items():
        content.append(Paragraph(f"<b>{k}:</b> {v}", styles["Normal"]))
        content.append(Spacer(1, 8))
    doc.build(content)

# ✅ SINGLE SOURCE OF TRUTH (RPC)
def generate_receipt_no(supabase, tenant_id):
    try:
        res = supabase.rpc("get_next_receipt", {"p_tenant": tenant_id}).execute()
        return res.data
    except Exception as e:
        st.error(f"Receipt generation failed: {e}")
        return f"RCPT-{datetime.now().strftime('%Y%m%d%H%M%S')}"

# ==============================
# 💵 PAYMENTS MODULE
# ==============================
def show_payments_page():
    st.markdown("## 💵 Payments Management")

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

    # Normalize
    for df in [loans_df, payments_df, borrowers_df]:
        if not df.empty:
            df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

    # Ensure IDs
    for df, col in [(borrowers_df, "id"), (loans_df, "borrower_id"), (loans_df, "id"), (payments_df, "loan_id")]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # borrower mapping
    if not borrowers_df.empty and "name" in borrowers_df.columns:
        borrower_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].map(borrower_map).fillna("Unknown")
    else:
        loans_df["borrower"] = "Unknown"

    # Numeric fields
    loans_df["total_repayable"] = pd.to_numeric(loans_df.get("total_repayable", 0), errors="coerce").fillna(0)

    # ✅ ALWAYS derive from payments
    if not payments_df.empty:
        payments_df["amount"] = pd.to_numeric(payments_df.get("amount", 0), errors="coerce").fillna(0)
        payment_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(payment_sums).fillna(0)
    else:
        loans_df["amount_paid"] = 0

    # ==============================
    # 🔥 GET ACTIVE LOAN
    # ==============================
    def get_active_loan(loans_df, loan_row):
        current = loan_row
        visited = set()

        while True:
            if current["id"] in visited:
                break
            visited.add(current["id"])

            child = loans_df[loans_df["parent_loan_id"] == current["id"]]
            if child.empty:
                return current

            current = child.iloc[0]

        return current

    # ==============================
    # 📑 TABS
    # ==============================
    tab1, tab2 = st.tabs(["➕ Record Payment", "📜 History"])

    # ==============================
    # ➕ RECORD PAYMENT
    # ==============================
    with tab1:
        active_loans = loans_df.copy()

        def format_loan(row):
            balance = row["total_repayable"] - row["amount_paid"]
            sn = row.get("loan_id_label") or row.get("sn") or "N/A"
            return f"{row['borrower']} | SN: {sn} | BAL: UGX {balance:,.0f}"

        active_loans["label"] = active_loans.apply(format_loan, axis=1)

        selected_index = st.selectbox(
            "Select Loan",
            active_loans.index,
            format_func=lambda i: active_loans.loc[i, "label"]
        )

        loan = active_loans.loc[selected_index]

        # 🔥 CRITICAL FIX
        active_loan = get_active_loan(loans_df, loan)
        loan_id = active_loan["id"]

        balance = active_loan["total_repayable"] - active_loan["amount_paid"]

        st.info(f"Active Loan Used: {active_loan['borrower']} (ID: {loan_id[:6]})")

        st.metric("Balance", f"UGX {balance:,.0f}")

        with st.form("payment_form"):
            amount = st.number_input("Amount", min_value=0.0, step=1000.0)
            method = st.selectbox("Method", ["Cash", "Mobile Money", "Bank"])
            date = st.date_input("Date", datetime.now())
            submit = st.form_submit_button("Post Payment")

        if submit:
            if amount <= 0:
                st.warning("Enter valid amount")
                return

            try:
                tenant_id = st.session_state.get("tenant_id")

                # ✅ SINGLE SOURCE OF TRUTH
                receipt_no = generate_receipt_no(supabase, tenant_id)

                supabase.table("payments").insert({
                    "receipt_no": receipt_no,
                    "loan_id": loan_id,
                    "borrower": active_loan["borrower"],
                    "amount": float(amount),
                    "date": date.strftime("%Y-%m-%d"),
                    "method": method,
                    "tenant_id": tenant_id
                }).execute()

                # Recalculate locally
                new_paid = active_loan["amount_paid"] + amount
                new_balance = active_loan["total_repayable"] - new_paid

                # Receipt
                file_path = f"/tmp/{receipt_no}.pdf"
                generate_receipt_pdf({
                    "Receipt No": receipt_no,
                    "borrower": active_loan["borrower"],
                    "Amount": f"UGX {amount:,.0f}",
                    "Method": method,
                    "Date": date.strftime("%Y-%m-%d"),
                }, file_path)

                with open(file_path, "rb") as f:
                    st.download_button("📥 Download Receipt", f, file_name=f"{receipt_no}.pdf")

                st.success(f"✅ Payment posted. New Balance: UGX {new_balance:,.0f}")

                st.cache_data.clear()
                st.rerun()

            except Exception as e:
                st.error(f"❌ Error: {e}")

    # ==============================
    # 📜 HISTORY
    # ==============================
    with tab2:
        if payments_df.empty:
            st.info("No payment history")
        else:
            # 1. Prepare Display Data
            payments_df["amount_display"] = payments_df["amount"].apply(lambda x: f"UGX {x:,.0f}")
            # Ensure ID and Receipt exist for the logic below
            payments_df["id"] = payments_df["id"].astype(str)
            payments_df["receipt_no"] = payments_df["receipt_no"].fillna("No Receipt")
            
            display_cols = ["date", "borrower", "amount_display", "method", "receipt_no"]
            st.dataframe(payments_df[display_cols], use_container_width=True, hide_index=True)

            # --- 🛠️ EDIT/DELETE MANAGEMENT ---
            st.markdown("---")
            st.markdown("### ⚙️ Payment Maintenance")
            
            # Create the selection map using payments_df
            pay_map = {
                f"{row['receipt_no']} | {row['borrower']} | {row['amount_display']}": row['id'] 
                for _, row in payments_df.iterrows()
            }
            
            selected_pay_label = st.selectbox("Choose Payment to Modify", list(pay_map.keys()))
            
            target_pay_id = pay_map[selected_pay_label]
            # Fetch the specific record from payments_df
            target_pay = payments_df[payments_df['id'] == target_pay_id].iloc[0]

            p_col1, p_col2 = st.columns(2)

            if p_col1.button("🗑️ Delete Payment", use_container_width=True):
                try:
                    supabase.table("payments").delete().eq("id", target_pay_id).execute()
                    st.cache_data.clear() 
                    st.warning(f"Payment {target_pay['receipt_no']} removed.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")

            if p_col2.button("📝 Edit Payment", use_container_width=True):
                st.session_state["edit_pay_mode"] = True

            if st.session_state.get("edit_pay_mode"):
                with st.form("edit_payment_form"):
                    st.info(f"Modifying: {target_pay['receipt_no']}")
                    new_amt = st.number_input("Revised Amount", value=float(target_pay['amount']))
                    
                    # Safe index lookup for the method
                    current_method = target_pay['method']
                    method_options = ["Cash", "Mobile Money", "Bank"]
                    method_idx = method_options.index(current_method) if current_method in method_options else 0
                    
                    new_method = st.selectbox("Revised Method", method_options, index=method_idx)
                    
                    eb1, eb2 = st.columns(2)
                    if eb1.form_submit_button("💾 Save Changes"):
                        try:
                            supabase.table("payments").update({
                                "amount": new_amt,
                                "method": new_method
                            }).eq("id", target_pay_id).execute()
                            
                            st.session_state["edit_pay_mode"] = False
                            st.cache_data.clear()
                            st.success("Payment updated successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Update failed: {e}")
                    
                    if eb2.form_submit_button("❌ Cancel"):
                        st.session_state["edit_pay_mode"] = False
                        st.rerun()

