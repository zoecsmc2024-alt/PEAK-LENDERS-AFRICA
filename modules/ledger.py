# ==============================
# 21. MASTER LEDGER (SAAS + ENTERPRISE)
# ==============================

import pandas as pd
import streamlit as st
from datetime import datetime
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from io import BytesIO


# ==============================
# PDF GENERATION BACKEND
# ==============================
def generate_pdf_statement(client_name, loans_df, payments_df):
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)

    styles = getSampleStyleSheet()
    elements = []

    elements.append(Paragraph(f"<b>{st.session_state.get('company_name', 'ZOE CONSULTS').upper()}</b>", styles["Title"]))
    elements.append(Paragraph(f"Client: {client_name}", styles["Normal"]))
    elements.append(Paragraph(f"Statement Date: {datetime.now().strftime('%d %b %Y')}", styles["Normal"]))
    elements.append(Spacer(1, 20))

    grand_total = 0

    for _, loan in loans_df.iterrows():
        loan_id = str(loan["id"])
        # We use a readable label if possible, or truncate the long UUID for the header
        display_id = str(loan.get("loan_id_label", loan_id)) 
        principal = float(loan.get("principal", 0))
        interest = float(loan.get("interest", 0))
        initial_amount = principal + interest

        loan_payments = pd.DataFrame()
        if payments_df is not None and not payments_df.empty:
            loan_payments = payments_df[
                payments_df["loan_id"].astype(str) == loan_id
            ].copy()

        if not loan_payments.empty:
            date_col = "payment_date" if "payment_date" in loan_payments.columns else "date"
            if date_col in loan_payments.columns:
                loan_payments = loan_payments.sort_values(by=date_col)

        balance = initial_amount

        elements.append(Paragraph(f"<b>Loan Ref:</b> {display_id}", styles["Heading3"]))

        data = [["Date", "Description", "Debit", "Credit", "balance"]]

        # Truncate dates to YYYY-MM-DD to prevent overwriting
        start_date_raw = str(loan.get("created_at", loan.get("start_date", "")))
        clean_start_date = start_date_raw[:10] if len(start_date_raw) > 10 else start_date_raw

        data.append([
            clean_start_date,
            "Loan Disbursement",
            f"{initial_amount:,.0f}",
            "0",
            f"{balance:,.0f}"
        ])

        if not loan_payments.empty:
            for _, p in loan_payments.iterrows():
                amount = float(p.get("amount", 0))
                balance -= amount
                
                pay_date_raw = str(p.get("payment_date", p.get("date", "")))
                clean_pay_date = pay_date_raw[:10] if len(pay_date_raw) > 10 else pay_date_raw

                data.append([
                    clean_pay_date,
                    "Repayment",
                    "0",
                    f"{amount:,.0f}",
                    f"{balance:,.0f}"
                ])
        else:
            data.append(["-", "No payments", "-", "-", f"{balance:,.0f}"])

        grand_total += balance

        # Adjusted colWidths: widened the Description and balance columns
        table = Table(data, repeatRows=1, colWidths=[75, 170, 85, 85, 100])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.darkblue),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("FONTname", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9), # Slightly smaller font for better fit
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))

        elements.append(table)
        elements.append(Spacer(1, 20))

    elements.append(Paragraph(f"<b>Total Outstanding: {grand_total:,.0f} UGX</b>", styles["Title"]))

    doc.build(elements)
    buffer.seek(0)
    return buffer


# ==============================
# MAIN LEDGER FUNCTION (BABY BLUE EDITION)
# ==============================
def show_ledger():
    # 🎨 THEME COLORS & FONTS
    baby_blue = "#89CFF0"
    st.markdown(f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
            .ledger-header {{
                font-family: 'Inter', sans-serif;
                color: {baby_blue};
                font-weight: 700;
                letter-spacing: -0.5px;
            }}
            .snapshot-text {{
                font-family: 'Inter', sans-serif;
                font-weight: 600;
                color: #555;
            }}
        </style>
        <h2 class='ledger-header'>📘 Master Ledger</h2>
    """, unsafe_allow_html=True)

    # 📥 LOAD DATA
    loans_df = get_cached_data("loans")
    payments_df = get_cached_data("payments")
    borrowers_df = get_cached_data("borrowers")

    if loans_df is None or loans_df.empty:
        st.info("💡 Your system is clear! No active loans found.")
        return

    # Normalize column names
    loans_df.columns = loans_df.columns.str.strip().str.lower().str.replace(" ", "_")
    if payments_df is not None and not payments_df.empty:
        payments_df.columns = payments_df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Map borrower names
    bor_map = {}
    if borrowers_df is not None and not borrowers_df.empty:
        borrowers_df.columns = borrowers_df.columns.str.strip().str.lower().str.replace(" ", "_")
        bor_map = dict(zip(borrowers_df["id"].astype(str), borrowers_df["name"]))

    if "borrower" not in loans_df.columns:
        loans_df["borrower"] = loans_df["borrower_id"].astype(str).map(bor_map).fillna("Unknown")

    # ==============================
    # 🎯 SELECTION INTERFACE
    # ==============================
    loan_map = {
        f"ID: {r.get('loan_id_label', r['id'])} - {r['borrower']}": str(r["id"])
        for _, r in loans_df.iterrows()
    }

    selected_label = st.selectbox("🎯 Select Loan Account", list(loan_map.keys()))
    raw_id = loan_map[selected_label]
    
    # Corrected data check to avoid nameError 'df'
    filtered_loan = loans_df[loans_df["id"].astype(str) == raw_id]
    if filtered_loan.empty:
        st.error("Loan data not found.")
        return
        
    loan_info = filtered_loan.iloc[0]

    # ==============================
    # 📊 STATEMENT PREVIEW (BABY BLUE SNAPSHOT)
    # ==============================
    st.markdown("<h4 class='snapshot-text'>📑 Account Snapshot</h4>", unsafe_allow_html=True)
    
    p = float(loan_info.get("principal", 0))
    i = float(loan_info.get("interest", 0))
    total_due = p + i
    paid = float(loan_info.get("amount_paid", 0))
    bal = float(loan_info.get("balance", 0))

    # Metric Cards with Styled Font
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("principal", f"UGX {p:,.0f}")
    m2.metric("Total interest", f"UGX {i:,.0f}")
    m3.metric("Total Paid", f"UGX {paid:,.0f}", delta=f"{paid/total_due:.1%}" if total_due > 0 else None)
    m4.metric("Current balance", f"UGX {bal:,.0f}", delta_color="inverse", delta=f"-{paid:,.0f}")

    # ==============================
    # 📜 TRANSACTION HISTORY (LEDGER)
    # ==============================
    ledger_data = []
    running_bal = p + i

    # Entry 1: Disbursement
    ledger_data.append({
        "Date": str(loan_info.get("start_date", "-"))[:10],
        "Description": "🏦 Loan Disbursement",
        "Debit (Due)": p,
        "Credit (Paid)": 0,
        "balance": running_bal
    })

    # Entry 2: interest Charge
    if i > 0:
        ledger_data.append({
            "Date": str(loan_info.get("start_date", "-"))[:10],
            "Description": "📈 Monthly interest Applied",
            "Debit (Due)": i,
            "Credit (Paid)": 0,
            "balance": running_bal
        })

    # Entry 3+: Repayments
    if payments_df is not None and not payments_df.empty:
        rel_payments = payments_df[payments_df["loan_id"].astype(str) == raw_id]
        if not rel_payments.empty:
            for _, p_row in rel_payments.iterrows():
                amt = float(p_row.get("amount", 0))
                running_bal -= amt
                ledger_data.append({
                    "Date": str(p_row.get("date", p_row.get("payment_date", "-")))[:10],
                    "Description": "💰 Repayment Received",
                    "Debit (Due)": 0,
                    "Credit (Paid)": amt,
                    "balance": running_bal
                })

    # Render Modern Styled Ledger
    st.markdown("<br>", unsafe_allow_html=True)
    st.dataframe(
        pd.DataFrame(ledger_data),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Date": st.column_config.TextColumn("Date"),
            "Description": st.column_config.TextColumn("Transaction Details"),
            "Debit (Due)": st.column_config.NumberColumn("Debit (UGX)", format="%,d"),
            "Credit (Paid)": st.column_config.NumberColumn("Credit (UGX)", format="%,d"),
            "balance": st.column_config.NumberColumn("Running balance (UGX)", format="%,d"),
        }
    )

    st.markdown("---")

    # ==============================
    # 📄 PREMIUM DOWNLOAD SECTION
    # ==============================
    # Container for Download Area with Baby Blue Border
    st.markdown(f"""
        <div style="border: 1px solid {baby_blue}55; padding: 1.5rem; border-radius: 12px; background-color: {baby_blue}10;">
            <p style="font-family: 'Inter', sans-serif; font-weight: 600; margin-bottom: 5px;">Ready to share this ledger?</p>
            <p style="font-family: 'Inter', sans-serif; font-size: 0.9rem; color: #666; margin-bottom: 15px;">
                The premium PDF statement includes full history, company letterhead, and a formal stamp section.
            </p>
        </div>
    """, unsafe_allow_html=True)

    if st.button("✨ Generate PDF Statement", use_container_width=True):
        client_name = loan_info.get("borrower", "Unknown")
        client_loans = loans_df[loans_df["borrower"] == client_name]

        with st.spinner("Compiling Ledger..."):
            pdf = generate_pdf_statement(client_name, client_loans, payments_df)

        st.download_button(
            label=f"⬇️ Download PDF for {client_name}",
            data=pdf,
            file_name=f"Statement_{client_name.replace(' ', '_')}.pdf",
            mime="application/pdf",
            use_container_width=True
        )
