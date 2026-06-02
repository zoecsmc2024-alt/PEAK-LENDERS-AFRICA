import streamlit as st
import pandas as pd
from io import BytesIO
from datetime import datetime

# Safe ReportLab Core & Formatting Engine Imports
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet

# Core DB Utilities
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas

# ==========================================
# 📄 PREMIUM PDF STATEMENT GENERATION ENGINE
# ==========================================
def generate_pdf_statement(client_name, loans_df, payments_df):
    buffer = BytesIO()
    # Enforce safe document margins
    doc = SimpleDocTemplate(
        buffer, 
        pagesize=A4, 
        rightMargin=30, 
        leftMargin=30, 
        topMargin=30, 
        bottomMargin=30
    )
    styles = getSampleStyleSheet()
    elements = []
    
    # Statement Document Header Banner
    company_title = st.session_state.get('company_name', 'ZOE CONSULTS').upper()
    elements.append(Paragraph(f"<b>{company_title}</b>", styles["Title"]))
    elements.append(Paragraph(f"<b>Client Account Statement:</b> {client_name}", styles["Normal"]))
    elements.append(Paragraph(f"<b>Statement Issuance Date:</b> {datetime.now().strftime('%d %b %Y')}", styles["Normal"]))
    elements.append(Spacer(1, 20))
    
    grand_total = 0.0
    
    for _, loan in loans_df.iterrows():
        loan_id = str(loan["id"])
        display_id = str(loan.get("loan_id_label", loan_id)) 
        
        principal = float(loan.get("principal", 0.0))
        interest = float(loan.get("interest", 0.0))
        initial_amount = principal + interest
        
        # Isolate child client payments targeting this unique row index
        loan_payments = pd.DataFrame()
        if payments_df is not None and not payments_df.empty:
            loan_payments = payments_df[payments_df["loan_id"].astype(str) == loan_id].copy()
            
        if not loan_payments.empty:
            date_col = "payment_date" if "payment_date" in loan_payments.columns else "date"
            if date_col in loan_payments.columns:
                loan_payments[date_col] = pd.to_datetime(loan_payments[date_col], errors='coerce')
                loan_payments = loan_payments.sort_values(by=date_col)
                
        # Accurate double-entry accounting state setup
        running_pdf_bal = 0.0
        elements.append(Paragraph(f"<b>Loan Reference:</b> {display_id}", styles["Heading3"]))
        
        # Double-entry ledger columns blueprint
        data = [["Date", "Transaction Details", "Debit (Due)", "Credit (Paid)", "Running Balance"]]
        
        # Safe Date Parser
        start_date_raw = str(loan.get("start_date", loan.get("created_at", datetime.now().strftime("%Y-%m-%d"))))[:10]
        if start_date_raw.lower() in ['nan', 'none', '']:
            start_date_raw = datetime.now().strftime("%Y-%m-%d")
            
        # Entry 1: Base Principal Disbursement
        running_pdf_bal += principal
        data.append([
            start_date_raw,
            "🏦 Loan Principal Disbursement",
            f"{principal:,.0f}",
            "0",
            f"{running_pdf_bal:,.0f}"
        ])
        
        # Entry 2: Interest Applied
        if interest > 0:
            running_pdf_bal += interest
            data.append([
                start_date_raw,
                "📈 Financial Interest Applied",
                f"{interest:,.0f}",
                "0",
                f"{running_pdf_bal:,.0f}"
            ])
            
        # Entry 3+: Chronological Payments Tracking 
        if not loan_payments.empty:
            for _, p in loan_payments.iterrows():
                amount = float(p.get("amount", 0.0))
                running_pdf_bal -= amount
                
                pay_date_raw = str(p.get("payment_date", p.get("date", start_date_raw)))[:10]
                if pay_date_raw.lower() in ['nan', 'none', '']:
                    pay_date_raw = start_date_raw
                    
                data.append([
                    pay_date_raw,
                    "💰 Repayment Allocated",
                    "0",
                    f"{amount:,.0f}",
                    f"{running_pdf_bal:,.0f}"
                ])
        
        grand_total += running_pdf_bal
        
        # Render PDF Table Flow Layout
        table = Table(data, repeatRows=1, colWidths=[75, 170, 85, 85, 100])
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2B3F87")), # Match Enterprise Brand Identity Blueprint
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ALIGN", (2, 1), (-1, -1), "RIGHT"),
            ("ALIGN", (0, 0), (0, -1), "LEFT"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        
        elements.append(table)
        elements.append(Spacer(1, 15))
        
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(f"<b>Aggregate Outstanding Balance: UGX {grand_total:,.0f}</b>", styles["Title"]))
    
    doc.build(elements)
    buffer.seek(0)
    return buffer

# ==========================================
# 📘 MAIN LEDGER DASHBOARD INTERFACE
# ==========================================
def show_ledger():
    baby_blue = "#89CFF0"
    st.markdown(f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
            .ledger-header {{
                font-family: 'Inter', sans-serif;
                color: #2B3F87;
                font-weight: 700;
                letter-spacing: -0.5px;
                }}
            .snapshot-text {{
                font-family: 'Inter', sans-serif;
                font-weight: 600;
                color: #333333;
            }}
        </style>
        <h2 class='ledger-header'>📘 Master Account Ledger</h2>
    """, unsafe_allow_html=True)

    # 📥 DATA PIPELINE FETCH LAYER
    # FIX: Added tenant variable allocation tracking to catch multi-tenant criteria explicitly
    current_tenant = st.session_state.get('tenant_id')
    loans_raw = get_cached_data("loans", current_tenant)
    payments_raw = get_cached_data("payments", current_tenant)
    borrowers_raw = get_cached_data("borrowers", current_tenant)

    if loans_raw is None or (isinstance(loans_raw, pd.DataFrame) and loans_raw.empty) or len(loans_raw) == 0:
        st.info("💡 Portfolio Clear: There are currently no active system loan accounts found.")
        return

    # Normalize Schema Names to Uniform Formats
    loans_df = pd.DataFrame(loans_raw).copy()
    payments_df = pd.DataFrame(payments_raw).copy() if payments_raw is not None else pd.DataFrame()
    borrowers_df = pd.DataFrame(borrowers_raw).copy() if borrowers_raw is not None else pd.DataFrame()

    loans_df.columns = loans_df.columns.str.strip().str.lower().str.replace(" ", "_")
    if not payments_df.empty:
        payments_df.columns = payments_df.columns.str.strip().str.lower().str.replace(" ", "_")
    if not borrowers_df.empty:
        borrowers_df.columns = borrowers_df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Resolve Unknown Borrowers Safely 
    bor_map = dict(zip(borrowers_df["id"].astype(str), borrowers_df["name"])) if "id" in borrowers_df.columns and "name" in borrowers_df.columns else {}
    if "borrower" not in loans_df.columns and "borrower_id" in loans_df.columns:
        loans_df["borrower"] = loans_df["borrower_id"].astype(str).map(bor_map).fillna("Unknown Account Holder")
    elif "borrower" not in loans_df.columns:
        loans_df["borrower"] = "Unknown Account Holder"

    # ==========================================
    # 🎯 SELECTION INTERFACE MATRIX
    # ==========================================
    loan_map = {
        f"Ref: {r.get('loan_id_label', r['id'])} — {r['borrower']}": str(r["id"])
        for _, r in loans_df.iterrows()
    }
    
    selected_label = st.selectbox("🎯 Select Active Target Loan Account File", list(loan_map.keys()))
    raw_id = loan_map[selected_label]
    
    filtered_loan = loans_df[loans_df["id"].astype(str) == raw_id]
    if filtered_loan.empty:
        st.error("❌ Data Sync Failure: Target account profiles could not be resolved.")
        return
    loan_info = filtered_loan.iloc[0]

    # ==========================================
    # 📊 ACCOUNT SNAPSHOT DASHBOARD METRICS
    # ==========================================
    st.markdown("<h4 class='snapshot-text'>📑 Account Balance Breakdown</h4>", unsafe_allow_html=True)
    
    p = float(loan_info.get("principal", 0.0))
    i = float(loan_info.get("interest", 0.0))
    total_due = p + i
    paid = float(loan_info.get("amount_paid", 0.0))
    bal = float(loan_info.get("balance", 0.0))
    
    m1, m2, m3, m4 = st.columns(4)
    
    m1.metric("Principal Issued", f"UGX {p:,.0f}", delta="Base Disbursed Amount", delta_color="off")
    m2.metric("Total Interest Charged", f"UGX {i:,.0f}", delta=f"{(i/total_due*100):.1f}% of Cost" if total_due > 0 else "0%")
    m3.metric("Total Payments Settled", f"UGX {paid:,.0f}", delta=f"{paid/total_due:.1%} Settled" if total_due > 0 else "0%")
    m4.metric("Current Due Balance", f"UGX {bal:,.0f}", delta=f"{bal:,.0f} Outstanding", delta_color="inverse")

    # ==========================================
    # 📜 INTERACTIVE DOUBLE-ENTRY LEDGER GENERATION
    # ==========================================
    ledger_data = []
    running_ui_bal = 0.0
    start_date_str = str(loan_info.get("start_date", "-"))[:10]

    # Chronological Row 1: Principal Allocation
    running_ui_bal += p
    ledger_data.append({
        "date": start_date_str,
        "Description": "🏦 Loan Principal Disbursement",
        "Debit (Due)": p,
        "Credit (Paid)": 0.0,
        "Balance": running_ui_bal
    })

    # Chronological Row 2: Interest Escalation 
    if i > 0:
        running_ui_bal += i
        ledger_data.append({
            "date": start_date_str,
            "Description": "📈 System Financial Interest Applied",
            "Debit (Due)": i,
            "Credit (Paid)": 0.0,
            "Balance": running_ui_bal
        })

    # Chronological Row 3+: Repayment Deductions
    if not payments_df.empty and "loan_id" in payments_df.columns:
        rel_payments = payments_df[payments_df["loan_id"].astype(str) == raw_id]
        if not rel_payments.empty:
            # Sort payments by date column dynamically
            pay_date_col = "date" if "date" in rel_payments.columns else "payment_date"
            if pay_date_col in rel_payments.columns:
                rel_payments = rel_payments.copy()
                rel_payments[pay_date_col] = pd.to_datetime(rel_payments[pay_date_col], errors='coerce')
                rel_payments = rel_payments.sort_values(by=pay_date_col)
                
            for _, p_row in rel_payments.iterrows():
                amt = float(p_row.get("amount", 0.0))
                running_ui_bal -= amt
                
                payment_date_str = str(p_row.get("date", p_row.get("payment_date", "-")))[:10]
                ledger_data.append({
                    "date": payment_date_str,
                    "Description": "💰 Repayment Transaction Received",
                    "Debit (Due)": 0.0,
                    "Credit (Paid)": amt,
                    "Balance": running_ui_bal
                })

    st.markdown("<br>", unsafe_allow_html=True)
    st.dataframe(
        pd.DataFrame(ledger_data),
        use_container_width=True,
        hide_index=True,
        column_config={
            "date": st.column_config.TextColumn("Transaction Date"),
            "Description": st.column_config.TextColumn("Transaction Details Description"),
            "Debit (Due)": st.column_config.NumberColumn("Debit (UGX)", format="%,d"),
            "Credit (Paid)": st.column_config.NumberColumn("Credit (UGX)", format="%,d"),
            "Balance": st.column_config.NumberColumn("Running Balance (UGX)", format="%,d"),
        }
    )

    st.divider()

    # ==========================================
    # 📄 EXPORT PLATFORM PORTAL WITH PREVIEW LAYER
    # ==========================================
    st.markdown(f"""
        <div style="border: 1px solid {baby_blue}77; padding: 1.5rem; border-radius: 14px; background-color: {baby_blue}15;">
            <p style="font-family: 'Inter', sans-serif; font-weight: 600; margin-bottom: 5px; color:#1F2937;">Ready to export this account file ledger?</p>
            <p style="font-family: 'Inter', sans-serif; font-size: 0.88rem; color: #4B5563; margin-bottom: 15px;">
                Generates an industry-standard audit-compliant PDF document incorporating organizational branding, tabular breakdowns, and processing balance anchors.
            </p>
        </div>
    """, unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    client_name = loan_info.get("borrower", "Unknown Account Holder")
    client_loans = loans_df[loans_df["borrower"] == client_name]

    # ADDED: Interactive Live Document Statement Preview Matrix UI Block
    with st.expander("🔍 Preview Statement Document Structure Before Compiling", expanded=False):
        company_title_preview = st.session_state.get('company_name', 'ZOE CONSULTS').upper()
        st.markdown(f"### {company_title_preview}")
        st.markdown(f"**Client Statement Account Target:** {client_name}")
        st.markdown(f"**Generated On:** {datetime.now().strftime('%d %b %Y')}")
        st.divider()
        
        # Build out a mirrored, clean presentation breakdown of all metrics bound to this client's profile
        preview_data = []
        for _, c_loan in client_loans.iterrows():
            st.markdown(f"📄 **Loan Reference Protocol Block:** {c_loan.get('loan_id_label', c_loan['id'])}")
            c_p = float(c_loan.get("principal", 0.0))
            c_i = float(c_loan.get("interest", 0.0))
            c_paid = float(c_loan.get("amount_paid", 0.0))
            c_bal = float(c_loan.get("balance", 0.0))
            
            st.text(f"   • Principal Balance: UGX {c_p:,.0f} | Interest Balance: UGX {c_i:,.0f}")
            st.text(f"   • Total Disbursed Matrix: UGX {(c_p + c_i):,.0f} | Amount Paid: UGX {c_paid:,.0f}")
            st.markdown(f"   • **Outstanding Allocation Net Due: UGX {c_bal:,.0f}**")
            st.write("")

    if st.button("✨ Compile Formal PDF Statement", use_container_width=True):
        with st.spinner("Compiling cryptographic ledger data rows..."):
            pdf_report = generate_pdf_statement(client_name, client_loans, payments_df)

        st.download_button(
            label=f"⬇️ Download PDF Audit Report for {client_name}",
            data=pdf_report,
            file_name=f"Statement_{client_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf",
            mime="application/pdf",
            use_container_width=True
        )
