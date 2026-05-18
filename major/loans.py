import streamlit as st
import pandas as pd
import uuid
from datetime import datetime, timedelta
from core.database import supabase, get_cached_data

# ==========================================
# 🔐 SAAS TENANT CONTEXT (UUID SAFE)
# ==========================================
def get_current_tenant():
    """Returns tenant UUID only"""
    tenant_id = st.session_state.get("tenant_id", None)
    if tenant_id in [None, "", "default_tenant"]:
        return None
    return str(tenant_id)


# ==========================================
# 🧠 DATABASE ADAPTER (MULTI-TENANT SAFE)
# ==========================================
def get_data(table_name):
    tenant_id = str(get_current_tenant()).strip()
    df = get_cached_data(table_name)

    if df is None:
        return pd.DataFrame()

    if df.empty:
        return df

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    if "tenant_id" in df.columns:
        df["tenant_id"] = df["tenant_id"].astype(str).str.strip()
        df = df[df["tenant_id"] == tenant_id].copy()

    return df.reset_index(drop=True)

def save_data_saas(table_name, df):
    tenant_id = get_current_tenant()
    if tenant_id:
        df["tenant_id"] = str(tenant_id)
    return save_data(table_name, df)


# =========================================================
# 🎨 LOANS PAGE PREMIUM STYLING (V2)
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


# ==========================================
# 💵 LOANS MANAGEMENT PAGE
# ==========================================
def show_loans():
    # Inject V2 Base Stylesheets
    loans_styles()

    # Premium Layout Header Component
    st.markdown("""
    <div class="loans-header">
        <h1>💵 Loans Portfolio & Risk Management</h1>
    </div>
    """, unsafe_allow_html=True)

    # ------------------------------
    # LOAD DATA (V1 Multi-Tenant Wrappers)
    # ------------------------------
    raw_loans = get_data("loans")
    borrowers_df = get_data("borrowers")
    payments_df = get_data("payments")

    # V2 Protection: Filter out accidental string calculations or summaries
    if not raw_loans.empty and "status" in raw_loans.columns:
        loans_df = raw_loans[raw_loans["status"] != "TOTAL"].copy()
    else:
        loans_df = raw_loans.copy()

    # ------------------------------
    # SAFETY FALLBACKS & DEFAULTS
    # ------------------------------
    if loans_df.empty:
        loans_df = pd.DataFrame(columns=[
            "id", "sn", "loan_id_label", "parent_loan_id", "borrower_id",
            "borrower", "loan_type", "principal", "interest", "total_repayable",
            "amount_paid", "balance", "status", "start_date", "end_date", "cycle_no", "tenant_id"
        ])

    if borrowers_df.empty:
        borrowers_df = pd.DataFrame(columns=["id", "name", "status"])

    if payments_df.empty:
        payments_df = pd.DataFrame(columns=["loan_id", "amount"])

    required_defaults = {
        "id": "", "sn": "", "loan_id_label": "", "parent_loan_id": "", "borrower_id": "",
        "borrower": "", "loan_type": "", "principal": 0.0, "interest": 0.0,
        "total_repayable": 0.0, "amount_paid": 0.0, "balance": 0.0, "status": "ACTIVE",
        "start_date": "", "end_date": "", "cycle_no": 1, "tenant_id": ""
    }

    for col, val in required_defaults.items():
        if col not in loans_df.columns:
            loans_df[col] = val

    loans_df = loans_df.copy()
    payments_df = payments_df.copy()

    # Strict Casting
    loans_df["id"] = loans_df["id"].astype(str)
    loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)
    loans_df["parent_loan_id"] = loans_df["parent_loan_id"].fillna("").astype(str)

    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].astype(str)

    # Numerical Engine Formatting
    for col in ["principal", "interest", "total_repayable", "amount_paid", "balance"]:
        loans_df[col] = pd.to_numeric(loans_df[col], errors="coerce").fillna(0)

    if not payments_df.empty and "amount" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(payments_df["amount"], errors="coerce").fillna(0)

    for col in ["start_date", "end_date"]:
        loans_df[col] = pd.to_datetime(loans_df[col], errors="coerce")

    # ------------------------------
    # REAL-TIME PAYMENT RECONCILIATION
    # ------------------------------
    loans_df["amount_paid"] = 0.0
    if not payments_df.empty and "loan_id" in payments_df.columns:
        pay_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(pay_sums).fillna(0)

    loans_df["balance"] = (loans_df["total_repayable"] - loans_df["amount_paid"]).clip(lower=0)

    # ==============================
    # SERIAL ENGINE (V1 TRUSTED LINEAGE WALKER)
    # ==============================
    existing_nums = []
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip()
    existing_sn_map = dict(zip(loans_df["id"], loans_df["sn"]))

    for val in loans_df["sn"]:
        if val.startswith("LN-"):
            try:
                existing_nums.append(int(val.replace("LN-", "")))
            except:
                pass

    next_sn_val = max(existing_nums, default=0)

    for i in loans_df.index:
        current_id = loans_df.at[i, "id"]
        existing_sn = str(existing_sn_map.get(current_id, "")).strip()
        
        if existing_sn.startswith("LN-"):
            continue

        parent_id = str(loans_df.at[i, "parent_loan_id"]).strip()
        inherited_sn = ""

        # Walk Deep Family Tree Recursively
        while parent_id != "":
            parent_match = loans_df[loans_df["id"] == parent_id]
            if parent_match.empty:
                break
            parent_row = parent_match.iloc[0]
            parent_sn = str(parent_row["sn"]).strip()

            if parent_sn.startswith("LN-"):
                inherited_sn = parent_sn
                break
            parent_id = str(parent_row["parent_loan_id"]).strip()

        if inherited_sn:
            loans_df.at[i, "sn"] = inherited_sn

        if not str(loans_df.at[i, "sn"]).startswith("LN-"):
            next_sn_val += 1
            loans_df.at[i, "sn"] = f"LN-{next_sn_val:04d}"

    # Sort Chronologically for Cycle Identifications
    loans_df = loans_df.sort_values(by=["sn", "start_date", "id"])
    loans_df["cycle_no"] = loans_df.groupby("sn").cumcount() + 1

    # ==============================
    # SMART STATUS ENGINE V2 (V1 TRUTH)
    # ==============================
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip().str.upper()
    loans_df = loans_df.sort_values(by=["sn", "cycle_no", "start_date"])

    for sn_val, grp in loans_df.groupby("sn"):
        indices = grp.index.tolist()
        latest_idx = indices[-1]

        # Prior historical steps in generation chain auto-convert to Brought Forward
        if len(indices) > 1:
            loans_df.loc[indices[:-1], "status"] = "BCF"

        latest_row = loans_df.loc[latest_idx]

        if abs(latest_row["balance"]) < 1.0:
            loans_df.at[latest_idx, "status"] = "CLEARED"
        else:
            if int(latest_row["cycle_no"]) == 1:
                loans_df.at[latest_idx, "status"] = "ACTIVE"
            else:
                loans_df.at[latest_idx, "status"] = "PENDING"

    # Absolute System Constraints
    loans_df.loc[loans_df["balance"] <= 0, "status"] = "CLEARED"

    # Final Serialization Sort 
    loans_df = loans_df.sort_values(by=["sn", "cycle_no"], ascending=[True, True]).reset_index(drop=True)
    loans_df["loan_id_label"] = loans_df["sn"].str.replace("LN-", "", regex=False).str.zfill(4)

    # ==============================
    # OPTIMIZED DATABASE SYNC ENGINE (V1)
    # ==============================
    raw_db_df = get_cached_data("loans")
    
    def needs_update(row):
        if raw_db_df is None or raw_db_df.empty:
            return True
        db_match = raw_db_df[raw_db_df["id"] == row["id"]]
        if db_match.empty:
            return True
        db_row = db_match.iloc[0]
        return (str(db_row.get("sn", "")) != str(row["sn"]) or 
                str(db_row.get("loan_id_label", "")) != str(row["loan_id_label"]) or
                int(db_row.get("cycle_no", 0)) != int(row["cycle_no"]))

    to_sync = loans_df[loans_df.apply(needs_update, axis=1)]

    if not to_sync.empty:
        with st.status("🔄 Synchronizing Generated Ledger Formats...", expanded=False) as status:
            for _, row in to_sync.iterrows():
                sync_data = {
                    "sn": row["sn"],
                    "loan_id_label": row["loan_id_label"],
                    "cycle_no": int(row["cycle_no"])
                }
                try:
                    supabase.table("loans").update(sync_data).eq("id", row["id"]).execute()
                except Exception as e:
                    st.error(f"Sync Interrupted on Item Row {row['id']}: {e}")
            
            st.cache_data.clear()
            status.update(label="✅ Database Records Validated and Aligned!", state="complete", expanded=False)
            st.rerun()

    # Dynamic Relational Mapping
    if not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        loans_df["borrower"] = loans_df["borrower_id"].map(bor_map).fillna(loans_df["borrower"]).fillna("Unknown")

    active_borrowers_unit = (
        borrowers_df[borrowers_df["status"].astype(str).str.upper() == "ACTIVE"] 
        if "status" in borrowers_df.columns else pd.DataFrame(columns=["id", "name"])
    )

    # ==============================
    # TAB CONFIGURATION LAYOUT
    # ==============================
    tab_view, tab_add, tab_manage, tab_actions = st.tabs([
        "📑 Portfolio View",
        "➕ New Loan Agreement",
        "🛠️ Ledger Modification Engine",
        "⚙️ Financial Rollover Operations"
    ])

    # ==============================
    # TAB 1: PORTFOLIO VIEW
    # ==============================
    with tab_view:
        search_query = st.text_input("🔍 Filter Master Book (Borrower Name, Serial, Class Type)", key="loan_search_main")
        filtered_loans = loans_df.copy()

        if not filtered_loans.empty and search_query:
            filtered_loans = filtered_loans[
                filtered_loans.apply(lambda r: search_query.lower() in str(r).lower(), axis=1)
            ]

        # Fiscal Reporting Calculations (July–June Bounds Validation)
        if not filtered_loans.empty:
            start_dt = pd.to_datetime(filtered_loans["start_date"], errors="coerce")
            start_dt = start_dt.fillna(pd.Timestamp.today())
            
            fiscal_years_list = [
                f"{dt.year}/{dt.year + 1}" if dt.month >= 7 else f"{dt.year - 1}/{dt.year}" 
                for dt in start_dt
            ]
            filtered_loans["fiscal_year"] = fiscal_years_list

            fy_unique = sorted(list(set(fiscal_years_list)))
            fy_selected = st.selectbox("📅 Structural Fiscal Reporting Cycle", ["All Business Years"] + fy_unique)
            
            if fy_selected != "All Business Years":
                filtered_loans = filtered_loans[filtered_loans["fiscal_year"] == fy_selected]

        # ------------------------------
        # METRIC METRICS CONTAINER BLOCK (V2 Premium Gradations)
        # ------------------------------
        if not filtered_loans.empty:
            total_loans = filtered_loans["sn"].nunique()
            original_loans = filtered_loans[filtered_loans["cycle_no"] == 1]  
            total_principal = original_loans["principal"].sum()
            total_paid = filtered_loans["amount_paid"].sum()
            total_pending = filtered_loans[filtered_loans["status"] == "PENDING"]["total_repayable"].sum()

            m_col1, m_col2, m_col3, m_col4 = st.columns(4)
            m_col1.markdown(f'<div style="background: linear-gradient(135deg, #3b82f6, #1e3a8a); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:13px;opacity:0.9;">Total Unique Loans</div><div style="font-size:22px;font-weight:bold;">{total_loans}</div></div>', unsafe_allow_html=True)
            m_col2.markdown(f'<div style="background: linear-gradient(135deg, #10b981, #065f46); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:13px;opacity:0.9;">Principal Exposure</div><div style="font-size:22px;font-weight:bold;">{total_principal:,.0f} UGX</div></div>', unsafe_allow_html=True)
            m_col3.markdown(f'<div style="background: linear-gradient(135deg, #f59e0b, #92400e); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:13px;opacity:0.9;">Total Recovered</div><div style="font-size:22px;font-weight:bold;">{total_paid:,.0f} UGX</div></div>', unsafe_allow_html=True)
            m_col4.markdown(f'<div style="background: linear-gradient(135deg, #ef4444, #991b1b); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:13px;opacity:0.9;">Active Risk Tranche</div><div style="font-size:22px;font-weight:bold;">{total_pending:,.0f} UGX</div></div>', unsafe_allow_html=True)
            
            st.markdown("---")

            # ------------------------------
            # STYLED DATAFRAME VIEW PORT
            # ------------------------------
            show_cols = ["sn", "loan_id_label", "borrower", "cycle_no", "principal", "total_repayable", "amount_paid", "balance", "start_date", "end_date", "status"]
            
            def style_entire_row(row):
                val = str(row["status"]).upper().strip()
                color_map = {
                    "ACTIVE": "background-color:#dbeafe; color:#1e40af; font-weight:bold;",
                    "PENDING": "background-color:#fee2e2; color:#991b1b; font-weight:bold;",
                    "CLEARED": "background-color:#d1fae5; color:#065f46;",
                    "BCF": "background-color:#ffedd5; color:#9a3412;",
                    "CLOSED": "background-color:#f3f4f6; color:#374151;"
                }
                return [color_map.get(val, "")] * len(row)

            styled_df = (
                filtered_loans[show_cols].style
                .apply(style_entire_row, axis=1)
                .format({
                    "principal": "{:,.0f}",
                    "amount_paid": "{:,.0f}",
                    "total_repayable": "{:,.0f}",
                    "balance": "{:,.0f}",
                    "start_date": lambda t: t.strftime('%Y-%m-%d') if not pd.isna(t) else '',
                    "end_date": lambda t: t.strftime('%Y-%m-%d') if not pd.isna(t) else ''
                })
            )

            st.dataframe(styled_df, column_order=show_cols, use_container_width=True, hide_index=True)
        else:
            st.warning("No operational loan files discovered matching target filter variants.")

    # ==============================
    # TAB 2: NEW LOAN AGREEMENT
    # ==============================
    with tab_add:
        if active_borrowers_unit.empty:
            st.info("💡 Structural Advisory: Active borrower profile verification missing. Complete entity registration initialization loops.")
        else:
            with st.form("loan_issue_form_v2"):
                st.markdown("<h4 style='color:#0A192F;'>📝 Originate Secured Asset Placement</h4>", unsafe_allow_html=True)
                col1, col2 = st.columns(2)

                borrower_map = dict(zip(active_borrowers_unit["name"], active_borrowers_unit["id"]))
                selected_name = col1.selectbox("Target Counterparty Asset Entity", list(borrower_map.keys()))
                selected_id = str(borrower_map[selected_name]).strip()

                amount = col1.number_input("Principal Commitment Target (UGX)", min_value=0, step=50000)
                date_issued = col1.date_input("Origination Execution Date", value=datetime.now())

                loan_type = col2.selectbox("Underwriting Allocation Frame", ["Business", "Personal", "Emergency", "Other"])
                interest_rate = col2.number_input("Nominal Base Margin Multiplier (%)", min_value=0.0, step=0.5)
                date_due = col2.date_input("Target Maturation End Date", value=date_issued + timedelta(days=30))

                total_due = amount + (amount * interest_rate / 100)
                st.info(f"Origination Parameter Ledger Preview: Total Repayable calculated at {total_due:,.0f} UGX")

                if st.form_submit_button("🚀 Finalize Underwriting Placement"):
                    tenant_id = get_current_tenant()
                    if not tenant_id:
                        st.error("System Error: Tenant access session configuration data corrupted.")
                        st.stop()

                    loan_data = {
                        "id": str(uuid.uuid4()), "sn": "", "loan_id_label": "", "parent_loan_id": None,
                        "borrower_id": selected_id, "borrower": selected_name, "loan_type": loan_type,
                        "principal": float(amount), "interest": float(amount * interest_rate / 100),
                        "total_repayable": float(total_due), "amount_paid": 0.0, "balance": float(total_due),
                        "status": "ACTIVE", "start_date": str(date_issued), "end_date": str(date_due),
                        "cycle_no": 1, "tenant_id": tenant_id
                    }

                    if save_data_saas("loans", pd.DataFrame([loan_data])):
                        st.success("✅ Underlying Loan Agreement Registered Successfully.")
                        st.cache_data.clear()
                        st.rerun()

    # ==============================
    # TAB 3: FINANCIAL ROLLOVER OPERATIONS
    # ==============================
    with tab_actions:
        st.markdown("<h4 style='color: #0A192F;'>🔄 Multi-Stage Lineage Rollover Protocol</h4>", unsafe_allow_html=True)
        eligible_loans = loans_df[(~loans_df["status"].isin(["CLEARED"])) & (loans_df["balance"] > 0)]

        if eligible_loans.empty:
            st.success("Zero Exposure Risk: All accounts normalized or fully settled. ✨")
        else:
            roll_map = {
                f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']} • Bal: {row['balance']:,.0f}": row["id"]
                for _, row in eligible_loans.iterrows()
            }
            roll_sel = st.selectbox("Select Exposure Target to Roll Forward", list(roll_map.keys()))
            parent_id = roll_map[roll_sel]
            loan_to_roll = eligible_loans[eligible_loans["id"] == parent_id].iloc[0]

            new_interest_rate = st.number_input("New Term Periodic Interest Multiplier (%)", value=3.0, step=0.5)

            if st.button("🔥 Execute Lineage Node Chain Step", use_container_width=True):
                old_due = pd.to_datetime(loan_to_roll["end_date"], errors="coerce")
                if pd.isna(old_due):
                    old_due = datetime.now()

                new_start = old_due
                new_due = old_due + timedelta(days=30)
                current_status = str(loan_to_roll["status"]).strip().upper()

                if current_status == "PENDING":
                    loans_df.loc[loans_df["id"] == parent_id, "status"] = "BCF"

                save_data_saas("loans", loans_df)

                unpaid = float(loan_to_roll["balance"])
                new_interest = unpaid * (new_interest_rate / 100)

                new_row = {
                    "id": str(uuid.uuid4()), "sn": "", "loan_id_label": "", "parent_loan_id": parent_id,
                    "borrower_id": loan_to_roll["borrower_id"], "loan_type": loan_to_roll["loan_type"],
                    "principal": unpaid, "interest": new_interest, "total_repayable": unpaid + new_interest,
                    "amount_paid": 0.0, "balance": unpaid + new_interest, "status": "PENDING",
                    "start_date": str(new_start.date()), "end_date": str(new_due.date()),
                    "cycle_no": int(loan_to_roll["cycle_no"]) + 1, "tenant_id": get_current_tenant()
                }

                if save_data_saas("loans", pd.DataFrame([new_row])):
                    st.success(f"✅ Lineage Block Configured. Asset Allocated to Trailing Cycle Row {int(loan_to_roll['cycle_no']) + 1}")
                    st.cache_data.clear()
                    st.rerun()

    # ==============================
    # TAB 4: LEDGER MODIFICATION ENGINE
    # ==============================
    with tab_manage:
        if not loans_df.empty:
            edit_map = {
                f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']}": row["id"]
                for _, row in loans_df.iterrows()
            }
            selected = st.selectbox("Select Target Segment Entry Row", list(edit_map.keys()))
            target_id = edit_map[selected]
            loan_match = loans_df[loans_df["id"] == target_id]

            if not loan_match.empty:
                loan_to_edit = loan_match.iloc[0]

                with st.form(f"edit_form_{target_id}"):
                    e_princ = st.number_input("Alter Asset Core Principal Base Value", value=float(loan_to_edit["principal"]))
                    status_options = ["ACTIVE", "PENDING", "CLEARED", "BCF", "CLOSED"]
                    current_stat = str(loan_to_edit["status"]).upper()
                    idx = status_options.index(current_stat) if current_stat in status_options else 0
                    e_stat = st.selectbox("Manual State Flag Assignment", status_options, index=idx)

                    if st.form_submit_button("💾 Force Commit System Database Refactor"):
                        supabase.table("loans").update({"principal": e_princ, "status": e_stat}).eq("id", target_id).execute()
                        st.success("✅ Ledger Elements Rewritten and Synchronized Globally.")
                        st.cache_data.clear()
                        st.rerun()

                # Core Data Deletion Layer
                st.markdown("<br><hr><h5 style='color:#dc2626;'>Destructive Superuser Protocols</h5>", unsafe_allow_html=True)
                if st.button("🗑️ Drop Target Entry Record Irreversibly From Table Rows", use_container_width=True):
                    supabase.table("loans").delete().eq("id", target_id).execute()
                    st.warning("Data dropped from cloud relational sequence mapping context.")
                    st.cache_data.clear()
                    st.rerun()
