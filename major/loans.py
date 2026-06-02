import streamlit as st
import pandas as pd
import uuid
import datetime as dt_mod
from datetime import datetime, timedelta
from core.database import save_data_saas, get_cached_data

# Ensure supabase connection client context is available globally
from core.database import supabase 

# ==============================
# 🔐 SAAS TENANT CONTEXT (UUID SAFE)
# ==============================

def get_current_tenant():
    """Returns tenant UUID only"""
    tenant_id = st.session_state.get("tenant_id", None)

    if tenant_id in [None, "", "default_tenant"]:
        return None

    return str(tenant_id)


# ==============================
# 🧠 DATABASE ADAPTER (MULTI-TENANT SAFE)
# ==============================
def get_data(table_name):
    tenant_id = str(get_current_tenant()).strip()
    
    # FIX: Pass tenant_id into the cached function so Streamlit tracks it as a cache key!
    df = get_cached_data(table_name, tenant_id)

    if df is None:
        return pd.DataFrame()

    if df.empty:
        return df

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    if "tenant_id" in df.columns:
        df["tenant_id"] = df["tenant_id"].astype(str).str.strip()
        
        # This local filtering acts as an excellent secondary security safety net
        df = df[df["tenant_id"] == tenant_id].copy()

    return df.reset_index(drop=True)

def save_data_saas_local(table_name, df):
    """Local fallback connector bypassing recursion loop names"""
    tenant_id = get_current_tenant()

    if tenant_id:
        df["tenant_id"] = str(tenant_id)

    return save_data_saas(table_name, df)


# ==============================
# 13. LOANS MANAGEMENT PAGE
# ==============================
def show_loans():
    # ==============================
    # 🎨 1. MASTER BUTTON STYLING (GLOBAL OVERRIDE)
    # ==============================
    st.markdown("""
    <style>
    /* MASTER BUTTON SELECTOR - Applies the premium look to ALL standard buttons */
    div.stButton > button,
    div.stFormSubmitButton > button {
        background: linear-gradient(135deg, #1d4ed8 0%, #1e3a8a 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.7rem 1.5rem !important;
        font-weight: 700 !important;
        font-size: 15px !important;
        height: 48px !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        gap: 8px !important;
        transition: all 0.25s ease-out !important;
        box-shadow: 0 4px 14 rgba(30, 58, 138, 0.2) !important;
        width: auto;
    }
    
    div.stButton > button[width="100%"] {
        width: 100% !important;
    }

    /* HOVER STATE (FLOAT EFFECT) */
    div.stButton > button:hover,
    div.stFormSubmitButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 25px rgba(30, 58, 138, 0.35) !important;
        background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%) !important;
    }
    
    /* ACTIVE/CLICK STATE */
    div.stButton > button:active,
    div.stFormSubmitButton > button:active {
        transform: translateY(1px) !important;
        box-shadow: 0 2px 8px rgba(30, 58, 138, 0.15) !important;
    }

    /* Specific icon adjustments */
    .save-icon { font-size: 1.1em; color: #a3e635; margin-right: 2px; }
    .cancel-icon { font-size: 1.1em; color: #f87171; margin-right: 2px; }
    .download-icon { font-size: 1.1em; color: #60a5fa; margin-right: 2px; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown(
        "<h2 style='color: #0A192F;'>💵 Loans Management</h2>",
        unsafe_allow_html=True
    )

    # ------------------------------
    # LOAD DATA
    # ------------------------------
    loans_df = get_data("loans")
    borrowers_df = get_data("borrowers")
    payments_df = get_data("payments")

    # ------------------------------
    # SAFETY FALLBACKS
    # ------------------------------
    if loans_df is None or loans_df.empty:
        loans_df = pd.DataFrame(columns=[
            "id", "sn", "loan_id_label", "parent_loan_id", "borrower_id",
            "borrower", "loan_type", "principal", "interest", "total_repayable",
            "amount_paid", "balance", "status", "start_date", "end_date",
            "cycle_no", "tenant_id"
        ])

    if borrowers_df is None:
        borrowers_df = pd.DataFrame()

    if payments_df is None:
        payments_df = pd.DataFrame()

    # ------------------------------
    # REQUIRED DEFAULTS
    # ------------------------------
    required_defaults = {
        "id": "", "sn": "", "loan_id_label": "", "parent_loan_id": "",
        "borrower_id": "", "borrower": "", "loan_type": "", "principal": 0.0,
        "interest": 0.0, "total_repayable": 0.0, "amount_paid": 0.0,
        "balance": 0.0, "status": "ACTIVE", "start_date": "", "end_date": "",
        "cycle_no": 1, "tenant_id": ""
    }

    for col, val in required_defaults.items():
        if col not in loans_df.columns:
            loans_df[col] = val

    loans_df = loans_df.copy()
    payments_df = payments_df.copy()

    # ------------------------------
    # TYPE CLEANUP
    # ------------------------------
    loans_df["id"] = loans_df["id"].astype(str)
    loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)
    loans_df["parent_loan_id"] = loans_df["parent_loan_id"].fillna("").astype(str)

    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].astype(str)

    # ------------------------------
    # NUMERIC CLEANUP
    # ------------------------------
    for col in ["principal", "interest", "total_repayable", "amount_paid", "balance"]:
        loans_df[col] = pd.to_numeric(loans_df[col], errors="coerce").fillna(0)

    if not payments_df.empty and "amount" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(payments_df["amount"], errors="coerce").fillna(0)

    # ------------------------------
    # DATE CLEANUP
    # ------------------------------
    for col in ["start_date", "end_date"]:
        loans_df[col] = pd.to_datetime(loans_df[col], errors="coerce")

    # ------------------------------
    # PAYMENT SYNC
    # ------------------------------
    loans_df["amount_paid"] = 0

    if not payments_df.empty and "loan_id" in payments_df.columns:
        pay_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(pay_sums).fillna(0)

    loans_df["balance"] = (loans_df["total_repayable"] - loans_df["amount_paid"]).clip(lower=0)

    # ==============================
    # SERIAL ENGINE
    # ==============================
    existing_nums = []
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip()
    existing_sn_map = dict(zip(loans_df["id"], loans_df["sn"]))
    
    for val in loans_df["sn"]:
        val = val.strip()
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
    
    loans_df = loans_df.sort_values(by=["sn", "start_date", "id"])
    loans_df["cycle_no"] = loans_df.groupby("sn").cumcount() + 1
    
    # ------------------------------
    # REVISED SMART STATUS LOGIC (V2)
    # ------------------------------
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip().str.upper()
    loans_df = loans_df.sort_values(by=["sn", "cycle_no", "start_date"])
    
    for sn_val, grp in loans_df.groupby("sn"):
        indices = grp.index.tolist()
        if len(indices) > 1:
            loans_df.loc[indices[:-1], "status"] = "BCF"
        
        latest_idx = indices[-1]
        latest_row = loans_df.loc[latest_idx]
        
        if abs(latest_row["balance"]) < 1.0: 
            loans_df.at[latest_idx, "status"] = "CLEARED"
        else:
            if int(latest_row["cycle_no"]) == 1:
                loans_df.at[latest_idx, "status"] = "ACTIVE"
            else:
                loans_df.at[latest_idx, "status"] = "PENDING"
    
    mask_zero = (loans_df["balance"] <= 0) & (loans_df["status"] != "BCF")
    loans_df.loc[mask_zero, "status"] = "CLEARED"
    loans_df.loc[loans_df["balance"] <= 0, "status"] = "CLEARED"
    
    loans_df = loans_df.sort_values(by=["sn", "cycle_no"], ascending=[True, True]).reset_index(drop=True)
    loans_df["loan_id_label"] = loans_df["sn"].str.replace("LN-", "", regex=False).str.zfill(4)

    # ==============================
    # 🔄 DATABASE SYNC ENGINE (OPTIMIZED)
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
        with st.status("🔄 Syncing Serial Numbers to Database...", expanded=False) as status:
            for _, row in to_sync.iterrows():
                sync_data = {
                    "sn": row["sn"],
                    "loan_id_label": row["loan_id_label"],
                    "cycle_no": int(row["cycle_no"])
                }
                try:
                    supabase.table("loans").update(sync_data).eq("id", row["id"]).execute()
                except Exception as e:
                    st.error(f"Error syncing row {row['id']}: {e}")
            
            st.cache_data.clear()
            status.update(label="✅ Database Serial Numbers Synced!", state="complete", expanded=False)
            st.rerun()

    # ------------------------------
    # BORROWER MAP
    # ------------------------------
    if not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(borrowers_df["id"], borrowers_df["name"]))
        mapped_names = loans_df["borrower_id"].map(bor_map)
        loans_df["borrower"] = mapped_names.fillna(loans_df["borrower"]).fillna("Unknown")

    if not borrowers_df.empty and "status" in borrowers_df.columns:
        Active_borrowers = borrowers_df[borrowers_df["status"].astype(str).str.upper() == "ACTIVE"]
    else:
        Active_borrowers = pd.DataFrame(columns=["id", "name"])

    # ==============================
    # TABS INTERFACE
    # ==============================
    tab_view, tab_add, tab_manage, tab_actions = st.tabs([
        "📑 Portfolio View", "➕ New Loan", "🛠️ Manage/Edit", "⚙️ Actions"
    ])

    # ==============================
    # TAB VIEW
    # ==============================
    with tab_view:
        search_query = st.text_input("🔍 Search Loan / borrower", key="loan_search_main")
        filtered_loans = loans_df.copy() if not loans_df.empty else pd.DataFrame()

        if not filtered_loans.empty and search_query:
            filtered_loans = filtered_loans[
                filtered_loans.apply(lambda r: search_query.lower() in str(r).lower(), axis=1)
            ]

        if not filtered_loans.empty:
            total_loans = filtered_loans["sn"].nunique()
            original_loans = filtered_loans[filtered_loans["cycle_no"] == 1]  
            total_principal = original_loans["principal"].sum()
            total_repayable = filtered_loans["total_repayable"].sum()
            total_paid = filtered_loans["amount_paid"].sum()
            total_pending = filtered_loans[filtered_loans["status"] == "PENDING"]["total_repayable"].sum()

            col1, col2, col3, col4 = st.columns(4)
            col1.markdown(f'<div style="background: linear-gradient(135deg, #3b82f6, #1e3a8a); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">📄 Total Loans</div><div style="font-size:22px;font-weight:bold;">{total_loans}</div></div>', unsafe_allow_html=True)
            col2.markdown(f'<div style="background: linear-gradient(135deg, #10b981, #065f46); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">💰 Principal</div><div style="font-size:22px;font-weight:bold;">{total_principal:,.0f}</div></div>', unsafe_allow_html=True)
            col3.markdown(f'<div style="background: linear-gradient(135deg, #f59e0b, #92400e); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">💳 Paid</div><div style="font-size:22px;font-weight:bold;">{total_paid:,.0f}</div></div>', unsafe_allow_html=True)
            col4.markdown(f'<div style="background: linear-gradient(135deg, #ef4444, #991b1b); padding:15px; border-radius:10px; color:white; text-align:center;"><div style="font-size:14px;">⏳ Total Pending</div><div style="font-size:22px;font-weight:bold;">{total_pending:,.0f}</div></div>', unsafe_allow_html=True)

            st.markdown("---")

            if "fiscal_year" not in filtered_loans.columns:
                start_dt = pd.to_datetime(filtered_loans.get("start_date"), errors="coerce")
                start_dt = start_dt.fillna(pd.to_datetime(filtered_loans.get("created_at", pd.Timestamp.today())))
                fiscal_years_list = [f"{dt.year}/{dt.year + 1}" if dt.month >= 7 else f"{dt.year - 1}/{dt.year}" for dt in start_dt]
                filtered_loans["fiscal_year"] = fiscal_years_list
            
            fy_unique = sorted(filtered_loans["fiscal_year"].dropna().unique().tolist())
            fy_selected = st.selectbox("Filter by Fiscal Year", ["All"] + fy_unique)
            
            if fy_selected != "All":
                filtered_loans = filtered_loans[filtered_loans["fiscal_year"] == fy_selected]
        
            filtered_loans["balance"] = filtered_loans["total_repayable"] - filtered_loans["amount_paid"]
        
            show_cols = ["sn", "loan_id_label", "borrower", "cycle_no", "principal", "total_repayable", "amount_paid", "balance", "start_date", "end_date", "status"]
            
            def style_entire_row(row):
                val = str(row["status"]).upper().strip()
                color_map = {
                    "ACTIVE": "background-color:#dbeafe;color:#1e40af;font-weight:bold;",
                    "PENDING": "background-color:#fee2e2;color:#991b1b;font-weight:bold;",
                    "CLEARED": "background-color:#d1fae5;color:#065f46;",
                    "BCF": "background-color:#ffedd5;color:#9a3412;",
                    "CLOSED": "background-color:#f3f4f6;color:#374151;"
                }
                return [color_map.get(val, "")] * len(row)
        
            styled_df = (filtered_loans[show_cols].style.apply(style_entire_row, axis=1).format({
                "principal": "{:,.0f}", "amount_paid": "{:,.0f}", "total_repayable": "{:,.0f}", "balance": "{:,.0f}"
            }))
        
            st.dataframe(styled_df, column_order=show_cols, use_container_width=True, hide_index=True)

    # ==============================         
    # TAB ADD LOAN
    # ==============================
    with tab_add:
        if Active_borrowers.empty:
            st.info("💡 Tip: Activate borrower first.")
        else:
            with st.form("loan_issue_form_v2"):
                st.markdown("<h4 style='color:#0A192F;'>📝 Create New Loan Agreement</h4>", unsafe_allow_html=True)
                col1, col2 = st.columns(2)

                borrower_map = dict(zip(Active_borrowers["name"], Active_borrowers["id"]))
                selected_name = col1.selectbox("Select borrower", list(borrower_map.keys()))
                selected_id = str(borrower_map[selected_name]).strip()

                amount = col1.number_input("Principal amount (UGX)", min_value=0, step=50000)
                date_issued = col1.date_input("Start date", value=datetime.now())

                loan_type = col2.selectbox("Loan type", ["Business", "Personal", "Emergency", "Other"])
                interest_rate = col2.number_input("Monthly Interest Rate (%)", min_value=0.0, step=0.5)
                date_due = col2.date_input("Due date", value=date_issued + timedelta(days=30))

                total_due = amount + (amount * interest_rate / 100)
                st.info(f"Preview: Total Repayable {total_due:,.0f} UGX")

                submit = st.form_submit_button("🚀 Confirm & Issue Loan")

                if submit:
                    tenant_id = get_current_tenant()
                    if not tenant_id:
                        st.error("Tenant session missing.")
                        st.stop()
                    if selected_id == "":
                        st.error("borrower ID missing.")
                        st.stop()

                    loan_data = {
                        "id": str(uuid.uuid4()), "sn": "", "loan_id_label": "", "parent_loan_id": None,
                        "borrower_id": selected_id, "borrower": selected_name, "loan_type": loan_type,
                        "principal": float(amount), "interest": float(amount * interest_rate / 100),
                        "total_repayable": float(total_due), "amount_paid": 0.0, "balance": float(total_due),
                        "status": "ACTIVE", "start_date": str(date_issued), "end_date": str(date_due),
                        "cycle_no": 1, "tenant_id": tenant_id
                    }

                    new_loan_df = pd.DataFrame([loan_data])
                    if save_data_saas("loans", new_loan_df):
                        st.success("🎉 Loan Agreement Issued Successfully!")
                        st.cache_data.clear()
                        st.rerun()

    # ==============================
    # TAB ACTIONS
    # ==============================
    with tab_actions:
        st.markdown("<h4 style='color: #0A192F;'>🔄 Multi-Stage Loan Rollover</h4>", unsafe_allow_html=True)
        eligible_loans = loans_df[(~loans_df["status"].isin(["CLEARED"])) & (loans_df["balance"] > 0)]
    
        if eligible_loans.empty:
            st.success("All loans brought up to date! ✨")
        else:
            roll_map = {f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']} • Bal {row['balance']:,.0f}": row["id"] for _, row in eligible_loans.iterrows()}
            roll_sel = st.selectbox("Select Loan to Roll Forward", list(roll_map.keys()))
            parent_id = roll_map[roll_sel]
            loan_to_roll = eligible_loans[eligible_loans["id"] == parent_id].iloc[0]
    
            new_interest_rate = st.number_input("New Monthly Interest (%)", value=3.0, step=0.5)
    
            if st.button("🔥 Execute Next Rollover", use_container_width=True):
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
                    st.success("✅ Loan rolled forward.")
                    st.cache_data.clear()
                    st.rerun()

    # ==============================
# TAB MANAGE (FIXED & SECURE)
# ==============================
with tab_manage:

    # Fix: Ensure you use an alias for the module to avoid variable name shadowing crashes
    import datetime as dt_mod
    from datetime import datetime
    from datetime import timedelta  # Fix: Added missing import

    if not loans_df.empty:

        edit_map = {
            f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']}": row["id"]
            for _, row in loans_df.iterrows()
        }

        selected = st.selectbox(
            "Select Loan to Edit",
            list(edit_map.keys())
        )

        target_id = edit_map[selected]

        loan_match = loans_df[
            loans_df["id"] == target_id
        ]

        if loan_match.empty:
            st.error("Loan not found.")
            st.stop()

        loan_to_edit = loan_match.iloc[0]

        # =====================================
        # FORM
        # =====================================
        # Fix: Dynamic keys based on row values keep form containers isolated during switches
        with st.form(key=f"edit_form_container_{target_id}"):

            e_princ = st.number_input(
                "Principal",
                value=float(loan_to_edit["principal"])
            )

            raw_date = loan_to_edit.get("start_date")

            # Secure conversion using the explicit module alias to protect global scopes
            if isinstance(raw_date, str) and raw_date != "":
                default_date = datetime.strptime(
                    raw_date[:10],
                    "%Y-%m-%d"
                ).date()

            elif hasattr(raw_date, "date"):
                default_date = raw_date.date()

            elif hasattr(raw_date, "strftime"):
                default_date = raw_date

            else:
                default_date = dt_mod.date.today()

            # Fix: Keep input name distinct from any module name ('e_date_val')
            e_date_val = st.date_input(
                "Date",
                value=default_date
            )

            e_interest_rate = st.number_input(
                "Interest Rate (%)",
                value=float(
                    loan_to_edit.get(
                        "interest_rate",
                        loan_to_edit.get("interest", 0.0)
                    )
                ),
                step=0.01
            )

            e_type = st.text_input(
                "Loan Type",
                value=str(
                    loan_to_edit.get("loan_type", "")
                )
            )

            status_options = [
                "ACTIVE",
                "PENDING",
                "CLEARED",
                "BCF",
                "CLOSED"
            ]

            current_stat = str(
                loan_to_edit["status"]
            ).upper().strip()

            idx = (
                status_options.index(current_stat)
                if current_stat in status_options
                else 0
            )

            e_stat = st.selectbox(
                "Status",
                status_options,
                index=idx
            )

            # Fix: Native Streamlit components handle batch validation inside form containers
            save_changes = st.form_submit_button(
                "💾 Save Changes"
            )

        # =====================================
        # SAVE LOGIC
        # =====================================
        if save_changes:

            # Force pure ISO text strings to eliminate JSON Serialization errors
            formatted_start_date = e_date_val.strftime("%Y-%m-%d")
            
            calculated_end_date = (
                dt_mod.datetime.combine(e_date_val, dt_mod.time.min) + timedelta(days=30)
            ).date().strftime("%Y-%m-%d")

            updated_row = {
                "id": target_id,
                "sn": loan_to_edit["sn"],
                "loan_id_label": loan_to_edit["loan_id_label"],
                "parent_loan_id": (
                    loan_to_edit["parent_loan_id"]
                    if pd.notna(loan_to_edit["parent_loan_id"]) and loan_to_edit["parent_loan_id"] != ""
                    else None
                ),
                "borrower_id": loan_to_edit["borrower_id"],
                "borrower": loan_to_edit["borrower"],
                "loan_type": e_type,
                "principal": float(e_princ),
                "interest": float(e_princ * e_interest_rate / 100),
                "total_repayable": float(e_princ + (e_princ * e_interest_rate / 100)),
                "amount_paid": float(loan_to_edit["amount_paid"]),
                "balance": float((e_princ + (e_princ * e_interest_rate / 100)) - loan_to_edit["amount_paid"]),
                "status": e_stat,
                "start_date": formatted_start_date,
                "end_date": calculated_end_date,
                "cycle_no": int(loan_to_edit["cycle_no"]),
                "tenant_id": get_current_tenant()
            }

            # Convert to DataFrame row payload
            payload_df = pd.DataFrame([updated_row])

            # ROUTE VIA CORE ADAPTER: Handles upsert processing and cache invalidation automatically
            success = save_data_saas("loans", payload_df)

            if success:
                st.toast("🎉 Loan configurations updated cleanly!", icon="✅")
                st.rerun()
    
            # =====================================
            # DELETE BUTTON
            # OUTSIDE FORM
            # =====================================
            if st.button(
                "🗑️ Delete Loan Permanently",
                use_container_width=True,
                key=f"delete_{target_id}"
            ):
    
                supabase.table("loans").delete().eq(
                    "id",
                    target_id
                ).execute()
    
                st.warning("Loan Deleted.")
    
                st.cache_data.clear()
    
                st.rerun()
