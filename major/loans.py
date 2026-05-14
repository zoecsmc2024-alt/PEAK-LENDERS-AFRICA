import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
# Core DB utilities
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas

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


# ==============================
# 13. LOANS MANAGEMENT PAGE
# ==============================
def show_loans():

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
            "id",
            "sn",
            "loan_id_label",
            "parent_loan_id",
            "borrower_id",
            "borrower",
            "loan_type",
            "principal",
            "interest",
            "total_repayable",
            "amount_paid",
            "balance",
            "status",
            "start_date",
            "end_date",
            "cycle_no",
            "tenant_id"
        ])

    if borrowers_df is None:
        borrowers_df = pd.DataFrame()

    if payments_df is None:
        payments_df = pd.DataFrame()

    # ------------------------------
    # REQUIRED DEFAULTS
    # ------------------------------
    required_defaults = {
        "id": "",
        "sn": "",
        "loan_id_label": "",
        "parent_loan_id": "",
        "borrower_id": "",
        "borrower": "",
        "loan_type": "",
        "principal": 0.0,
        "interest": 0.0,
        "total_repayable": 0.0,
        "amount_paid": 0.0,
        "balance": 0.0,
        "status": "ACTIVE",
        "start_date": "",
        "end_date": "",
        "cycle_no": 1,
        "tenant_id": ""
    }

    for col, val in required_defaults.items():
        if col not in loans_df.columns:
            loans_df[col] = val

    loans_df = loans_df.copy()
    payments_df = payments_df.copy()

    # ------------------------------
    # type CLEANUP
    # ------------------------------
    loans_df["id"] = loans_df["id"].astype(str)
    loans_df["borrower_id"] = loans_df["borrower_id"].astype(str)
    loans_df["parent_loan_id"] = loans_df["parent_loan_id"].fillna("").astype(str)

    if not payments_df.empty and "loan_id" in payments_df.columns:
        payments_df["loan_id"] = payments_df["loan_id"].astype(str)

    # ------------------------------
    # NUMERIC CLEANUP
    # ------------------------------
    for col in [
        "principal",
        "interest",
        "total_repayable",
        "amount_paid",
        "balance"
    ]:
        loans_df[col] = pd.to_numeric(loans_df[col], errors="coerce").fillna(0)

    if not payments_df.empty and "amount" in payments_df.columns:
        payments_df["amount"] = pd.to_numeric(
            payments_df["amount"], errors="coerce"
        ).fillna(0)

    # ------------------------------
    # date CLEANUP
    # ------------------------------
    for col in ["start_date", "end_date"]:
        loans_df[col] = pd.to_datetime(loans_df[col], errors="coerce")

    # ------------------------------
    # PAYMENT SYNC
    # ------------------------------
    loans_df["amount_paid"] = 0  # ✅ ensure column always exists

    if not payments_df.empty and "loan_id" in payments_df.columns:
        pay_sums = payments_df.groupby("loan_id")["amount"].sum()
        loans_df["amount_paid"] = loans_df["id"].map(pay_sums).fillna(0)

    loans_df["balance"] = (
        loans_df["total_repayable"] - loans_df["amount_paid"]
    ).clip(lower=0)

    # ==============================
    # SERIAL ENGINE (MOVE UP)
    # ==============================
    existing_nums = []
    
    # ------------------------------
    # 🔒 NORMALIZE EXISTING SNs
    # ------------------------------
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip()
    
    # Map existing SNs (immutability protection)
    existing_sn_map = dict(zip(loans_df["id"], loans_df["sn"]))
    
    for val in loans_df["sn"]:
        val = val.strip()
        if val.startswith("LN-"):
            try:
                existing_nums.append(int(val.replace("LN-", "")))
            except:
                pass
    
    next_sn_val = max(existing_nums, default=0)
    
    # ------------------------------
    # 🔁 SAFE SN ASSIGNMENT
    # ------------------------------
    for i in loans_df.index:
    
        current_id = loans_df.at[i, "id"]
    
        # 🔒 NEVER touch already valid SN
        existing_sn = str(existing_sn_map.get(current_id, "")).strip()
        if existing_sn.startswith("LN-"):
            continue
    
        parent_id = str(loans_df.at[i, "parent_loan_id"]).strip()
    
        inherited_sn = ""
    
        # 🔗 WALK FULL LINEAGE (not just direct parent)
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
    
        # ✅ APPLY INHERITED SN
        if inherited_sn:
            loans_df.at[i, "sn"] = inherited_sn
    
        # 🆕 CREATE NEW SN ONLY IF STILL MISSING
        if not str(loans_df.at[i, "sn"]).startswith("LN-"):
            next_sn_val += 1
            loans_df.at[i, "sn"] = f"LN-{next_sn_val:04d}"
    
    # ✅ SORT BEFORE ASSIGNING CYCLES (Ensures Parent is Cycle 1)
    loans_df = loans_df.sort_values(by=["sn", "start_date", "id"])
    
    loans_df["cycle_no"] = (
        loans_df.groupby("sn").cumcount() + 1
    )
    
    # ------------------------------
    # REVISED SMART STATUS LOGIC (V2)
    # ------------------------------
    
    loans_df["sn"] = loans_df["sn"].astype(str).str.strip().str.upper()
    
    # 1. Sort to ensure chronological order
    loans_df = loans_df.sort_values(by=["sn", "cycle_no", "start_date"])
    
    # 2. Process each loan family
    for sn_val, grp in loans_df.groupby("sn"):
        
        indices = grp.index.tolist()
        latest_idx = indices[-1]
        
        # 3. Mark all rows EXCEPT the last one as BCF
        # (Because a newer cycle exists, these are inherently "Brought Forward")
        if len(indices) > 1:
            loans_df.loc[indices[:-1], "status"] = "BCF"
        
        # 4. Handle the Latest Cycle
        latest_row = loans_df.loc[latest_idx]
        
        # Check if balance is effectively zero (handling float rounding)
        if abs(latest_row["balance"]) < 1.0: 
            loans_df.at[latest_idx, "status"] = "CLEARED"
        else:
            # If there's a balance, determine if it's a fresh loan or a rollover
            if int(latest_row["cycle_no"]) == 1:
                loans_df.at[latest_idx, "status"] = "ACTIVE"
            else:
                loans_df.at[latest_idx, "status"] = "PENDING"
    
    # ------------------------------
    # FINAL SAFETY OVERRIDE
    # ------------------------------
    # If ANY row (even a middle one) has 0 balance, it cannot be PENDING or ACTIVE.
    # It's either BCF (if a newer cycle exists) or CLEARED.
    # This rule forces any 0 balance "Pending" rows to "Cleared".
    
    mask_zero = (loans_df["balance"] <= 0) & (loans_df["status"] != "BCF")
    loans_df.loc[mask_zero, "status"] = "CLEARED"
    
    # ------------------------------
    # FINAL RULE: FORCE CLEARED STATE
    # ------------------------------
    # Any loan with balance = 0 is ALWAYS CLEARED
    loans_df.loc[
        loans_df["balance"] <= 0,
        "status"
    ] = "CLEARED"
    
    # ------------------------------
    # FINAL SORT
    # ------------------------------
    loans_df = loans_df.sort_values(
        by=["sn", "cycle_no"],
        ascending=[True, True]
    ).reset_index(drop=True)
    
    # ------------------------------
    # LABELS
    # ------------------------------
    loans_df["loan_id_label"] = (
        loans_df["sn"]
        .str.replace("LN-", "", regex=False)
        .str.zfill(4)
    )
    

    # ==============================
    # 🔄 DATABASE SYNC ENGINE (OPTIMIZED)
    # ==============================
    # 1. Fetch the raw data from cache to see what's actually in the DB right now
    raw_db_df = get_cached_data("loans")
    
    # 2. Identify only rows where our calculated SN/Cycle differs from the DB
    def needs_update(row):
        db_match = raw_db_df[raw_db_df["id"] == row["id"]]
        if db_match.empty:
            return True
        db_row = db_match.iloc[0]
        # Only sync if SN, Label, or Cycle has changed or is missing in DB
        return (str(db_row.get("sn", "")) != str(row["sn"]) or 
                str(db_row.get("loan_id_label", "")) != str(row["loan_id_label"]) or
                int(db_row.get("cycle_no", 0)) != int(row["cycle_no"]))

    # 3. Filter to_sync to only include actual changes
    to_sync = loans_df[loans_df.apply(needs_update, axis=1)]
    
    if not to_sync.empty:
        # Use st.status for a cleaner look than st.spinner
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
            
            # Clear cache so the next run sees the updated data
            st.cache_data.clear()
            status.update(label="✅ Database Serial Numbers Synced!", state="complete", expanded=False)
            st.rerun()
    # ------------------------------
    # borrower MAP
    # ------------------------------
    if not borrowers_df.empty:
        borrowers_df["id"] = borrowers_df["id"].astype(str)
        bor_map = dict(zip(
            borrowers_df["id"],
            borrowers_df["name"]
        ))
        mapped_names = loans_df["borrower_id"].map(bor_map)

        loans_df["borrower"] = mapped_names.fillna(loans_df["borrower"]).fillna("Unknown")

    # ------------------------------
    # ACTIVE borrowerS
    # ------------------------------
    if not borrowers_df.empty and "status" in borrowers_df.columns:
        Active_borrowers = borrowers_df[
            borrowers_df["status"]
            .astype(str)
            .str.upper() == "ACTIVE"
        ]
    else:
        Active_borrowers = pd.DataFrame(columns=["id", "name"])

    # ==============================
    # TABS
    # ==============================
    tab_view, tab_add, tab_manage, tab_actions = st.tabs([
        "📑 Portfolio View",
        "➕ New Loan",
        "🛠️ Manage/Edit",
        "⚙️ Actions"
    ])

    # ==============================
    # TAB VIEW
    # ==============================
    with tab_view:
        search_query = st.text_input(
            "🔍 Search Loan / borrower",
            key="loan_search_main"
        )

        # Create a local copy for filtering
        filtered_loans = loans_df.copy() if not loans_df.empty else pd.DataFrame()

        if not filtered_loans.empty and search_query:
            filtered_loans = filtered_loans[
                filtered_loans.apply(
                    lambda r: search_query.lower() in str(r).lower(),
                    axis=1
                )
            ]
        # ------------------------------
        # 📊 PORTFOLIO METRICS
        # ------------------------------
        if not filtered_loans.empty:
            # Basic Metrics
            total_loans = filtered_loans["sn"].nunique()
            original_loans = filtered_loans[filtered_loans["cycle_no"] == 1]  
            total_principal = original_loans["principal"].sum()
            total_paid = filtered_loans["amount_paid"].sum()
            
            # --- FIX: CALCULATION FOR REMAINING BALANCE ---
            # 1. Filter for Pending and Active loans
            active_pending_df = filtered_loans[filtered_loans["status"].isin(["PENDING", "ACTIVE"])].copy()
        
            # 2. Ensure numeric types to avoid calculation errors
            total_rep = pd.to_numeric(active_pending_df["total_repayable"], errors="coerce").fillna(0)
            paid_so_far = pd.to_numeric(active_pending_df["amount_paid"], errors="coerce").fillna(0)
        
            # 3. Calculate True Pending (Remaining Balance)
            # This prevents early payments from being double-counted in 'Paid' and 'Active'
            total_pending = (total_rep - paid_so_far).sum()
        
            col1, col2, col3, col4 = st.columns(4)

            col1.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #3b82f6, #1e3a8a);
                padding:15px;
                border-radius:10px;
                color:white;
                text-align:center;">
                <div style="font-size:14px;">📄 Total Loans</div>
                <div style="font-size:22px;font-weight:bold;">{total_loans}</div>
            </div>
            """, unsafe_allow_html=True)
            
            col2.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #10b981, #065f46);
                padding:15px;
                border-radius:10px;
                color:white;
                text-align:center;">
                <div style="font-size:14px;">💰 Principal</div>
                <div style="font-size:22px;font-weight:bold;">{total_principal:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)
            
            col3.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #f59e0b, #92400e);
                padding:15px;
                border-radius:10px;
                color:white;
                text-align:center;">
                <div style="font-size:14px;">💳 Paid</div>
                <div style="font-size:22px;font-weight:bold;">{total_paid:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)

            # --- UPDATED METRIC CARD ---
            col4.markdown(f"""
            <div style="
                background: linear-gradient(135deg, #ef4444, #991b1b);
                padding:15px;
                border-radius:10px;
                color:white;
                text-align:center;">
                <div style="font-size:14px;">⏳ Active & Pending</div>
                <div style="font-size:22px;font-weight:bold;">{total_pending:,.0f}</div>
            </div>
            """, unsafe_allow_html=True)

            st.markdown("---")
        # ------------------------------
        # 📋 LOAN DATA TABLE
        # ------------------------------
        if filtered_loans.empty:
            st.warning("No matching loans found.")
        else:
            # ------------------------------
            # 🎯 Fiscal Year Engine (July–June FIXED)
            # ------------------------------
            if "fiscal_year" not in filtered_loans.columns:
                # Convert start_date to datetime
                start_dt = pd.to_datetime(filtered_loans.get("start_date"), errors="coerce")
                
                # Fill missing with created_at or today
                start_dt = start_dt.fillna(pd.to_datetime(filtered_loans.get("created_at", pd.Timestamp.today())))
                
                # Compute fiscal year (July–June)
                fiscal_years_list = []
                for dt in start_dt:
                    if dt.month >= 7:
                        fiscal_years_list.append(f"{dt.year}/{dt.year + 1}")
                    else:
                        fiscal_years_list.append(f"{dt.year - 1}/{dt.year}")
                
                filtered_loans["fiscal_year"] = fiscal_years_list
            
            # Build dropdown
            fy_unique = sorted(filtered_loans["fiscal_year"].dropna().unique().tolist())
            fy_selected = st.selectbox("Filter by Fiscal Year", ["All"] + fy_unique)
            
            # Filter loans if a specific FY is selected
            if fy_selected != "All":
                filtered_loans = filtered_loans[filtered_loans["fiscal_year"] == fy_selected]
        
            # Calculation update (Ensuring amount_paid reduces total_repayable)
            filtered_loans["balance"] = filtered_loans["total_repayable"] - filtered_loans["amount_paid"]
        
            show_cols = [
                "sn",
                "loan_id_label",
                "borrower",
                "cycle_no",
                "principal",
                "total_repayable",
                "amount_paid",
                "balance",
                "start_date",
                "end_date",
                "status"
            ]
            def style_entire_row(row):
                val = str(row["status"]).upper().strip()
                color_map = {
                    "ACTIVE": "background-color:#dbeafe;color:#1e40af;font-weight:bold;",
                    "PENDING": "background-color:#fee2e2;color:#991b1b;font-weight:bold;",
                    "CLEARED": "background-color:#d1fae5;color:#065f46;",
                    "BCF": "background-color:#ffedd5;color:#9a3412;",
                    "CLOSED": "background-color:#f3f4f6;color:#374151;"
                }
                style = color_map.get(val, "")
                return [style] * len(row)
        
            # Apply styling and currency formatting
            styled_df = (
                filtered_loans[show_cols].style
                .apply(style_entire_row, axis=1)
                .format({
                    "principal": "{:,.0f}",
                    "amount_paid": "{:,.0f}",
                    "total_repayable": "{:,.0f}",
                    "balance": "{:,.0f}"
                })
            )
        
            st.dataframe(
                styled_df,
                column_order=show_cols,
                use_container_width=True,
                hide_index=True
            )
            
    # ==============================       
    # TAB ADD LOAN
    # ==============================
    with tab_add:

        if Active_borrowers.empty:

            st.info("💡 Tip: Activate borrower first.")

        else:

            with st.form("loan_issue_form_v2"):

                st.markdown(
                    "<h4 style='color:#0A192F;'>📝 Create New Loan Agreement</h4>",
                    unsafe_allow_html=True
                )

                col1, col2 = st.columns(2)

                borrower_map = dict(
                    zip(
                        Active_borrowers["name"],
                        Active_borrowers["id"]
                    )
                )

                selected_name = col1.selectbox(
                    "Select borrower",
                    list(borrower_map.keys())
                )

                selected_id = str(
                    borrower_map[selected_name]
                ).strip()

                amount = col1.number_input(
                    "Principal amount (UGX)",
                    min_value=0,
                    step=50000
                )

                date_issued = col1.date_input(
                    "Start date",
                    value=datetime.now()
                )

                loan_type = col2.selectbox(
                    "Loan type",
                    ["Business", "Personal", "Emergency", "Other"]
                )

                interest_rate = col2.number_input(
                    "Monthly Interest Rate (%)",
                    min_value=0.0,
                    step=0.5
                )

                date_due = col2.date_input(
                    "Due date",
                    value=date_issued + timedelta(days=30)
                )

                total_due = amount + (
                    amount * interest_rate / 100
                )

                st.info(
                    f"Preview: Total Repayable {total_due:,.0f} UGX"
                )

                submit = st.form_submit_button(
                    "🚀 Confirm & Issue Loan"
                )

                if submit:

                    tenant_id = get_current_tenant()

                    if not tenant_id:
                        st.error("Tenant session missing.")
                        st.stop()

                    if selected_id == "":
                        st.error("borrower ID missing.")
                        st.stop()

                    loan_data = {
                        "id": str(uuid.uuid4()),
                        "sn": "",
                        "loan_id_label": "",
                        "parent_loan_id": None,
                        "borrower_id": selected_id,
                        "borrower": selected_name,
                        "loan_type": loan_type,
                        "principal": float(amount),
                        "interest": float(
                            amount * interest_rate / 100
                        ),
                        "total_repayable": float(total_due),
                        "amount_paid": 0.0,
                        "balance": float(total_due),
                        "status": "ACTIVE",
                        "start_date": str(date_issued),
                        "end_date": str(date_due),
                        "cycle_no": 1,
                        "tenant_id": tenant_id
                    }

                    if save_data(
                        "loans",
                        pd.DataFrame([loan_data])
                    ):
                        st.success("✅ Loan issued.")
                        st.cache_data.clear()
                        st.session_state.pop("loans", None)
                        st.rerun()
    # ==============================
    # TAB ACTIONS
    # ==============================
    with tab_actions:
    
        st.markdown(
            "<h4 style='color: #0A192F;'>🔄 Multi-Stage Loan Rollover</h4>",
            unsafe_allow_html=True
        )
    
        eligible_loans = loans_df[
            (~loans_df["status"].isin(["CLEARED"])) &
            (loans_df["balance"] > 0)
        ]
    
        if eligible_loans.empty:
            st.success("All loans brought up to date! ✨")
    
        else:
            roll_map = {
                f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']} • Bal {row['balance']:,.0f}":
                row["id"]
                for _, row in eligible_loans.iterrows()
            }
    
            roll_sel = st.selectbox(
                "Select Loan to Roll Forward",
                list(roll_map.keys())
            )
    
            parent_id = roll_map[roll_sel]
    
            loan_to_roll = eligible_loans[
                eligible_loans["id"] == parent_id
            ].iloc[0]
    
            new_interest_rate = st.number_input(
                "New Monthly Interest (%)",
                value=3.0,
                step=0.5
            )
    
            if st.button(
                "🔥 Execute Next Rollover",
                use_container_width=True
            ):
    
                old_due = pd.to_datetime(
                    loan_to_roll["end_date"],
                    errors="coerce"
                )
    
                if pd.isna(old_due):
                    old_due = datetime.now()
    
                new_start = old_due
                new_due = old_due + timedelta(days=30)
    
                # --- Corrected Indentation for Status Check ---
                current_status = str(
                    loan_to_roll["status"]
                ).strip().upper()
    
                # Only pending loans become BCF when pushed forward
                if current_status == "PENDING":
                    loans_df.loc[
                        loans_df["id"] == parent_id,
                        "status"
                    ] = "BCF"
    
                save_data_saas("loans", loans_df)
                # ----------------------------------------------
    
                # This 'unpaid' value is (Old Total Repayable - Old amount Paid)
                unpaid = float(
                    loan_to_roll["balance"]
                )
    
                new_interest = unpaid * (
                    new_interest_rate / 100
                )
    
                new_row = {
                    "id": str(uuid.uuid4()),
                    "sn": "",  # Handled by your serial engine
                    "loan_id_label": "",
                    "parent_loan_id": parent_id,
                    "borrower_id": loan_to_roll["borrower_id"],
                    "loan_type": loan_to_roll["loan_type"],
                    "principal": unpaid, # ✅ New Principal is Old Principal + Old Interest - Payments
                    "interest": new_interest,
                    "total_repayable": unpaid + new_interest,
                    "amount_paid": 0.0,
                    "balance": unpaid + new_interest,
                    "status": "PENDING",
                    "start_date": str(new_start.date()),
                    "end_date": str(new_due.date()),
                    "cycle_no": int(
                        loan_to_roll["cycle_no"]
                    ) + 1,
                    "tenant_id": get_current_tenant()
                }
    
                if save_data(
                    "loans",
                    pd.DataFrame([new_row])
                ):
                    st.success("✅ Loan rolled forward.")
                    st.cache_data.clear()
                    st.rerun()

    # ==============================
    # TAB MANAGE
    # ==============================
    with tab_manage:

        if not loans_df.empty:

            edit_map = {
                f"{row['borrower']} • {row['loan_id_label']} • Cycle {row['cycle_no']}":
                row["id"]
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

            with st.form(f"edit_form_{target_id}"):

                e_princ = st.number_input(
                    "Principal",
                    value=float(
                        loan_to_edit["principal"]
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
                ).upper()

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

                if st.form_submit_button(
                    "💾 Save Changes"
                ):

                    supabase.table("loans").update({
                        "principal": e_princ,
                        "status": e_stat
                    }).eq(
                        "id",
                        target_id
                    ).execute()

                    st.success("✅ Updated!")
                    st.cache_data.clear()
                    st.rerun()

            if st.button(
                "🗑️ Delete Loan Permanently",
                use_container_width=True
            ):

                supabase.table("loans").delete().eq(
                    "id",
                    target_id
                ).execute()

                st.warning("Loan Deleted.")
                st.cache_data.clear()
                st.rerun()
