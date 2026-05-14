import streamlit as st
import pandas as pd

# Core DB utilities
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas
# ==============================                           
# 🛡️ 15. COLLATERAL MANAGEMENT
# ==============================

def show_collateral():
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    # ==============================
    # 🔐 SAFETY CHECK
    # ==============================
    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    st.markdown(f"<h2 style='color: {brand_color};'>🛡️ Collateral & Security</h2>", unsafe_allow_html=True)

    # ==============================
    # 📦 FETCH DATA
    # ==============================
    collateral_df = get_data("collateral") 
    loans_df = get_data("loans")

    # ==============================
    # 🔍 FILTER ELIGIBLE LOANS
    # ==============================
    if loans_df is not None and not loans_df.empty:
        # Clean column names: remove spaces and make lowercase
        loans_df.columns = [str(c).strip().lower() for c in loans_df.columns]
        
        active_statuses = ["active", "overdue", "pending", "bcf"]
        
        # Safely filter by status if it exists
        if 'status' in loans_df.columns:
            available_loans = loans_df[loans_df["status"].str.lower().isin(active_statuses)].copy()
        else:
            available_loans = loans_df.copy()
    else:
        available_loans = pd.DataFrame()

    # --- TABS ---
    tab_reg, tab_view = st.tabs(["➕ Register Asset", "📋 Inventory & Status"])

    # ==============================
    # ➕ TAB 1: REGISTER ASSET
    # ==============================
    with tab_reg:
        if available_loans.empty:
            st.info("ℹ️ No Active or Overdue loans found to attach collateral to.")
        else:
            # 1. FIND THE BORROWER COLUMN DYNAMICALLY
            all_cols = [str(c).strip().lower() for c in loans_df.columns]
            borrower_col = None
            
            # Look for any column that sounds like 'borrower' or 'client'
            for col in loans_df.columns:
                c_clean = str(col).strip().lower()
                if c_clean in ['borrower', 'client', 'borrower_name', 'client_name']:
                    borrower_col = col
                    break
            
            # Fallback: if no name match, use the 3rd column (index 2) 
            # based on your previous table screenshots
            if borrower_col is None and len(loans_df.columns) >= 3:
                borrower_col = loans_df.columns[2]

            # 2. CREATE MASTER LOOKUP
            if borrower_col:
                name_lookup = dict(zip(loans_df['id'], loans_df[borrower_col]))
            else:
                name_lookup = {}

            with st.form("collateral_reg_form", clear_on_submit=True):
                st.write("### Link Asset to Loan")
                c1, c2 = st.columns(2)

                loan_map = {}
                for _, row in available_loans.iterrows():
                    loan_id = row['id']
                    
                    # Pull name using our dynamic column discovery
                    b_name = name_lookup.get(loan_id, "Unknown")
                    
                    # Double-check for 'nan' or empty strings
                    if str(b_name).lower() in ['nan', 'none', '']:
                        b_name = "Unknown"

                    ref = row.get('loan_id_label', 'N/A')
                    amt = f"UGX {row.get('principal', 0):,.0f}"
                    
                    clean_label = f"{b_name} | {amt} (Ref: {ref})"
                    loan_map[loan_id] = clean_label

                selected_loan_id = c1.selectbox(
                    "Select Loan/Borrower",
                    options=list(loan_map.keys()),
                    format_func=lambda x: loan_map.get(x, "Select Loan")
                )
                # ----------------------------

                asset_type = c2.selectbox(
                    "Asset type",
                    ["Logbook (Car)", "Land Title", "Electronics", "House Deed", "Business Stock", "Other"]
                )

                desc = st.text_input("Detailed Asset Description (e.g. Plate No, Plot No)")
                est_value = st.number_input("Estimated Market Value (UGX)", min_value=0, step=100000)
                
                st.markdown("---")
                uploaded_photo = st.file_uploader("Upload Asset Photo (Verification)", type=["jpg", "png", "jpeg"])

                submit_save = st.form_submit_button("💾 Save & Secure Asset", use_container_width=True)

            if submit_save:
                if not desc or est_value <= 0:
                    st.error("❌ Please provide a description and valid market value.")
                else:
                    try:
                        # Extract just the Name for the record
                        full_label = loan_map[selected_loan_id]
                        borrower_for_db = full_label.split(" | ")[0]

                        new_asset = pd.DataFrame([{
                            "loan_id": selected_loan_id,
                            "tenant_id": str(current_tenant),
                            "borrower": borrower_for_db,
                            "type": asset_type,
                            "description": desc,
                            "value": float(est_value),
                            "status": "In Custody",
                            "date_added": datetime.now().strftime("%Y-%m-%d")
                        }])

                        if save_data_saas("collateral", new_asset):
                            st.success(f"✅ Asset secured for {borrower_for_db}!")
                            st.cache_data.clear()
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Save failed: {e}")

    # ==============================
    # 📋 TAB 2: INVENTORY & STATUS (INTERACTIVE)
    # ==============================
    with tab_view:
    
        if collateral_df is None or collateral_df.empty:
            st.info("💡 No assets currently in the registry.")
    
        else:
    
            # ==============================
            # 📊 METRIC DASHBOARD
            # ==============================
            collateral_df["value"] = pd.to_numeric(collateral_df["value"], errors="coerce").fillna(0)
    
            total_value = collateral_df["value"].sum()
            held_count = len(collateral_df[collateral_df["status"] == "In Custody"])
    
            m1, m2 = st.columns(2)
            m1.metric("Total Asset Value (Security)", f"UGX {total_value:,.0f}")
            m2.metric("Items in Custody", held_count)
    
            st.divider()
    
            # ==============================
            # 🔍 FILTERS (NEW INTERACTIVE LAYER)
            # ==============================
            col1, col2 = st.columns(2)
    
            status_filter = col1.selectbox(
                "Filter by Status",
                ["All"] + sorted(collateral_df["status"].dropna().unique().tolist())
            )
    
            borrower_filter = col2.text_input("Search borrower / description").lower()
    
            df = collateral_df.copy()
    
            # Apply filters
            if status_filter != "All":
                df = df[df["status"] == status_filter]
    
            if borrower_filter:
                df = df[
                    df["borrower"].str.lower().str.contains(borrower_filter, na=False) |
                    df["description"].str.lower().str.contains(borrower_filter, na=False)
                ]
    
            # ==============================
            # 📊 INTERACTIVE TABLE (NO HTML)
            # ==============================
            st.markdown("### Asset Ledger")
    
            display_df = df.copy()
    
            display_df["Value (UGX)"] = display_df["value"].apply(lambda x: f"{x:,.0f}")
            display_df = display_df.rename(columns={
                "date_added": "date Registered",
                "borrower": "Borrower",
                "type": "type",
                "description": "Description",
                "status": "Status"
            })
    
            table_df = display_df[[
                "date Registered",
                "Borrower",
                "type",
                "Description",
                "Value (UGX)",
                "Status"
            ]]
    
            st.dataframe(
                table_df,
                use_container_width=True,
                hide_index=True
            )
    
            st.divider()
    
            # ==============================
            # ⚙️ ASSET MANAGEMENT & PHOTO VIEW
            # ==============================
            st.markdown("### 🛠️ View Details & Manage Lifecycle")
    
            manageable = df.copy()
    
            if manageable.empty:
                st.warning("No assets match your filters.")
            else:
    
                # Better labels (keeps your logic but cleaner UX)
                manageable["label"] = manageable.apply(
                    lambda x: f"{x['borrower']} — {x['description']}", axis=1
                )
    
                selected_label = st.selectbox(
                    "Select Asset",
                    manageable["label"].tolist()
                )
    
                selected_row = manageable[manageable["label"] == selected_label].iloc[0]
                asset_id = selected_row["id"]
    
                # ==============================
                # 📸 PHOTO EVIDENCE
                # ==============================
                st.markdown("#### 📸 Photo Evidence")
    
                asset_photo = selected_row.get("photo", selected_row.get("image_url", None))
    
                if asset_photo:
                    st.image(asset_photo, caption=selected_row["description"], use_container_width=True)
                else:
                    st.info("No photo uploaded for this asset.")
    
                st.divider()
    
                # ==============================
                # 🔄 STATUS UPdate (INTERACTIVE IMPROVED)
                # ==============================
                st.markdown("#### 🔄 Update Status")
    
                status_options = ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"]
    
                col_stat, col_btn = st.columns([3, 1])
    
                new_status = col_stat.selectbox(
                    "Change Status",
                    status_options,
                    index=status_options.index(selected_row["status"])
                    if selected_row["status"] in status_options else 0
                )
    
                if col_btn.button("Update Status", use_container_width=True):
    
                    update_row = pd.DataFrame([{
                        "id": asset_id,
                        "status": new_status,
                        "tenant_id": str(current_tenant)
                    }])
    
                    if save_data_saas("collateral", update_row):
                        st.success("✅ Asset status updated successfully!")
                        st.cache_data.clear()
                        st.rerun()
