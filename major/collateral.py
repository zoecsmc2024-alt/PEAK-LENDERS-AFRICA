import streamlit as st
import pandas as pd
import os
from datetime import datetime
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas

# ==========================================
# 🛡️ COLLATERAL & SECURITY MANAGEMENT MODULE
# ==========================================
def show_collateral():
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    # ==========================================
    # 🔐 TENANT VALIDATION ENGINE
    # ==========================================
    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    st.markdown(f"<h2 style='color: {brand_color};'>🛡️ Collateral & Security</h2>", unsafe_allow_html=True)

    # ==========================================
    # 📦 CACHE RETRIEVAL & NORMALIZATION LAYER
    # ==========================================
    collateral_raw = get_cached_data("collateral") 
    loans_raw = get_cached_data("loans")

    collateral_df = pd.DataFrame(collateral_raw).copy() if collateral_raw is not None else pd.DataFrame()
    loans_df = pd.DataFrame(loans_raw).copy() if loans_raw is not None else pd.DataFrame()

    # Clean data structure schemas to uniform snake_case
    for target_df in [collateral_df, loans_df]:
        if not target_df.empty:
            target_df.columns = [str(c).strip().lower().replace(" ", "_") for c in target_df.columns]

    # ==========================================
    # 🔍 FILTER MATRIX FOR ELIGIBLE ACTIVE LOANS
    # ==========================================
    if not loans_df.empty:
        active_statuses = ["active", "overdue", "pending", "bcf"]
        if 'status' in loans_df.columns:
            # Enforce clean lowercase matches for accurate filtering
            loans_df['status_clean'] = loans_df['status'].astype(str).str.strip().str.lower()
            available_loans = loans_df[loans_df["status_clean"].isin(active_statuses)].copy()
        else:
            available_loans = loans_df.copy()
    else:
        available_loans = pd.DataFrame()

    # App Tabs Layout Presentation Layer
    tab_reg, tab_view = st.tabs(["➕ Register Asset", "📋 Inventory & Status"])

    # ==========================================
    # ➕ TAB 1: REGISTER NEW ASSET RISK MANAGEMENT
    # ==========================================
    with tab_reg:
        if available_loans.empty:
            st.info("ℹ️ No Active, Pending, or Overdue loans found to attach collateral items to.")
        else:
            # Dynamic client mapping schema discoverer
            borrower_col = None
            for col in loans_df.columns:
                if col in ['borrower', 'client', 'borrower_name', 'client_name']:
                    borrower_col = col
                    break
            
            # Fallback index mapping parameter safety assignment
            if borrower_col is None and len(loans_df.columns) >= 3:
                borrower_col = loans_df.columns[2]

            name_lookup = dict(zip(loans_df['id'].astype(str), loans_df[borrower_col])) if borrower_col and 'id' in loans_df.columns else {}

            with st.form("collateral_reg_form", clear_on_submit=True):
                st.write("### Link Asset to Loan Reference")
                c1, c2 = st.columns(2)

                loan_map = {}
                for _, row in available_loans.iterrows():
                    loan_id = str(row['id'])
                    b_name = name_lookup.get(loan_id, "Unknown")
                    
                    if str(b_name).lower() in ['nan', 'none', '']:
                        b_name = "Unknown"

                    ref = row.get('loan_id_label', 'N/A')
                    amt = f"UGX {pd.to_numeric(row.get('principal', 0), errors='coerce'):,.0f}"
                    
                    loan_map[loan_id] = f"{b_name} | {amt} (Ref: {ref})"

                selected_loan_id = c1.selectbox(
                    "Select Target Loan Portfolio",
                    options=list(loan_map.keys()),
                    format_func=lambda x: loan_map.get(x, "Select Loan Account")
                )

                asset_type = c2.selectbox(
                    "Collateral Category Asset Type",
                    ["Logbook (Car)", "Land Title", "Electronics", "House Deed", "Business Stock", "Other"]
                )

                desc = st.text_input("Detailed Asset Asset Description (e.g., Engine No, Plot No, Asset SN)")
                est_value = st.number_input("Estimated Current Market Value (UGX)", min_value=0, step=500000)
                
                st.markdown("---")
                uploaded_photo = st.file_uploader("Upload Verification Asset Photo Evidence", type=["jpg", "png", "jpeg"])

                submit_save = st.form_submit_button("💾 Save & Secure Asset In Registry", use_container_width=True)

            if submit_save:
                if not desc or est_value <= 0:
                    st.error("❌ Process Halting: Please provide a descriptive asset entry and valid positive valuation.")
                else:
                    try:
                        full_label = loan_map[selected_loan_id]
                        borrower_for_db = full_label.split(" | ")[0]
                        photo_url = None

                        # ==========================================
                        # 📸 BINARY FILE STORAGE UPLOAD PIPELINE
                        # ==========================================
                        if uploaded_photo is not None:
                            file_ext = os.path.splitext(uploaded_photo.name)[1]
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            secure_filename = f"{current_tenant}_{selected_loan_id}_{timestamp}{file_ext}"
                            
                            # Execute targeted raw byte bucket upload stream pipeline
                            file_bytes = uploaded_photo.read()
                            upload_res = supabase.storage.from_("collateral").upload(
                                path=secure_filename,
                                file=file_bytes,
                                file_options={"content-type": uploaded_photo.type}
                            )
                            
                            # Construct public access path location payload key
                            photo_url = supabase.storage.from_("collateral").get_public_url(secure_filename)

                        new_asset = pd.DataFrame([{
                            "loan_id": selected_loan_id,
                            "tenant_id": str(current_tenant),
                            "borrower": borrower_for_db,
                            "type": asset_type,
                            "description": desc,
                            "value": float(est_value),
                            "status": "In Custody",
                            "image_url": photo_url,
                            "date_added": datetime.now().strftime("%Y-%m-%d")
                        }])

                        if save_data_saas("collateral", new_asset):
                            st.success(f"✅ Risk Mitigated: Asset successfully secured for {borrower_for_db}!")
                            st.cache_data.clear()
                            st.rerun()
                    except Exception as e:
                        st.error(f"❌ Storage Pipeline or Database Mutation Interrupted: {e}")

    # ==========================================
    # 📋 TAB 2: INVENTORY RISK REGISTRY LEDGER
    # ==========================================
    with tab_view:
        if collateral_df.empty:
            st.info("💡 No assets found inside this tenant's historical collateral risk registry registry matrix.")
        else:
            # Asset value normalizer
            collateral_df["value"] = pd.to_numeric(collateral_df["value"], errors="coerce").fillna(0.0)
            
            total_value = collateral_df["value"].sum()
            held_count = len(collateral_df[collateral_df["status"].astype(str).str.strip().lower() == "in custody"])

            # Matrix KPI Presentation Display Layer
            m1, m2 = st.columns(2)
            m1.metric("Total Asset Value Under Management", f"UGX {total_value:,.0f}")
            m2.metric("Active Assets Safely In Custody", held_count)
            st.divider()

            # Dynamic Context Filters
            col1, col2 = st.columns(2)
            status_filter = col1.selectbox(
                "Filter Ledger Status",
                ["All"] + sorted(collateral_df["status"].dropna().unique().tolist())
            )
            borrower_filter = col2.text_input("Search Borrower Name / Description Attribute").strip().lower()

            df_filtered = collateral_df.copy()

            if status_filter != "All":
                df_filtered = df_filtered[df_filtered["status"] == status_filter]

            if borrower_filter:
                df_filtered = df_filtered[
                    df_filtered["borrower"].astype(str).str.lower().str.contains(borrower_filter, na=False) |
                    df_filtered["description"].astype(str).str.lower().str.contains(borrower_filter, na=False)
                ]

            st.markdown("### Active Asset Risk Ledger Registry")
            
            display_df = df_filtered.copy()
            if not display_df.empty:
                display_df["Value (UGX)"] = display_df["value"].apply(lambda x: f"UGX {x:,.0f}")
                
                # Dynamic mapping interface configurations
                rename_schema = {
                    "date_added": "Date Registered",
                    "borrower": "Borrower Name",
                    "type": "Asset Category Type",
                    "description": "Asset Specifications",
                    "status": "Current Status Status"
                }
                
                # Build safe column mapping definitions match array 
                for old_col, new_col in rename_schema.items():
                    if old_col in display_df.columns:
                        display_df = display_df.rename(columns={old_col: new_col})

                cols_to_render = [c for c in ["Date Registered", "Borrower Name", "Asset Category Type", "Asset Specifications", "Value (UGX)", "Current Status Status"] if c in display_df.columns]
                
                st.dataframe(
                    display_df[cols_to_render],
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.warning("No record items available inside parameters matched by operational filtering filters.")

            st.divider()

            # ==========================================
            # ⚙️ ASSET RISK LIFECYCLE MANAGEMENT MUTATION
            # ==========================================
            st.markdown("### 🛠️ View Details & Operational Lifecycle Management")
            
            if df_filtered.empty:
                st.warning("Select items from active filtering array matrix parameters above to handle operations processing.")
            else:
                # Absolute data primary key mapping array to safely lock mutations context
                df_filtered["ui_label"] = df_filtered.apply(lambda x: f"{x.get('borrower', 'N/A')} — {x.get('description', 'N/A')} (ID: {x.get('id', 'N/A')})", axis=1)
                
                selected_label = st.selectbox(
                    "Choose Selected Target Collateral Asset File Record",
                    options=df_filtered["ui_label"].tolist()
                )

                selected_row = df_filtered[df_filtered["ui_label"] == selected_label].iloc[0]
                asset_id = selected_row["id"]

                # Media render verification flow layout channel
                st.markdown("#### 📸 Stored Asset Photo Verification Evidence Verification")
                asset_photo = selected_row.get("image_url", selected_row.get("photo", None))
                
                if asset_photo and str(asset_photo).strip().lower() not in ['nan', 'none', '']:
                    st.image(asset_photo, caption=f"Verification File Check Reference: {selected_row['description']}", use_container_width=True)
                else:
                    st.info("ℹ️ Database Check Complete: No physical photo attachment uploaded for this ledger registry file row.")

                st.divider()

                # Operational status lifecycle handling state mutations
                st.markdown("#### 🔄 Asset Lifecycle State Transition Status")
                status_options = ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"]
                current_saved_status = str(selected_row.get("status", "In Custody")).strip()

                col_stat, col_btn = st.columns([3, 1])
                
                new_status = col_stat.selectbox(
                    "Transition Asset Operations Management Lifecycle State",
                    options=status_options,
                    index=status_options.index(current_saved_status) if current_saved_status in status_options else 0
                )

                if col_btn.button("Update Status", use_container_width=True):
                    # Structured payload build array tracking primary key explicitly 
                    update_payload = pd.DataFrame([{
                        "id": asset_id,
                        "status": new_status,
                        "tenant_id": str(current_tenant)
                    }])

                    if save_data_saas("collateral", update_payload):
                        st.success(f"✅ Asset record entry updated safely to: '{new_status}' State.")
                        st.cache_data.clear()
                        st.rerun()
