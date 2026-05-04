# ==============================                           
# 🛡️ 15. COLLATERAL MANAGEMENT PAGE (SAAS + ENTERPRISE UPGRADE)
# ==============================
import mimetypes
from datetime import datetime
import pandas as pd
import streamlit as st
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
                    "Asset Type",
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
    # 📋 TAB 2: INVENTORY & STATUS
    # ==============================
    with tab_view:
        if collateral_df is None or collateral_df.empty:
            st.info("💡 No assets currently in the registry.")
        else:
            # 📊 METRIC DASHBOARD
            collateral_df["value"] = pd.to_numeric(collateral_df["value"], errors="coerce").fillna(0)
            total_value = collateral_df["value"].sum()
            held_count = len(collateral_df[collateral_df["status"] == "In Custody"])
            
            m1, m2 = st.columns(2)
            m1.metric("Total Asset Value (Security)", f"UGX {total_value:,.0f}")
            m2.metric("Items in Custody", held_count)

            st.markdown("### Asset Ledger")
            
            display_df = collateral_df.copy()
            display_df["value_fmt"] = display_df["value"].apply(lambda x: f"{x:,.0f}")
            
            st.dataframe(
                display_df[["date_added", "borrower", "type", "description", "value_fmt", "status"]].rename(
                    columns={"value_fmt": "Value (UGX)", "date_added": "Date Registered"}
                ),
                use_container_width=True,
                hide_index=True
            )

            # ==============================
            # ⚙️ ASSET MANAGEMENT & PHOTO VIEW
            # ==============================
            st.markdown("---")
            with st.expander("🛠️ View Details & Manage Lifecycle", expanded=True):
                # Filter out released assets for the selection list
                manageable = collateral_df.copy()
                
                if manageable.empty:
                    st.write("No assets found.")
                else:
                    # 1. Select the Asset
                    asset_labels = manageable.apply(lambda x: f"{x['borrower']} - {x['description']}", axis=1).tolist()
                    asset_to_manage = st.selectbox("Select Asset to View/Update", options=asset_labels)
                    
                    # 2. Get the specific row for that asset
                    selected_row = manageable[manageable.apply(lambda x: f"{x['borrower']} - {x['description']}", axis=1) == asset_to_manage].iloc[0]
                    asset_id = selected_row["id"]

                    # --- PHOTO EVIDENCE SECTION ---
                    st.markdown("#### 📸 Photo Evidence")
                    
                    # Check if 'photo' or 'image' column exists in your DB
                    # If you saved the photo in the previous step, it's likely stored as a URL or Base64
                    asset_photo = selected_row.get('photo', selected_row.get('image_url', None))

                    if asset_photo:
                        st.image(asset_photo, caption=f"Current Photo of {selected_row['description']}", use_column_width=True)
                    else:
                        st.warning("⚠️ No photo was uploaded for this asset.")
                    
                    st.markdown("---")

                    # 3. Update Status Section
                    st.markdown("#### 🔄 Update Status")
                    col_stat, col_btn = st.columns([2,1])
                    new_stat = col_stat.selectbox(
                        "Set New Status", 
                        ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"],
                        index=["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"].index(selected_row['status']) if selected_row['status'] in ["In Custody", "Released", "Disposed (Auctioned)", "Held for Pickup"] else 0
                    )
                    
                    if col_btn.button("Update Status", use_container_width=True):
                        update_row = pd.DataFrame([{
                            "id": asset_id, 
                            "status": new_stat,
                            "tenant_id": str(current_tenant)
                        }])
                        
                        if save_data_saas("collateral", update_row):
                            st.success("✅ Asset status updated!")
                            st.cache_data.clear()
                            st.rerun()
