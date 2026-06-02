import streamlit as st
import pandas as pd
import uuid
import plotly.express as px
from datetime import datetime

# Core DB utilities
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas
st.write("CURRENT TENANT:", current_tenant)

debug = supabase.table("expenses").select("*").execute()

st.write("ROWS IN SUPABASE:", len(debug.data))

if len(debug.data):
    temp = pd.DataFrame(debug.data)

    st.write("TENANTS IN TABLE:")
    st.write(temp["tenant_id"].unique())
# ==========================================
# 📁 EXPENSE MANAGEMENT MODULE
# ==========================================
def show_expenses():
    # ==============================
    # 🎨 1. MASTER BUTTON STYLING (GLOBAL OVERRIDE)
    # ==============================
    st.markdown("""
    <style>
    /* MASTER BUTTON SELECTOR - Uniform look across the entire application workspace */
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
        box-shadow: 0 4px 14px rgba(30, 58, 138, 0.2) !important;
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

    # ==============================
    # 🎨 BRANDING & THEME
    # ==============================
    brand_color = st.session_state.get("theme_color", "#1E3A8A")
    st.markdown(f"<h2 style='color:{brand_color};'>📉 Operating Expenses</h2>", unsafe_allow_html=True)

    current_tenant = st.session_state.get('tenant_id')
    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    # Dynamic July-to-June financial year logic handler
    def get_fy_label(date_val):
        try:
            dt = pd.to_datetime(date_val)
            return f"FY{dt.year}-{dt.year+1}" if dt.month >= 7 else f"FY{dt.year-1}-{dt.year}"
        except:
            return "Unknown FY"

    # ==========================================
    # 📦 CACHE RETRIEVAL & DATA NORMALIZATION
    # ==========================================
    try:
        # FIXED: Added current_tenant as required by your utility function signature
        raw_expenses = get_cached_data("expenses", current_tenant)
    
        # FIXED: Handles raw data cleanly whether it returns a DataFrame or a list/None
        if isinstance(raw_expenses, pd.DataFrame):
            df = raw_expenses.copy()
        else:
            df = pd.DataFrame(raw_expenses or [])
    
        if not df.empty:
            df.columns = (
                df.columns
                .str.lower()
                .str.strip()
                .str.replace(" ", "_")
            )
    
            # Kept the background processing logic intact, removed the noisy st.write UI elements
            if "tenant_id" in df.columns:
                df = df[
                    df["tenant_id"].astype(str)
                    == str(current_tenant)
                ]
            
            df["id"] = df["id"].astype(str)
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
            date_col = "payment_date" if "payment_date" in df.columns else "date"
            df["financial_year"] = df[date_col].apply(get_fy_label)
        else:
            df = pd.DataFrame(columns=[
                "id", "category", "amount", "date", "description",
                "payment_date", "receipt_no", "tenant_id", "financial_year"
            ])
            
    except Exception as e:
        st.error(f"Expense Load Error: {e}")
        df = pd.DataFrame(columns=[
            "id", "category", "amount", "date", "description",
            "payment_date", "receipt_no", "tenant_id", "financial_year"
        ])
    EXPENSE_CATS = [
        "Rent", "Insurance", "Utilities", "Salaries", "Licence Expenses", 
        "Marketing", "Office Expenses", "Operating Expenses", 
        "Fuel and Motor Vehicle", "Taxes", "Corporate Social Responsibilities", "Other"
    ]

    tab_add, tab_view, tab_manage = st.tabs([
        "➕ Record Expense", "📊 Spending Analysis", "⚙️ Manage Records"
    ])

    # ==========================================
    # ➕ TAB 1: RECORD NEW ENTRY PIPELINE
    # ==========================================
    with tab_add:
        with st.form("add_expense_form", clear_on_submit=True):
            st.write("### Log Operational Cost")
            c1, c2 = st.columns(2)

            category = c1.selectbox("Category", EXPENSE_CATS)
            amount = c2.number_input("Amount (UGX)", min_value=0, step=5000)
            desc = st.text_input("Description Details")

            c3, c4 = st.columns(2)
            p_date = c3.date_input("Payment Date", value=datetime.now())
            receipt_no = c4.text_input("Receipt # / Reference")

            # Applied HTML context layout pattern to make this form submit button look unified
            submit_lbl = '🚀 Save Operational Expense'
            submit_add = st.form_submit_button(submit_lbl, use_container_width=True)

            if submit_add:
                if amount > 0 and desc:
                    try:
                        formatted_date = p_date.strftime("%Y-%m-%d")

                        # Isolate single dictionary payload context row
                        new_row_df = pd.DataFrame([{
                            "id": str(uuid.uuid4()),
                            "category": category,
                            "amount": float(amount),
                            "date": formatted_date,
                            "description": desc,
                            "payment_date": formatted_date,
                            "receipt_no": receipt_no,
                            "tenant_id": str(current_tenant)
                        }])

                        # Ship isolated payload delta entry to table context directly
                        if save_data_saas("expenses", new_row_df):
                            st.success(f"✅ Transaction Secure: Expense tracked safely for {formatted_date}")
                            st.cache_data.clear()
                            st.rerun()
                    except Exception as e:
                        st.error(f"🚨 Mutation Pipeline Failure: {e}")
                else:
                    st.warning("⚠️ Access Rejected: Input values must contain a descriptive phrase and an amount > 0.")

    # ==========================================
    # 📊 TAB 2: SPENDING ANALYSIS LEDGER
    # ==========================================
    with tab_view:
        if df.empty:
            st.info("💡 Portfolio clear. No operational expenses logged yet.")
        else:
            fys = sorted(df["financial_year"].dropna().unique().tolist(), reverse=True)
            fy = st.selectbox("📅 Select Target View Window", ["All Time"] + fys)

            view_df = df if fy == "All Time" else df[df["financial_year"] == fy]
            total_outflow = view_df["amount"].sum()

            # 🎨 Visual KPI Card Presentation Layer
            st.markdown(f"""
                <div style="background-color:#fff; padding:20px; border-radius:12px;
                border-left:6px solid #FF4B4B; box-shadow: 0px 4px 12px rgba(0,0,0,0.05);">
                    <p style="margin:0; font-size:12px; color:#666; font-weight:bold; letter-spacing: 0.5px;">
                        TOTAL RETRIEVED OPERATIONAL OUTFLOW ({str(fy).upper()})
                    </p>
                    <h2 style="margin:5px 0 0 0; color:#FF4B4B; font-weight:700;">
                        UGX {total_outflow:,.0f}
                    </h2>
                </div><br>
            """, unsafe_allow_html=True)

            col_chart, col_summary = st.columns([2, 1])

            with col_chart:
                chart_data = view_df.groupby("category")["amount"].sum().reset_index()
                fig = px.pie(
                    chart_data,
                    names="category", 
                    values="amount",
                    hole=0.45,
                    title="Outflow Diversification Allocation Matrix",
                    color_discrete_sequence=px.colors.qualitative.Safe
                )
                fig.update_layout(margin=dict(t=40, b=20, l=20, r=20))
                st.plotly_chart(fig, use_container_width=True)

            with col_summary:
                st.write("#### Historical Aggregates")
                summary_grouped = df.groupby("financial_year")["amount"].sum().reset_index()
                summary_grouped.columns = ["Financial Year", "Total (UGX)"]
                summary_grouped["Total (UGX)"] = summary_grouped["Total (UGX)"].apply(lambda x: f"{x:,.0f}")
                st.dataframe(summary_grouped, hide_index=True, use_container_width=True)

            st.markdown("### 📋 Filtered Operational Expense Ledger")
            
            # Interactive filtering matrix interfaces
            col_f1, col_f2 = st.columns(2)
            available_cats = ["All"] + sorted(view_df["category"].dropna().unique().tolist())
            selected_cat = col_f1.selectbox("Isolate Category Class", available_cats)

            min_val = float(view_df["amount"].min()) if not view_df.empty else 0.0
            max_val = float(view_df["amount"].max()) if not view_df.empty else 100000.0
            if min_val == max_val:
                max_val += 1.0

            slider_bounds = col_f2.slider(
                "Isolate Transacted Range (UGX)",
                min_value=min_val,
                max_value=max_val,
                value=(min_val, max_val)
            )

            # Apply layout processing parameters 
            ledger_display_df = view_df.copy()
            if selected_cat != "All":
                ledger_display_df = ledger_display_df[ledger_display_df["category"] == selected_cat]

            ledger_display_df = ledger_display_df[
                (ledger_display_df["amount"] >= slider_bounds[0]) & 
                (ledger_display_df["amount"] <= slider_bounds[1])
            ]

            if ledger_display_df.empty:
                st.warning("No recorded expense line items found matching those metrics parameters.")
            else:
                ledger_display_df = ledger_display_df.sort_values("payment_date", ascending=False)
                
                # Format visual frame payload
                render_frame = ledger_display_df[["payment_date", "category", "description", "amount", "receipt_no"]].copy()
                render_frame.columns = ["Transaction Date", "Category Type", "Specification Details", "Amount (UGX)", "Receipt Reference #"]
                
                st.dataframe(
                    render_frame,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Amount (UGX)": st.column_config.NumberColumn("Amount (UGX)", format="%,d")
                    }
                )

    # ==========================================
    # ⚙️ TAB 3: MAINTENANCE LAYER (NO HTML)
    # ==========================================
    with tab_manage:
    
        st.markdown("### 🛠️ Record Maintenance Engine")
    
        if df.empty:
    
            st.info(
                "No logs currently tracked inside database system storage array profiles."
            )
    
        else:
            
            # Structural form context loop scope safe configuration
            @st.dialog("⚠️ Confirm Permanent Deletion")
            def confirm_delete_dialog(tid):
                st.warning(
                    "This action will permanently remove "
                    "the selected transaction record."
                )
                
                c1, c2 = st.columns(2)
                
                # CANCEL
                with c1:
                    if st.button(
                        "❌ Cancel",
                        use_container_width=True
                    ):
                        st.session_state["confirm_delete_expense"] = False
                        st.rerun()
                        
                # CONFIRM DELETE
                with c2:
                    if st.button(
                        "🔥 Yes, Delete",
                        use_container_width=True,
                        type="primary"
                    ):
                        if delete_data_saas(
                            "expenses",
                            tid
                        ):
                            st.session_state["confirm_delete_expense"] = False
                            st.warning(
                                "🗑️ Entry purged permanently "
                                "from cloud storage arrays."
                            )
                            st.cache_data.clear()
                            st.rerun()
    
            # ----------------------------------
            # BUILD SELECT LABELS
            # ----------------------------------
            df["selector_label"] = df.apply(
                lambda r:
                f"{r.get('payment_date', 'N/A')} | "
                f"{r.get('category', 'N/A')} | "
                f"UGX {r['amount']:,.0f} | "
                f"[ID: {str(r['id'])[:8]}]",
                axis=1
            )
    
            # ----------------------------------
            # LOOKUP MAP
            # ----------------------------------
            record_lookup_matrix = {
                row["selector_label"]: row.to_dict()
                for _, row in df.iterrows()
            }
    
            selected_target_label = st.selectbox(
                "Choose Target Entry for Adjustment Profile",
                list(record_lookup_matrix.keys())
            )
    
            # ----------------------------------
            # TARGET RECORD
            # ----------------------------------
            if selected_target_label:
    
                target_record = record_lookup_matrix[selected_target_label]
    
                target_id = target_record["id"]
    
                show_delete_modal = False
    
                # ==================================
                # EDIT FORM
                # ==================================
                with st.form(f"edit_form_container_{target_id}"):
    
                    st.write(
                        f"#### Modify Transaction Profile: {str(target_id)[:8]}"
                    )
    
                    adjusted_amount = st.number_input(
                        "Update Transacted Amount (UGX)",
                        value=float(target_record["amount"]),
                        min_value=0.0,
                        step=5000.0
                    )
    
                    adjusted_desc = st.text_input(
                        "Update Description Specs",
                        value=str(target_record["description"])
                    )
    
                    st.write("")
    
                    col_m1, col_m2 = st.columns(2)
    
                    # ----------------------------------
                    # BUTTONS
                    # ----------------------------------
                    action_save = col_m1.form_submit_button(
                        "💾 Save Structural Corrections",
                        use_container_width=True,
                        type="primary"
                    )
    
                    action_delete = col_m2.form_submit_button(
                        "🗑️ Purge Record Safely",
                        use_container_width=True
                    )
    
                    # ==================================
                    # SAVE EVENT
                    # ==================================
                    if action_save:
    
                        if adjusted_amount > 0 and adjusted_desc.strip():
    
                            mutation_payload = pd.DataFrame([{
                                "id": str(target_id),
                                "category": target_record.get("category"),
                                "amount": float(adjusted_amount),
                                "date": target_record.get("date"),
                                "description": adjusted_desc.strip(),
                                "payment_date": target_record.get("payment_date"),
                                "receipt_no": target_record.get("receipt_no"),
                                "tenant_id": str(current_tenant)
                            }])
    
                            if save_data_saas(
                                "expenses",
                                mutation_payload
                            ):
    
                                st.success(
                                    "✅ Database Sync Complete: "
                                    "Adjustment tracked successfully!"
                                )
    
                                st.cache_data.clear()
                                st.rerun()
    
                        else:
    
                            st.error(
                                "❌ Action Interrupted: "
                                "Data fields cannot be empty."
                            )
    
                    # ==================================
                    # DELETE EVENT
                    # ==================================
                    if action_delete:
                        show_delete_modal = True
                        st.session_state["confirm_delete_expense"] = True
    
                # ==================================
                # DELETE CONFIRMATION POPUP
                # ==================================
                if show_delete_modal or st.session_state.get("confirm_delete_expense", False):
                    confirm_delete_dialog(target_id)
