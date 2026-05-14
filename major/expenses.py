import streamlit as st
import pandas as pd
import uuid
import plotly.express as px
from datetime import datetime
import streamlit as st
import pandas as pd

# Core DB utilities
from core.database import supabase, get_cached_data, save_data_saas, delete_data_saas
def show_expenses():
    st.markdown("<h2 style='color: #2B3F87;'>📁 Expense Management</h2>", unsafe_allow_html=True)

    current_tenant = st.session_state.get('tenant_id')
    if not current_tenant:
        st.error("🔐 Session expired. Please log in.")
        st.stop()

    def get_fy_label(date_val):
        try:
            dt = pd.to_datetime(date_val)
            return f"FY{dt.year}-{dt.year+1}" if dt.month >= 7 else f"FY{dt.year-1}-{dt.year}"
        except:
            return "Unknown FY"

    # ==============================
    # DATA
    # ==============================
    try:
        df = get_cached_data("expenses")
    except:
        df = pd.DataFrame()

    if df is not None and not df.empty:
        df.columns = df.columns.str.lower().str.strip()

        if "tenant_id" in df.columns:
            df = df[df["tenant_id"].astype(str) == str(current_tenant)].copy()

        df["id"] = df["id"].astype(str)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
        df["financial_year"] = df["payment_date"].apply(get_fy_label)
    else:
        df = pd.DataFrame(columns=[
            "id","category","amount","date","description",
            "payment_date","receipt_no","tenant_id","financial_year"
        ])

    EXPENSE_CATS = ["Rent","Insurance","Utilities","Salaries","Licence Expenses","Marketing","Office Expenses","Operating Expenses","Fuel and Motor Vehicle","Taxes","Corporate Social Responsibilities","Other"]

    tab_add, tab_view, tab_manage = st.tabs([
        "➕ Record Expense","📊 Spending Analysis","⚙️ Manage Records"
    ])

    # ==============================
    # ADD
    # ==============================
    with tab_add:
        with st.form("add_expense_form", clear_on_submit=True):
            st.write("### Log Operational Cost")
            c1, c2 = st.columns(2)

            category = c1.selectbox("category", EXPENSE_CATS)
            amount = c2.number_input("amount (UGX)", min_value=0, step=1000)
            desc = st.text_input("Description")

            c3, c4 = st.columns(2)
            p_date = c3.date_input("Payment date", value=datetime.now())
            receipt_no = c4.text_input("Receipt #")

            if st.form_submit_button("🚀 Save Expense", use_container_width=True):
                if amount > 0 and desc:
                    try:
                        d = p_date.strftime("%Y-%m-%d")

                        new = pd.DataFrame([{
                            "id": str(uuid.uuid4()),
                            "category": category,
                            "amount": float(amount),
                            "date": d,
                            "description": desc,
                            "payment_date": d,
                            "receipt_no": receipt_no,
                            "tenant_id": str(current_tenant)
                        }])

                        save_df = pd.concat([df, new], ignore_index=True)\
                            .drop(columns=["financial_year"], errors="ignore")

                        if save_data("expenses", save_df):
                            st.success(f"✅ Expense saved for {d}")
                            st.cache_data.clear()
                            st.rerun()

                    except Exception as e:
                        st.error(f"🚨 {e}")
                else:
                    st.warning("⚠️ Fill all required fields")

    # ==============================
    # VIEW
    # ==============================
    with tab_view:
        if df.empty:
            st.info("💡 No expenses recorded yet.")
        else:
            fys = sorted(df["financial_year"].unique(), reverse=True)
            fy = st.selectbox("📅 Financial Year", ["All Time"] + fys)

            view_df = df if fy == "All Time" else df[df["financial_year"] == fy]

            total = view_df["amount"].sum()

            # 🎨 COLORED CARD
            st.markdown(f"""
                <div style="background-color:#fff;padding:20px;border-radius:15px;
                border-left:6px solid #FF4B4B;box-shadow:2px 2px 10px rgba(0,0,0,0.05);">
                    <p style="margin:0;font-size:12px;color:#666;font-weight:bold;">
                        TOTAL OUTFLOW ({fy})
                    </p>
                    <h2 style="margin:0;color:#FF4B4B;">
                        UGX {total:,.0f}
                    </h2>
                </div><br>
            """, unsafe_allow_html=True)

            col1, col2 = st.columns([2,1])

            with col1:
                fig = px.pie(
                    view_df.groupby("category")["amount"].sum().reset_index(),
                    names="category", values="amount",
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.write("#### FY Summary")
                st.dataframe(
                    df.groupby("financial_year")["amount"].sum().reset_index(),
                    hide_index=True
                )

            # 🎨 STYLED LEDGER

            # 4. Detailed Ledger
            st.markdown("### 📋 Expense Ledger")
            
            ledger_df = view_df.sort_values("payment_date", ascending=False).copy()
            
            try:
                # --- Clean data ---
                ledger_df["payment_date"] = pd.to_datetime(ledger_df["payment_date"], errors="coerce")
                ledger_df["amount"] = pd.to_numeric(ledger_df["amount"], errors="coerce").fillna(0)
            
                display_ledger = ledger_df[[
                    "payment_date",
                    "category",
                    "description",
                    "amount",
                    "receipt_no"
                ]].copy()
            
                display_ledger.columns = [
                    "date",
                    "category",
                    "Description",
                    "amount (UGX)",
                    "Ref #"
                ]
            
                # --- Format date ---
                display_ledger["date"] = display_ledger["date"].dt.strftime("%Y-%m-%d")
            
                # --- FORMAT commas (IMPORTANT) ---
                display_ledger["amount (UGX)"] = display_ledger["amount (UGX)"].apply(
                    lambda x: f"{x:,.0f}"
                )
            
                # --- Filters ---
                col1, col2 = st.columns(2)
            
                categories = ["All"] + sorted(display_ledger["category"].dropna().unique().tolist())
                selected_cat = col1.selectbox("Filter category", categories)
            
                # convert back to numeric for filtering (because we formatted strings)
                ledger_df["amount_num"] = pd.to_numeric(ledger_df["amount"], errors="coerce").fillna(0)
            
                min_amt, max_amt = col2.slider(
                    "amount Range",
                    float(ledger_df["amount_num"].min()),
                    float(ledger_df["amount_num"].max()),
                    (float(ledger_df["amount_num"].min()), float(ledger_df["amount_num"].max()))
                )
            
                # --- Apply filters ---
                if selected_cat != "All":
                    filtered = ledger_df[ledger_df["category"].fillna("General") == selected_cat]
                else:
                    filtered = ledger_df.copy()
                
                # --- amount filter ---
                filtered = filtered[
                    (filtered["amount_num"] >= min_amt) &
                    (filtered["amount_num"] <= max_amt)
                ]
            
                # --- Rebuild display after filtering ---
                final_display_df = filtered[[
                    "payment_date",
                    "category",
                    "description",
                    "amount",
                    "receipt_no"
                ]].copy()
                
                final_display_df.columns = [
                    "date",
                    "category",
                    "Description",
                    "amount (UGX)",
                    "Ref #"
                ]
                
                final_display_df["date"] = pd.to_datetime(
                    final_display_df["date"],
                    errors="coerce"
                ).dt.strftime("%Y-%m-%d")
                
                final_display_df["amount (UGX)"] = final_display_df["amount (UGX)"].apply(
                    lambda x: f"{float(x):,.0f}"
                )
                
                def color_amount(val):
                    return "color: #D32F2F; font-weight: 700;"
                
                styled = final_display_df.style.map(
                    color_amount,
                    subset=["amount (UGX)"]
                )
                
                st.dataframe(
                    styled,
                    use_container_width=True,
                    hide_index=True
                )
            
            except Exception as e:
                st.error(f"Ledger error: {e}")
    # --- TAB 3: MANAGE (CRUD) ---
    with tab_manage:
        st.markdown("### 🛠️ Record Maintenance")

        if df.empty:
            st.info("No expense records available to modify.")
        else:
            df["id"] = df["id"].astype(str)
            # Create identifiable label for selection
            df["selector_label"] = df.apply(
                lambda r: f"{r['payment_date']} | {r['category']} | UGX {r['amount']:,.0f}", axis=1
            )

            record_map = {row["selector_label"]: row for _, row in df.iterrows()}
            selected_label = st.selectbox("Select Record to Edit/Delete", list(record_map.keys()))

            if selected_label:
                target_record = record_map[selected_label]
                
                with st.form("edit_expense_form"):
                    new_amt = st.number_input("Update amount (UGX)", value=float(target_record['amount']))
                    new_desc = st.text_input("Update Description", value=target_record['description'])
                    
                    c1, c2 = st.columns(2)
                    save_btn = c1.form_submit_button("💾 Save Changes", use_container_width=True)
                    delete_btn = c2.form_submit_button("🗑️ Delete Record", use_container_width=True)

                    if save_btn:
                        df.loc[df["id"] == target_record["id"], ["amount", "description"]] = [new_amt, new_desc]
                        if save_data("expenses", df.drop(columns=['selector_label'])):
                            st.success("✅ Record updated!")
                            st.cache_data.clear()
                            st.rerun()

                    if delete_btn:
                        full_df = get_cached_data("expenses")  # reload FULL dataset
                    
                        full_df["id"] = full_df["id"].astype(str)
                    
                        updated_df = full_df[full_df["id"] != str(target_record["id"])]
                    
                        if save_data("expenses", updated_df):
                            st.warning("🗑️ Record deleted.")
                            st.cache_data.clear()
                            st.rerun()
