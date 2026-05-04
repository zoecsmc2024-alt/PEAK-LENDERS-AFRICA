# ==============================
# 💵 19. PETTY CASH MANAGEMENT PAGE
# ==============================
import pandas as pd
import streamlit as st
from datetime import datetime

def show_petty_cash():
    """
    Manages daily office cash transactions with a modern Banking UI.
    Tracks inflows/outflows for specific tenants with real-time balance alerts.
    """
    brand_color = st.session_state.get("theme_color", "#2B3F87")
    current_tenant = st.session_state.get('tenant_id')

    # ==============================
    # 🎨 BANKING UI SYSTEM (ENHANCED)
    # ==============================
    st.markdown(f"""
    <style>
    .block-container {{ padding-top: 1.2rem; }}
    
    /* Glassmorphism Cards */
    .glass-card {{
        backdrop-filter: blur(10px);
        background: linear-gradient(145deg, rgba(255,255,255,0.9), rgba(240,244,255,0.7));
        border-radius: 16px;
        padding: 20px;
        border: 1px solid rgba(43,63,135,0.1);
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
        transition: transform 0.2s ease;
    }}
    .glass-card:hover {{ transform: translateY(-3px); box-shadow: 0 8px 25px rgba(0,0,0,0.08); }}

    .metric-title {{ font-size: 11px; color: #6b7280; font-weight: 600; letter-spacing: 0.8px; text-transform: uppercase; }}
    .metric-value {{ font-size: 24px; font-weight: 700; margin-top: 4px; }}
    
    /* status Badges */
    .status-badge {{ font-size: 10px; padding: 3px 10px; border-radius: 12px; font-weight: 700; float: right; }}
    .badge-safe {{ background: #E1F9F0; color: #10B981; }}
    .badge-low {{ background: #FFEBEB; color: #FF4B4B; }}
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f"<h2 style='color:{brand_color};'>💵 Petty Cash Management</h2>", unsafe_allow_html=True)

    # ==============================
    # 📦 1. DATA ADAPTER & ISOLATION
    # ==============================
    df = get_cached_data("petty_cash")

    if df is None or df.empty:
        df = pd.DataFrame(columns=["id", "type", "amount", "date", "description", "tenant_id"])
    else:
        # Enforce multi-tenancy
        df = df[df["tenant_id"].astype(str) == str(current_tenant)].copy()
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    # ==============================
    # 📈 2. LIQUIDITY CALCULATIONS
    # ==============================
    inflow = df[df["type"] == "In"]["amount"].sum()
    outflow = df[df["type"] == "Out"]["amount"].sum()
    balance = inflow - outflow

    # Threshold for "Low balance" warning
    LOW_CASH_THRESHOLD = 50000
    bal_status = "SAFE" if balance >= LOW_CASH_THRESHOLD else "LOW"
    status_class = "badge-safe" if balance >= LOW_CASH_THRESHOLD else "badge-low"
    bal_color = "#10B981" if balance >= LOW_CASH_THRESHOLD else "#FF4B4B"

    # ==============================
    # 💎 KPI DASHBOARD
    # ==============================
    c1, c2, c3 = st.columns(3)

    c1.markdown(f"""<div class="glass-card"><div class="metric-title">Total Cash In</div>
        <div class="metric-value" style="color:#10B981;">{inflow:,.0f} <span style="font-size:12px;">UGX</span></div></div>""", unsafe_allow_html=True)

    c2.markdown(f"""<div class="glass-card"><div class="metric-title">Total Cash Out</div>
        <div class="metric-value" style="color:#FF4B4B;">{outflow:,.0f} <span style="font-size:12px;">UGX</span></div></div>""", unsafe_allow_html=True)

    c3.markdown(f"""<div class="glass-card">
        <div class="metric-title">Current balance <span class="status-badge {status_class}">{bal_status}</span></div>
        <div class="metric-value" style="color:{bal_color};">{balance:,.0f} <span style="font-size:12px;">UGX</span></div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ==============================
    # 📋 TABS: ACTION & LOG
    # ==============================
    tab_record, tab_history = st.tabs(["➕ Record Transaction", "📜 Digital Cashbook"])

    # --- TAB 1: RECORD ENTRY ---
    with tab_record:
        with st.form("petty_cash_form", clear_on_submit=True):
            st.write("### Log Cash Movement")
            col_a, col_b = st.columns(2)
            ttype = col_a.selectbox("Transaction type", ["Out", "In"], help="'In' for top-ups, 'Out' for expenses")
            t_amount = col_b.number_input("Amount (UGX)", min_value=0, step=500)
            desc = st.text_input("Purpose / Description", placeholder="e.g., Office Internet bundle, Cleaning supplies")

            if st.form_submit_button("💾 Commit to Cashbook", use_container_width=True):
                if t_amount > 0 and desc:
                    new_row = pd.DataFrame([{
                        "id": str(uuid.uuid4()) if 'uuid' in globals() else datetime.now().strftime("%Y%m%d%H%M%S"),
                        "type": ttype,
                        "amount": float(t_amount),
                        "date": datetime.now().strftime("%Y-%m-%d"),
                        "description": desc,
                        "tenant_id": str(current_tenant)
                    }])
                    
                    # Merge with existing for the save_data function
                    if save_data("petty_cash", pd.concat([df, new_row], ignore_index=True)):
                        st.success(f"✅ Recorded {t_amount:,.0f} UGX {ttype}flow")
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.error("⚠️ Please provide a valid amount and description.")

    # --- TAB 2: TRANSACTION HISTORY ---
    with tab_history:
        if df.empty:
            st.info("ℹ️ No cash transactions recorded yet.")
        else:
            st.markdown("### 📜 Transaction Log")
            
            # Format the dataframe for professional display
            display_df = df.sort_values("date", ascending=False).copy()
            
            # Stylized display using st.dataframe
            st.dataframe(
                display_df[["date", "type", "description", "amount"]].rename(
                    columns={"date": "Date", "type": "type", "description": "Details", "amount": "Amount (UGX)"}
                ),
                use_container_width=True,
                hide_index=True
            )

            # ==============================
            # ⚙️ ADVANCED MANAGEMENT (CRUD)
            # ==============================
            with st.expander("🛠️ Correct or Remove Entry"):
                # Use a specific list for the selectbox to prevent index errors
                entry_list = display_df.apply(lambda r: f"{r['date']} | {r['type']} | {r['description'][:20]}... | {r['amount']:,.0f}", axis=1).tolist()
                selected_label = st.selectbox("Select Entry to Modify", options=entry_list)
                
                # Get the original record
                selected_idx = entry_list.index(selected_label)
                original_record = display_df.iloc[selected_idx]
                entry_id = original_record["id"]

                c_edit, c_del = st.columns(2)
                
                # We use a sub-form for the edit to keep state clean
                with st.popover("📝 Edit Record Details"):
                    new_desc = st.text_input("Edit Description", value=original_record["description"])
                    new_amt = st.number_input("Edit Amount", value=float(original_record["amount"]))
                    if st.button("Save Changes"):
                        df.loc[df["id"] == entry_id, ["description", "amount"]] = [new_desc, new_amt]
                        if save_data("petty_cash", df):
                            st.success("Entry Updated")
                            st.cache_data.clear()
                            st.rerun()

                if c_del.button("🗑️ Delete Permanently", use_container_width=True, type="secondary"):
                    # Filter out the deleted ID
                    df_filtered = df[df["id"] != entry_id]
                    if save_data("petty_cash", df_filtered):
                        st.warning("Entry removed from digital cashbook.")
                        st.cache_data.clear()
                        st.rerun()
                
