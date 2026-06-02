import streamlit as st
import pandas as pd
import numpy as np
import datetime
from supabase import create_client

# -----------------------------
# 🔐 SaaS Tenant Context (UUID Safe)
# -----------------------------
def get_current_tenant():
    """Returns the current tenant ID, halting execution if missing."""
    tenant_id = st.session_state.get("tenant_id")

    if not tenant_id or tenant_id in ["", "default_tenant", None]:
        st.warning("⚠️ Please log in again (tenant missing).")
        st.stop()

    return str(tenant_id).strip()

# -----------------------------
# 🌐 Supabase Initialization
# -----------------------------
@st.cache_resource
def init_supabase():
    try:
        url = st.secrets.get("SUPABASE_URL")
        key = st.secrets.get("SUPABASE_KEY")
        if not url or not key:
            return None
        return create_client(url, key)
    except Exception:
        return None

supabase = init_supabase()
if supabase is None:
    st.warning("⚠️ Supabase not connected (some features may not work)")

# -----------------------------
# 🧠 Core Data Engine (Isolated Cache Keys)
# -----------------------------
@st.cache_data(ttl=600, show_spinner=False)
def get_cached_data(table_name, tenant_id):
    """Fetches data scoped by tenant_id to partition the Streamlit cache."""
    try:
        if supabase is None:
            return pd.DataFrame()
        
        res = supabase.table(table_name).select("*").eq("tenant_id", tenant_id).execute()
        
        if res.data:
            df = pd.DataFrame(res.data)
            df.columns = df.columns.str.strip().str.lower()
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Database Fetch Error [{table_name}]: {e}")
        return pd.DataFrame()

# -----------------------------
# 🧠 Database Adapter (Data Gateway)
# -----------------------------
def get_data(table_name):
    """Public data retrieval function that enforces tenant partitioning."""
    tenant_id = get_current_tenant()
    df = get_cached_data(table_name, tenant_id)

    if df is None or df.empty:
        return pd.DataFrame()

    # Uniform column naming conversion
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Bulletproof fallback: Double-verify tenant isolation locally
    if "tenant_id" in df.columns:
        df["tenant_id"] = df["tenant_id"].astype(str).str.strip()
        df = df[df["tenant_id"] == tenant_id].copy()

    return df.reset_index(drop=True)

# -----------------------------
# 💾 Data Persistence (Multi-Tenant Secure)
# -----------------------------
def serialize_row_data(val):
    """Converts dates, timestamps, and numbers to JSON-serializable types."""
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.strftime("%Y-%m-%d")
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    if pd.isna(val) or val is np.nan:
        return None
    return val

def save_data_saas(table_name, dataframe):
    """Saves multi-tenant safe data and breaks serialization blocks."""
    try:
        if supabase is None:
            st.error("❌ Database not connected")
            return False
        
        if dataframe is None or dataframe.empty:
            st.error("No Data")
            return False
            
        tenant_id = get_current_tenant()
        df_to_save = dataframe.copy()
        
        # Explicit tenant stamping
        df_to_save["tenant_id"] = tenant_id
        
        # Clean data structures to eliminate 'Timestamp is not JSON serializable' errors
        df_to_save = df_to_save.map(serialize_row_data)
        records = df_to_save.to_dict("records")
        
        response = supabase.table(table_name).upsert(records).execute()
        if response.data:
            st.success(f"Saved {len(response.data)} record(s)")
            # Invalidate cache partitions for this table to reflect updates instantly
            st.cache_data.clear()
            return True
        return False
    except Exception as e:
        st.error(f"DB Error [{table_name}]: {e}")
        return False

def delete_data_saas(table_name, row_id):
    """Deletes a record safely ensuring the active tenant owns it."""
    try:
        if supabase is None:
            return False
            
        tenant_id = get_current_tenant()
        
        # Enforces a matching compound filter so tenants cannot drop cross-tenant rows
        response = supabase.table(table_name).delete().match({
            "id": row_id,
            "tenant_id": tenant_id
        }).execute()
        
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Database Error: {e}")
        return False
