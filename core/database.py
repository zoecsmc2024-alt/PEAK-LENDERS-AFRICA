import streamlit as st
import pandas as pd
from supabase import create_client
# ==============================
# ⚡ CORE DATA ENGINE
# ==============================
@st.cache_data(ttl=600, show_spinner=False)
def get_cached_data(table_name):
    try:
        if supabase is None:
            return pd.DataFrame()

        require_tenant()
        tenant_id = get_tenant_id()

        res = supabase.table(table_name)\
            .select("*")\
            .eq("tenant_id", tenant_id)\
            .execute()

        if res.data:
            df = pd.DataFrame(res.data)
            df.columns = df.columns.str.strip().str.lower()
            return df

        return pd.DataFrame()

    except Exception as e:
        st.error(f"Database Fetch Error [{table_name}]: {e}")
        return pd.DataFrame()


# ==============================
# 🔌 2. SUPABASE INIT (SAFE GLOBAL)
# ==============================

# FIND THIS SECTION IN YOUR database.py
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

# THIS LINE IS THE KEY: It makes 'supabase' available to other files
supabase = init_supabase()

# Global Instance
supabase = init_supabase()

if supabase is None:
    st.warning("⚠️ Supabase not connected (some features may not work)")


# ==============================
# 💾 3. DATA PERSISTENCE
# ==============================

def save_data(table_name, dataframe):
    try:
        if supabase is None:
            st.error("❌ Database not connected")
            return False

        require_tenant()

        if dataframe is None or dataframe.empty:
            st.error("No Data")
            return False

        df_to_save = dataframe.copy()
        df_to_save["tenant_id"] = get_tenant_id()

        records = df_to_save.replace({np.nan: None}).to_dict("records")

        response = supabase.table(table_name).upsert(records).execute()

        if response.data:
            st.success(f"Saved {len(response.data)} record(s)")
            return True

        return False

    except Exception as e:
        st.error(f"DB Error [{table_name}]: {e}")
        return False


def delete_data_saas(table_name, filters):
    """
    Deletes a record from Supabase based on a filter (e.g., payroll_id).
    """
    try:
        # Assuming 'supabase' is your initialized client in database.py
        response = supabase.table(table_name).delete().match(filters).execute()
        return True
    except Exception as e:
        st.error(f"Database Error: {e}")
        return False
