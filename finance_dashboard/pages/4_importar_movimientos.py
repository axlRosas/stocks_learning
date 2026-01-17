import pandas as pd
import streamlit as st

from lib.db import get_conn, init_db, insert_transaction, load_transactions
from lib.settings import ACCOUNTS, EXTERNAL, TX_TYPES

st.set_page_config(page_title="Importar movimientos", page_icon="📥", layout="wide")
st.title("📥 Importar movimientos (CSV/XLSX)")
st.caption("Sube un archivo y se guarda en SQLite con deduplicación automática.")

conn = get_conn()
init_db(conn)

template = pd.DataFrame(
    [
        {"date": "2026-01-10", "type": "income", "from_account": EXTERNAL, "to_account": "BBVA", "amount": 30000, "description": "Nomina", "category": "Nomina", "merchant": "Empresa"},
        {"date": "2026-01-11", "type": "transfer", "from_account": "BBVA", "to_account": "Efectivo", "amount": 1000, "description": "Retiro efectivo", "category": "", "merchant": ""},
        {"date": "2026-01-12", "type": "expense", "from_account": "Efectivo", "to_account": EXTERNAL, "amount": 150, "description": "Cafe", "category": "Cafe", "merchant": "Cafeteria"},
    ]
)

st.download_button(
    "⬇️ Descargar template CSV",
    template.to_csv(index=False).encode("utf-8"),
    file_name="movimientos_template.csv",
    mime="text/csv",
)

st.markdown("---")
uploaded = st.file_uploader("Sube CSV o XLSX", type=["csv", "xlsx"])

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    req = {"date","type","from_account","to_account","amount","description","category","merchant"}
    missing = req - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas: {sorted(missing)}")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["amount"] = df["amount"].astype(float)

    if not set(df["type"]).issubset(set(TX_TYPES)):
        bad = sorted(set(df["type"]) - set(TX_TYPES))
        raise ValueError(f"type inválido: {bad}")

    valid_accounts = set(ACCOUNTS) | {EXTERNAL}
    bad_from = sorted(set(df["from_account"]) - valid_accounts)
    bad_to = sorted(set(df["to_account"]) - valid_accounts)
    if bad_from or bad_to:
        raise ValueError(f"Cuentas inválidas. from={bad_from} to={bad_to}")

    # Reglas consistentes
    # income: from=EXTERNAL, to=Cuenta
    # expense: from=Cuenta, to=EXTERNAL
    # transfer: from=Cuenta, to=Cuenta
    # (no lo forzamos duro para no bloquear imports; solo warning)
    return df

if uploaded:
    try:
        df_in = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
        df_norm = normalize(df_in)

        st.subheader("Vista previa")
        st.dataframe(df_norm, use_container_width=True, hide_index=True)

        if st.button("✅ Importar a SQLite"):
            inserted = 0
            for _, r in df_norm.iterrows():
                ok = insert_transaction(
                    conn,
                    date=r["date"].isoformat(),
                    tx_type=r["type"],
                    from_account=r["from_account"],
                    to_account=r["to_account"],
                    amount=float(r["amount"]),
                    description=str(r.get("description","") or ""),
                    category=(None if pd.isna(r.get("category")) or str(r.get("category")).strip()=="" else str(r.get("category")).strip()),
                    merchant=(None if pd.isna(r.get("merchant")) or str(r.get("merchant")).strip()=="" else str(r.get("merchant")).strip()),
                    source="csv_import",
                )
                if ok:
                    inserted += 1
            st.success(f"Importados {inserted} (deduplicación activa).")
            st.rerun()

    except Exception as e:
        st.error(str(e))

st.markdown("---")
st.subheader("Últimos movimientos")
df = load_transactions(conn).sort_values(["date","id"], ascending=[False, False]).head(30)
if df.empty:
    st.info("Aún no hay movimientos.")
else:
    st.dataframe(df, use_container_width=True, hide_index=True)
