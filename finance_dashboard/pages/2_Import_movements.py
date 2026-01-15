from __future__ import annotations

import pandas as pd
import streamlit as st

from lib.db import EXTERNAL, get_conn, init_db, insert_transaction, load_account_config, load_transactions, load_opening_balances

# Ajusta tus cuentas aquí (mismo set que usa Interés ahorros)
ACCOUNTS = ["BBVA", "Nu Turbo", "Nu 7.3", "Openbank"]
DEFAULT_RATES = {"BBVA": 0.0, "Nu Turbo": 0.13, "Nu 7.3": 0.073, "Openbank": 0.10}

st.set_page_config(page_title="Importar movimientos", page_icon="📥", layout="wide")
st.title("📥 Importar movimientos")
st.caption("Carga rápida por CSV/XLSX al dashboard (se guardan en SQLite).")

conn = get_conn()
init_db(conn, accounts=ACCOUNTS, default_rates=DEFAULT_RATES, default_day_basis=360)

st.subheader("1) Descarga el template")
template = pd.DataFrame(
    [
        # Ejemplos:
        {"tx_date": "2026-01-01", "tx_type": "deposit", "from_account": EXTERNAL, "to_account": "BBVA", "amount": 30000, "description": "Nomina"},
        {"tx_date": "2026-01-02", "tx_type": "transfer", "from_account": "BBVA", "to_account": "Nu Turbo", "amount": 5000, "description": "Ahorro"},
        {"tx_date": "2026-01-03", "tx_type": "withdrawal", "from_account": "BBVA", "to_account": EXTERNAL, "amount": 1200, "description": "Supermercado"},
    ]
)

csv_bytes = template.to_csv(index=False).encode("utf-8")
st.download_button("⬇️ Descargar template CSV", data=csv_bytes, file_name="movimientos_template.csv", mime="text/csv")

st.markdown("---")
st.subheader("2) Sube tu archivo")
uploaded = st.file_uploader("CSV o XLSX", type=["csv", "xlsx"])

def validate_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    required = {"tx_date", "tx_type", "from_account", "to_account", "amount", "description"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas: {sorted(missing)}")

    df = df.copy()
    df["tx_date"] = pd.to_datetime(df["tx_date"]).dt.date
    df["amount"] = df["amount"].astype(float)

    valid_types = {"deposit", "withdrawal", "transfer"}
    bad = set(df["tx_type"]) - valid_types
    if bad:
        raise ValueError(f"tx_type inválido: {sorted(bad)} (usa deposit|withdrawal|transfer)")

    valid_accounts = set(ACCOUNTS) | {EXTERNAL}
    bad_from = set(df["from_account"]) - valid_accounts
    bad_to = set(df["to_account"]) - valid_accounts
    if bad_from or bad_to:
        raise ValueError(f"Cuentas inválidas. from: {sorted(bad_from)} | to: {sorted(bad_to)}")

    # Transferencias: from != to
    is_transfer = df["tx_type"] == "transfer"
    if (df.loc[is_transfer, "from_account"] == df.loc[is_transfer, "to_account"]).any():
        raise ValueError("Hay transferencias con from_account == to_account")

    # Normaliza descripción
    df["description"] = df["description"].fillna("").astype(str)
    return df


if uploaded is not None:
    try:
        if uploaded.name.endswith(".csv"):
            df_in = pd.read_csv(uploaded)
        else:
            df_in = pd.read_excel(uploaded)

        df_norm = validate_and_normalize(df_in)

        st.subheader("Vista previa")
        st.dataframe(df_norm, use_container_width=True, hide_index=True)

        # Deduplicación simple (opcional): evita duplicar si se re-importa el mismo archivo
        st.caption("Tip: si re-importas el mismo archivo, marca deduplicación para no duplicar filas.")
        dedup = st.checkbox("Deduplicar (por tx_date, tx_type, from_account, to_account, amount, description)", value=True)

        if st.button("✅ Importar al dashboard"):
            if dedup:
                existing = load_transactions(conn)
                if not existing.empty:
                    key_cols = ["tx_date", "tx_type", "from_account", "to_account", "amount", "description"]
                    existing_keys = set(tuple(existing[c].astype(str).tolist()[i] for c in key_cols) for i in range(len(existing)))
                    keep = []
                    for _, r in df_norm.iterrows():
                        k = tuple(str(r[c]) for c in key_cols)
                        if k not in existing_keys:
                            keep.append(r)
                    df_norm = pd.DataFrame(keep) if keep else df_norm.iloc[0:0]

            for _, r in df_norm.iterrows():
                insert_transaction(
                    conn=conn,
                    tx_date=r["tx_date"].isoformat(),
                    tx_type=r["tx_type"],
                    from_account=r["from_account"],
                    to_account=r["to_account"],
                    amount=float(r["amount"]),
                    description=(r["description"].strip() or None),
                )

            st.success(f"Importados {len(df_norm)} movimientos.")
            st.rerun()

    except Exception as e:
        st.error(f"Error: {e}")

st.markdown("---")
st.subheader("3) Últimos movimientos guardados")
tx = load_transactions(conn).sort_values(["tx_date", "id"], ascending=[False, False]).head(30)
if tx.empty:
    st.info("Aún no hay movimientos.")
else:
    st.dataframe(tx[["id","tx_date","tx_type","from_account","to_account","amount","description"]], use_container_width=True, hide_index=True)
