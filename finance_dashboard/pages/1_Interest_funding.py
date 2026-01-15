from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
from app import ACCOUNTS


from lib import db as db_conn
from lib.ledger import AccountParams, compute_daily_ledger
from lib.formats import mxn


# =============================
# Config
# =============================

EXTERNAL = "EXTERNAL"  # depósito/retiro hacia fuera (nómina, gastos, etc.)

DEFAULT_RATES = {
    "BBVA": 0.00,
    "Nu Turbo": 0.13,
    "Nu 7.3": 0.073,
    "Openbank": 0.10,
}
DEFAULT_DAY_BASIS = 360



# =============================
# UI
# =============================
st.set_page_config(page_title="Interés ahorros", page_icon="📈", layout="wide")

conn = db_conn.get_conn()
db_conn.init_db(conn,ACCOUNTS,DEFAULT_RATES,DEFAULT_DAY_BASIS)

st.title("📈 Interés ahorros")
st.caption("Saldos reales con movimientos + interés diario (Nu/Openbank). BBVA sin interés.")

cfg_df = db_conn.load_account_config(conn)
open_df = db_conn.load_opening_balances(conn)
tx_df = db_conn.load_transactions(conn)

# ---- Sidebar: config
with st.sidebar:
    st.subheader("⚙️ Configuración")

    # Rates + basis (editable)
    cfg_map = {r["account"]: (float(r["annual_rate"]), int(r["day_basis"])) for _, r in cfg_df.iterrows()}

    with st.form("config_form"):
        st.markdown("**Tasas anuales (%):**")
        nu_turbo_rate = st.number_input("Nu Turbo (%)", min_value=0.0, value=cfg_map["Nu Turbo"][0] * 100, step=0.1)
        nu_73_rate = st.number_input("Nu 7.3 (%)", min_value=0.0, value=cfg_map["Nu 7.3"][0] * 100, step=0.1)
        ob_rate = st.number_input("Openbank (%)", min_value=0.0, value=cfg_map["Openbank"][0] * 100, step=0.1)

        st.markdown("**Base de días (cálculo diario):**")
        day_basis = st.selectbox("Base", [360, 365], index=0 if cfg_map["Openbank"][1] == 360 else 1)

        st.markdown("---")
        st.markdown("**Apertura de saldos** (fecha desde la cual quieres que cuente):")
        open_map = {r["account"]: (date.fromisoformat(r["as_of_date"]), float(r["amount"])) for _, r in open_df.iterrows()}

        opening_date = st.date_input("Fecha de apertura (misma para todas)", value=open_map["BBVA"][0])

        bbva_open = st.number_input("BBVA · Saldo en apertura", value=float(open_map["BBVA"][1]), step=500.0)
        nu_turbo_open = st.number_input("Nu Turbo · Saldo en apertura", value=float(open_map["Nu Turbo"][1]), step=500.0)
        nu_73_open = st.number_input("Nu 7.3 · Saldo en apertura", value=float(open_map["Nu 7.3"][1]), step=500.0)
        ob_open = st.number_input("Openbank · Saldo en apertura", value=float(open_map["Openbank"][1]), step=500.0)

        saved = st.form_submit_button("Guardar configuración")


    if saved:
        db_conn.save_account_config(
            conn,
            {
                "BBVA": (0.0, day_basis),
                "Nu Turbo": (nu_turbo_rate / 100.0, day_basis),
                "Nu 7.3": (nu_73_rate / 100.0, day_basis),
                "Openbank": (ob_rate / 100.0, day_basis),
            },
        )

        db_conn.save_opening_balances(
            conn,
            {
                "BBVA": (opening_date.isoformat(), bbva_open),
                "Nu Turbo": (opening_date.isoformat(), nu_turbo_open),
                "Nu 7.3": (opening_date.isoformat(), nu_73_open),
                "Openbank": (opening_date.isoformat(), ob_open),
            },
        )

        st.success("Configuración guardada. Recargando…")
        st.rerun()

# Reload updated config
cfg_df = db_conn.load_account_config(conn)
open_df = db_conn.load_opening_balances(conn)
tx_df = db_conn.load_transactions(conn)

account_params = {
    r["account"]: AccountParams(annual_rate=float(r["annual_rate"]), day_basis=int(r["day_basis"]))
    for _, r in cfg_df.iterrows()
}
opening = {
    r["account"]: (date.fromisoformat(r["as_of_date"]), float(r["amount"]))
    for _, r in open_df.iterrows()
}

# ---- Date range (default: últimos 30 días)
today = date.today()
default_start = today - timedelta(days=29)
colA, colB, colC = st.columns([1, 1, 2])
with colA:
    start = st.date_input("Desde", value=default_start)
with colB:
    end = st.date_input("Hasta", value=today)
with colC:
    st.write("")
    st.info(
        "Convención: primero aplica el **flujo neto del día** y luego calcula el **interés del día** "
        "sobre el saldo resultante (capitalización diaria)."
    )

ledger = compute_daily_ledger(
    transactions=tx_df,
    accounts=ACCOUNTS,
    account_params=account_params,
    opening=opening,
    start=start,
    end=end,
)

# ---- Tabs
tab_resumen, tab_mov = st.tabs(["📊 Resumen", "🧾 Movimientos"])

# =============================
# Resumen
# =============================
with tab_resumen:
    if ledger.empty:
        st.warning("No hay datos aún. Guarda una apertura > 0 o registra movimientos en la pestaña Movimientos.")
    else:
        # Último día (saldos actuales)
        last_day = ledger["Fecha"].max()
        last = ledger[ledger["Fecha"] == last_day].copy()

        bal_by_acct = last.set_index("Cuenta")["Saldo final"].to_dict()
        interest_range = ledger.groupby("Cuenta", as_index=True)["Interés del día"].sum().to_dict()
        nu_total_balance = bal_by_acct.get("Nu Turbo", 0.0) + bal_by_acct.get("Nu 7.3", 0.0)



        total_balance = sum(bal_by_acct.get(a, 0.0) for a in ACCOUNTS)
        bbva_balance = bal_by_acct.get("BBVA", 0.0)
        bbva_pct = (bbva_balance / total_balance * 100.0) if total_balance > 0 else 0.0
        ob_balance = bal_by_acct.get("Openbank", 0.0)

        b1,b2,b3,b4 = st.columns(4)
        b1.metric("Total · saldo", mxn(total_balance))
        b2.metric("BBVA · saldo", mxn(bbva_balance), f"{bbva_pct:.2f}% del total")
        b3.metric("Nu total · saldo", mxn(nu_total_balance))
        b4.metric("Openbank · saldo", mxn(ob_balance))


        c1, c2, c3, c4 = st.columns(4)
        c1.metric("BBVA (quieto) · saldo", mxn(bbva_balance))
        c2.metric("Nu Turbo · interés (rango)", mxn(float(interest_range.get("Nu Turbo", 0.0))))
        c3.metric("Nu 7.3 · interés (rango)", mxn(float(interest_range.get("Nu 7.3", 0.0))))
        c4.metric("Openbank · interés (rango)", mxn(float(interest_range.get("Openbank", 0.0))))
        nu_total_interest = float(interest_range.get("Nu Turbo", 0.0)) + float(interest_range.get("Nu 7.3", 0.0))
        st.metric("Nu total · interés (rango)", mxn(nu_total_interest))


        st.markdown("---")

        # Gráfica de saldos
        st.subheader("Saldos por cuenta (diario)")
        bal_pivot = (
            ledger.pivot_table(index="Fecha", columns="Cuenta", values="Saldo final", aggfunc="last")
            .reindex(columns=ACCOUNTS)
        )
        st.line_chart(bal_pivot)

        # Gráfica de interés acumulado (solo Nu/Openbank)
        st.subheader("Interés acumulado (Nu Turbo vs Nu 7.3 vs Openbank)")
        int_pivot = (
            ledger.pivot_table(index="Fecha", columns="Cuenta", values="Interés acumulado", aggfunc="last")
            .reindex(columns=["Nu Turbo", "Nu 7.3", "Openbank"])
        )
        st.line_chart(int_pivot)


        # Comparativo mensual
        st.subheader("Interés mensual (comparativo)")
        temp = ledger.copy()
        temp["Mes"] = pd.to_datetime(temp["Fecha"]).dt.to_period("M").astype(str)
        monthly = temp.groupby(["Mes", "Cuenta"], as_index=False)["Interés del día"].sum()
        monthly_pivot = monthly.pivot_table(index="Mes", columns="Cuenta", values="Interés del día", aggfunc="sum").fillna(0.0)
        monthly_pivot = monthly_pivot.reindex(columns=["Nu", "Openbank", "BBVA"], fill_value=0.0)

        # Bar chart (BBVA será 0)
        st.bar_chart(monthly_pivot)

        st.markdown("---")





# =============================
# Movimientos
# =============================
with tab_mov:
    st.subheader("Registrar movimiento")

    with st.form("tx_form", clear_on_submit=True):
        tx_date = st.date_input("Fecha", value=today)

        tx_type = st.selectbox("Tipo", ["deposit", "withdrawal", "transfer"])
        amount = st.number_input("Monto (MXN)", min_value=0.0, value=0.0, step=100.0)
        desc = st.text_input("Descripción (opcional)", value="")

        if tx_type == "deposit":
            from_acc = EXTERNAL
            to_acc = st.selectbox("A cuenta", ACCOUNTS, index=0)
        elif tx_type == "withdrawal":
            from_acc = st.selectbox("De cuenta", ACCOUNTS, index=0)
            to_acc = EXTERNAL
        else:  # transfer
            from_acc = st.selectbox("De", ACCOUNTS, index=0)
            to_acc = st.selectbox("A", ACCOUNTS, index=1 if len(ACCOUNTS) > 1 else 0)

        submitted = st.form_submit_button("Agregar")

    if submitted:
        if amount <= 0:
            st.error("El monto debe ser > 0.")
        elif tx_type == "transfer" and from_acc == to_acc:
            st.error("En una transferencia, 'De' y 'A' deben ser diferentes.")
        else:
            db_conn.insert_transaction(
                conn=conn,
                tx_date=tx_date.isoformat(),
                tx_type=tx_type,
                from_account=from_acc,
                to_account=to_acc,
                amount=float(amount),
                description=desc.strip() or None,
            )
            st.success("Movimiento agregado.")
            st.rerun()

    st.markdown("---")
    st.subheader("Historial de movimientos")

    tx_df = db_conn.load_transactions(conn)  # reload
    if tx_df.empty:
        st.info("Aún no hay movimientos. Agrega el primero arriba (o configura una apertura).")
    else:
        # Filtros
        fcol1, fcol2 = st.columns([1, 2])
        with fcol1:
            account_filter = st.selectbox("Filtrar por cuenta (participa en from/to)", ["(todas)"] + ACCOUNTS)
        with fcol2:
            st.caption("Tip: registra transferencias desde **BBVA → Nu/Openbank** para ver la distribución.")

        df_show = tx_df.copy()
        if account_filter != "(todas)":
            df_show = df_show[(df_show["from_account"] == account_filter) | (df_show["to_account"] == account_filter)]

        df_show = df_show.sort_values(["tx_date", "id"], ascending=[False, False])

        # Formateo
        df_disp = df_show.copy()
        df_disp["amount"] = df_disp["amount"].map(mxn)
        st.dataframe(
            df_disp[["id", "tx_date", "tx_type", "from_account", "to_account", "amount", "description"]],
            use_container_width=True,
            hide_index=True,
        )

        # Export
        csv = df_show.to_csv(index=False).encode("utf-8")
        st.download_button("Descargar CSV", data=csv, file_name="transactions.csv", mime="text/csv")

        # Delete (opcional)
        with st.expander("Eliminar movimiento (por ID)"):
            del_id = st.number_input("ID a borrar", min_value=1, step=1)
            if st.button("Eliminar"):
                db_conn.delete_transaction(conn, int(del_id))
                st.success(f"Movimiento {int(del_id)} eliminado.")
                st.rerun()
