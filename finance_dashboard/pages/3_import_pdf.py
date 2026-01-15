import streamlit as st
import pandas as pd

from parsers.bbva import parse_bbva_statement

st.set_page_config(page_title="Importar PDF", layout="wide")

st.title("Importar PDF (Estado de cuenta)")
st.caption("Sube tu estado de cuenta y genera movimientos automáticamente (BBVA).")

uploaded = st.file_uploader("Estado de cuenta (PDF)", type=["pdf"])

if uploaded:
    pdf_bytes = uploaded.read()

    with st.spinner("Leyendo y extrayendo movimientos..."):
        df = parse_bbva_statement(pdf_bytes)

    st.subheader("Vista previa")
    st.dataframe(
        df[["date", "description", "amount", "direction"]],
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    st.subheader("Mapeo a tus cuentas (opcional)")
    st.write("Por defecto, los cargos salen de **BBVA** hacia **EXTERNO** y los abonos entran de **EXTERNO** hacia **BBVA**.")

    accounts = ["EXTERNO", "BBVA", "Nu Turbo", "Nu 7.3", "Openbank"]

    default_counterparty = st.selectbox(
        "Cuenta contraparte por defecto (cuando no se detecte destino/origen)",
        options=accounts,
        index=0,
    )

    st.info(
        "Sugerencia: puedes mapear por palabras clave (ej. 'NU MEXICO' -> 'Nu Turbo') "
        "para que ciertos SPEI queden como transferencias."
    )

    # Reglas simples: keyword -> cuenta
    n_rules = st.number_input("Número de reglas", min_value=0, max_value=10, value=2, step=1)
    rules = []
    for i in range(int(n_rules)):
        c1, c2 = st.columns([2, 1])
        with c1:
            kw = st.text_input(f"Keyword #{i+1} (en descripción)", value=("NU MEXICO" if i == 0 else "OPENBANK"))
        with c2:
            acc = st.selectbox(f"Cuenta destino/origen #{i+1}", options=accounts, index=2 if i == 0 else 4)
        rules.append((kw.strip().upper(), acc))

    def apply_mapping(row):
        desc = (row["description"] or "").upper()
        for kw, acc in rules:
            if kw and kw in desc:
                return acc
        return default_counterparty

    df2 = df.copy()
    df2["counterparty_account"] = df2.apply(apply_mapping, axis=1)

    st.subheader("Resultado con mapeo")
    st.dataframe(
        df2[["date", "amount", "direction", "counterparty_account", "description"]],
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    # Aquí tú decides: guardar a CSV o a DB
    st.subheader("Guardar")
    save_as = st.radio("Formato", ["CSV", "SQLite (pendiente)"], horizontal=True)

    if save_as == "CSV":
        csv = df2.to_csv(index=False).encode("utf-8")
        st.download_button("Descargar CSV", data=csv, file_name="movimientos_bbva.csv", mime="text/csv")

    st.success("Listo: ya tienes tu historial de movimientos parseado desde el PDF.")
