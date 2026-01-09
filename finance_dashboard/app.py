import streamlit as st

st.set_page_config(page_title="Finanzas", page_icon="💰", layout="wide")

st.title("💰 Dashboard de finanzas")
st.write("Ve al menú lateral → **Interés ahorros** para registrar movimientos y ver saldos + intereses.")
st.info(
    "Modelo: interés diario con capitalización diaria (Nu/Openbank). "
    "BBVA se modela como cuenta sin interés."
)
