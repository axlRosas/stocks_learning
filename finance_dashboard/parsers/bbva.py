from __future__ import annotations

import re
import datetime as dt
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pandas as pd
import pdfplumber
import io


MONTHS = {
    "ENE": 1, "FEB": 2, "MAR": 3, "ABR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AGO": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12,
}

MONEY_RE = re.compile(r"(\d{1,3}(?:,\d{3})*\.\d{2})")


@dataclass(frozen=True)
class StatementTotals:
    total_cargos: float
    count_cargos: int
    total_abonos: float
    count_abonos: int


def _mxn_to_float(s: str) -> float:
    return float(s.replace(",", ""))


def _parse_period(text: str) -> Tuple[dt.date, dt.date]:
    """
    Busca: 'Periodo DEL 03/12/2025 AL 02/01/2026'
    """
    m = re.search(
        r"Periodo\s+DEL\s+(\d{2}/\d{2}/\d{4})\s+AL\s+(\d{2}/\d{2}/\d{4})",
        text
    )
    if not m:
        raise ValueError("No pude encontrar el periodo del estado de cuenta.")
    start = dt.datetime.strptime(m.group(1), "%d/%m/%Y").date()
    end = dt.datetime.strptime(m.group(2), "%d/%m/%Y").date()
    return start, end


def _parse_dd_mmm(dd_mmm: str, start_year: int, end_year: int, start_month: int) -> dt.date:
    """
    Convierte '03/DIC' a fecha con año correcto (maneja cruce de año dentro del periodo).
    Regla: si el mes es menor al mes de inicio del periodo -> usa end_year, si no -> start_year.
    """
    dd, mmm = dd_mmm.split("/")
    m = MONTHS.get(mmm.upper())
    if m is None:
        raise ValueError(f"Mes no reconocido: {dd_mmm}")
    year = end_year if m < start_month else start_year
    return dt.date(year, m, int(dd))


def _parse_totals(text: str) -> Optional[StatementTotals]:
    """
    Busca en el PDF:
      TOTAL IMPORTE CARGOS 56,383.20 TOTAL MOVIMIENTOS CARGOS 42
      TOTAL IMPORTE ABONOS 41,830.83 TOTAL MOVIMIENTOS ABONOS 6
    """
    m_c = re.search(
        r"TOTAL\s+IMPORTE\s+CARGOS\s+(\d{1,3}(?:,\d{3})*\.\d{2}).*?"
        r"TOTAL\s+MOVIMIENTOS\s+CARGOS\s+(\d+)",
        text,
        re.DOTALL
    )
    m_a = re.search(
        r"TOTAL\s+IMPORTE\s+ABONOS\s+(\d{1,3}(?:,\d{3})*\.\d{2}).*?"
        r"TOTAL\s+MOVIMIENTOS\s+ABONOS\s+(\d+)",
        text,
        re.DOTALL
    )
    if not (m_c and m_a):
        return None

    return StatementTotals(
        total_cargos=_mxn_to_float(m_c.group(1)),
        count_cargos=int(m_c.group(2)),
        total_abonos=_mxn_to_float(m_a.group(1)),
        count_abonos=int(m_a.group(2)),
    )


def _seed_credit(description: str) -> bool:
    u = description.upper()
    # Seeds confiables para abonos (puedes ampliar)
    return ("PAGO DE NOMINA" in u) or ("SPEI RECIBIDO" in u)


def _credit_score(description: str) -> int:
    """
    Puntaje para elegir cuáles movimientos (ambiguos) son ABONOS cuando hay más de una solución posible.
    Penaliza fuerte 'ENVIADO' (usualmente cargo).
    """
    u = description.upper()
    score = 0
    if "NOMINA" in u:
        score += 10
    if "RECIB" in u:
        score += 8
    if "ABONO" in u or "DEPOS" in u:
        score += 4

    # Penalizaciones por marcadores típicos de cargo:
    if "ENVIADO" in u:
        score -= 12
    if "RETIRO" in u:
        score -= 8
    if "PAGO TARJETA" in u:
        score -= 6
    if any(k in u for k in ["OXXO", "STARBUCKS", "AMAZON", "STRIPE", "GOOGLE", "WEB TICKETS", "NAYAX"]):
        score -= 4

    # Ambiguo (a veces entra y a veces sale). Le damos leve positivo para que sea candidato.
    if "PAGO CUENTA DE TERCERO" in u:
        score += 1

    return score


def _reconcile_credits(df: pd.DataFrame, totals: StatementTotals) -> pd.DataFrame:
    """
    Ajusta la columna 'direction' para que:
      sum(ABONOS)=total_abonos y count(ABONOS)=count_abonos
      sum(CARGOS)=total_cargos y count(CARGOS)=count_cargos
    Usando seeds + búsqueda exacta con preferencia por descripciones más plausibles como abono.
    """
    df = df.copy()
    target_sum = int(round(totals.total_abonos * 100))
    target_k = totals.count_abonos

    df["is_seed_credit"] = df["description"].apply(_seed_credit)
    seed_idx = df.index[df["is_seed_credit"]].tolist()
    seed_sum = int(round(df.loc[seed_idx, "amount"].sum() * 100))
    seed_k = len(seed_idx)

    remaining_k = target_k - seed_k
    remaining_sum = target_sum - seed_sum

    if remaining_k <= 0:
        credit_set = set(seed_idx)
        df["direction"] = df.index.map(lambda i: "credit" if i in credit_set else "debit")
        return df

    candidates = [i for i in df.index if i not in seed_idx]
    cents = {i: int(round(df.loc[i, "amount"] * 100)) for i in candidates}
    scores = {i: _credit_score(df.loc[i, "description"]) for i in candidates}

    # Orden: primero candidatos con mejor score (y luego por monto para ayudar al pruning)
    candidates.sort(key=lambda i: (scores[i], cents[i]), reverse=True)

    from functools import lru_cache

    @lru_cache(None)
    def best(pos: int, k: int, s: int):
        # returns (best_score, tuple(indices)) or (None, None)
        if k == 0:
            return (0, ()) if s == 0 else (None, None)
        if pos >= len(candidates):
            return (None, None)
        if len(candidates) - pos < k:
            return (None, None)

        # Bounds rápidos (min/max posible con lo que queda)
        remaining = [cents[candidates[j]] for j in range(pos, len(candidates))]
        if sum(sorted(remaining, reverse=True)[:k]) < s:
            return (None, None)
        if sum(sorted(remaining)[:k]) > s:
            return (None, None)

        i = candidates[pos]
        ci = cents[i]
        si = scores[i]

        best_score, best_tuple = (None, None)

        # Take
        if ci <= s:
            sc, tup = best(pos + 1, k - 1, s - ci)
            if sc is not None:
                sc2 = sc + si
                best_score, best_tuple = sc2, (i,) + tup

        # Skip
        sc, tup = best(pos + 1, k, s)
        if sc is not None and (best_score is None or sc > best_score):
            best_score, best_tuple = sc, tup

        return best_score, best_tuple

    sc, sol = best(0, remaining_k, remaining_sum)
    credit_set = set(seed_idx) | set(sol or ())

    df["direction"] = df.index.map(lambda i: "credit" if i in credit_set else "debit")
    return df


def parse_bbva_statement(pdf_bytes: bytes) -> pd.DataFrame:
    """
    Devuelve DataFrame con:
      date, liq_date, description, amount, direction, source
    """
    # 1) Extrae texto por página (sin OCR)
    pages_text: List[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            pages_text.append(page.extract_text() or "")

    full_text = "\n".join(pages_text)

    # 2) Periodo (para inferir año cuando aparece 03/DIC, 01/ENE, etc)
    start, end = _parse_period(full_text)
    start_year, end_year, start_month = start.year, end.year, start.month

    # 3) Totales (para validar / reconciliar abonos vs cargos)
    totals = _parse_totals(full_text)

    # 4) Construye líneas y recorta desde “Detalle de Movimientos Realizados”
    lines: List[str] = []
    for t in pages_text:
        for ln in (t or "").splitlines():
            ln = ln.strip()
            if ln:
                lines.append(ln)

    try:
        idx = lines.index("Detalle de Movimientos Realizados")
        lines = lines[idx + 1 :]
    except ValueError:
        raise ValueError("No encontré la sección 'Detalle de Movimientos Realizados'.")

    start_re = re.compile(r"^(\d{2}/[A-Z]{3})\s+(\d{2}/[A-Z]{3})\s+(.*)$")
    stop_re = re.compile(r"^Total de Movimientos\b")
    noise_re = re.compile(
        r"^(FECHA SALDO|OPER LIQ|PAGINA|No\. de Cuenta|No\. de Cliente|Estado de Cuenta|Libretón|BBVA MEXICO|Av\. Paseo|La GAT Real)\b"
    )

    txs = []
    current = None

    for ln in lines:
        if stop_re.match(ln):
            break
        if noise_re.match(ln):
            continue

        m = start_re.match(ln)
        if m:
            if current:
                txs.append(current)
            current = {
                "op_raw": m.group(1),
                "liq_raw": m.group(2),
                "line1": m.group(3),
                "cont": [],
            }
        else:
            if current:
                current["cont"].append(ln)

    if current:
        txs.append(current)

    # 5) Parse básico (fecha + monto + descripción)
    records = []
    for tx in txs:
        op_date = _parse_dd_mmm(tx["op_raw"], start_year, end_year, start_month)
        liq_date = _parse_dd_mmm(tx["liq_raw"], start_year, end_year, start_month)

        line1 = tx["line1"]
        monies = MONEY_RE.findall(line1)
        amount = _mxn_to_float(monies[0]) if monies else 0.0

        description = " ".join([line1] + tx["cont"]).strip()

        records.append(
            {
                "date": op_date,
                "liq_date": liq_date,
                "description": description,
                "amount": amount,
                "direction": "credit" if _seed_credit(description) else "debit",
                "source": f"bbva_pdf_{start.isoformat()}_{end.isoformat()}",
            }
        )

    df = pd.DataFrame(records).sort_values(["date", "amount"], ascending=[True, False]).reset_index(drop=True)

    # 6) Reconciliación con totales del estado (si existen)
    if totals is not None and len(df) > 0:
        df = _reconcile_credits(df, totals)

    return df
