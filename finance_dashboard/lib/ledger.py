from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd


@dataclass
class AccountParams:
    annual_rate: float
    day_basis: int


def compute_daily_ledger(
    transactions: pd.DataFrame,
    accounts: list[str],
    account_params: dict[str, AccountParams],
    opening: dict[str, tuple[date, float]],
    start: date,
    end: date,
) -> pd.DataFrame:
    if end < start:
        start, end = end, start

    flows = []
    if len(transactions):
        for _, r in transactions.iterrows():
            d = r["tx_date"]
            amt = float(r["amount"])
            fa = r["from_account"]
            ta = r["to_account"]

            if fa in accounts:
                flows.append({"date": d, "account": fa, "flow": -amt})
            if ta in accounts:
                flows.append({"date": d, "account": ta, "flow": +amt})

    if flows:
        flow_df = pd.DataFrame(flows).groupby(["date", "account"], as_index=False)["flow"].sum()
    else:
        flow_df = pd.DataFrame(columns=["date", "account", "flow"])

    days = (end - start).days + 1
    balances = {a: 0.0 for a in accounts}
    cum_interest = {a: 0.0 for a in accounts}
    opening_map = {a: (od, float(amt)) for a, (od, amt) in opening.items()}

    rows = []
    for i in range(days):
        current = start + timedelta(days=i)
        for a in accounts:
            if a in opening_map and opening_map[a][0] == current:
                balances[a] = opening_map[a][1]
                cum_interest[a] = 0.0

            bal_start = balances[a]
            if len(flow_df):
                mask = (flow_df["date"] == current) & (flow_df["account"] == a)
                net_flow = float(flow_df.loc[mask, "flow"].sum()) if mask.any() else 0.0
            else:
                net_flow = 0.0

            bal_for_interest = bal_start + net_flow
            params = account_params[a]

            interest = 0.0
            if params.annual_rate > 0 and bal_for_interest > 0:
                interest = bal_for_interest * (params.annual_rate / params.day_basis)

            bal_end = bal_for_interest + interest
            balances[a] = bal_end
            cum_interest[a] += interest

            rows.append(
                {
                    "Fecha": current,
                    "Cuenta": a,
                    "Saldo inicial": bal_start,
                    "Flujo neto": net_flow,
                    "Interés del día": interest,
                    "Interés acumulado": cum_interest[a],
                    "Saldo final": bal_end,
                }
            )

    return pd.DataFrame(rows)
