from __future__ import annotations

import os
import sqlite3
from datetime import date, datetime

import pandas as pd

EXTERNAL = "EXTERNAL"


def db_path() -> str:
    os.makedirs("data", exist_ok=True)
    return os.path.join("data", "finance.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection, accounts: list[str], default_rates: dict[str, float], default_day_basis: int = 360) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_config (
            account TEXT PRIMARY KEY,
            annual_rate REAL NOT NULL,
            day_basis INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS opening_balances (
            account TEXT PRIMARY KEY,
            as_of_date TEXT NOT NULL,
            amount REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tx_date TEXT NOT NULL,
            tx_type TEXT NOT NULL,          -- deposit | withdrawal | transfer
            from_account TEXT NOT NULL,
            to_account TEXT NOT NULL,
            amount REAL NOT NULL CHECK(amount >= 0),
            description TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    # Asegura cuentas en config
    existing = set(pd.read_sql_query("SELECT account FROM account_config", conn)["account"].tolist()) \
        if _table_has_rows(conn, "account_config") else set()

    for a in accounts:
        if a not in existing:
            conn.execute(
                "INSERT INTO account_config(account, annual_rate, day_basis) VALUES(?,?,?)",
                (a, float(default_rates.get(a, 0.0)), int(default_day_basis)),
            )
    conn.commit()

    # Asegura cuentas en opening balances
    existing_open = set(pd.read_sql_query("SELECT account FROM opening_balances", conn)["account"].tolist()) \
        if _table_has_rows(conn, "opening_balances") else set()

    today = date.today().isoformat()
    for a in accounts:
        if a not in existing_open:
            conn.execute(
                "INSERT INTO opening_balances(account, as_of_date, amount) VALUES(?,?,?)",
                (a, today, 0.0),
            )
    conn.commit()


def _table_has_rows(conn: sqlite3.Connection, table: str) -> bool:
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0] > 0
    except Exception:
        return False


def load_account_config(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT account, annual_rate, day_basis FROM account_config", conn)


def save_account_config(conn: sqlite3.Connection, cfg: dict[str, tuple[float, int]]) -> None:
    for account, (annual_rate, day_basis) in cfg.items():
        conn.execute(
            """
            INSERT INTO account_config(account, annual_rate, day_basis)
            VALUES(?,?,?)
            ON CONFLICT(account) DO UPDATE SET
              annual_rate=excluded.annual_rate,
              day_basis=excluded.day_basis
            """,
            (account, float(annual_rate), int(day_basis)),
        )
    conn.commit()


def load_opening_balances(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT account, as_of_date, amount FROM opening_balances", conn)


def save_opening_balances(conn: sqlite3.Connection, openings: dict[str, tuple[str, float]]) -> None:
    for account, (as_of_date, amount) in openings.items():
        conn.execute(
            """
            INSERT INTO opening_balances(account, as_of_date, amount)
            VALUES(?,?,?)
            ON CONFLICT(account) DO UPDATE SET
              as_of_date=excluded.as_of_date,
              amount=excluded.amount
            """,
            (account, as_of_date, float(amount)),
        )
    conn.commit()


def load_transactions(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT id, tx_date, tx_type, from_account, to_account, amount, description, created_at
        FROM transactions
        ORDER BY tx_date ASC, id ASC
        """,
        conn,
    )
    if len(df):
        df["tx_date"] = pd.to_datetime(df["tx_date"]).dt.date
    return df


def insert_transaction(
    conn: sqlite3.Connection,
    tx_date: str,
    tx_type: str,
    from_account: str,
    to_account: str,
    amount: float,
    description: str | None,
) -> None:
    conn.execute(
        """
        INSERT INTO transactions(tx_date, tx_type, from_account, to_account, amount, description, created_at)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            tx_date,
            tx_type,
            from_account,
            to_account,
            float(amount),
            description,
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
