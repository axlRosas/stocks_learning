from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import datetime
from typing import Optional

import pandas as pd

from lib.settings import ACCOUNTS, DEFAULT_DAY_BASIS, DEFAULT_RATES


def db_path() -> str:
    os.makedirs("data", exist_ok=True)
    return os.path.join("data", "finance.db")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    # Accounts config
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            name TEXT PRIMARY KEY,
            interest_rate REAL NOT NULL,
            day_basis INTEGER NOT NULL
        )
        """
    )

    # Ledger
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,                 -- YYYY-MM-DD
            type TEXT NOT NULL,                 -- income|expense|transfer
            from_account TEXT,
            to_account TEXT,
            amount REAL NOT NULL CHECK(amount >= 0),
            description TEXT,
            merchant TEXT,
            category TEXT,
            source TEXT,
            hash TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    # Category rules
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS category_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT NOT NULL,             -- keyword (simple) o regex (luego)
            category TEXT NOT NULL,
            merchant TEXT
        )
        """
    )

    # GBM trades (fase posterior)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gbm_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL,               -- buy|sell
            qty REAL NOT NULL,
            price REAL NOT NULL,
            fees REAL NOT NULL DEFAULT 0,
            notes TEXT
        )
        """
    )

    # Índice único para deduplicar imports
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_hash ON transactions(hash)")

    # Seed accounts
    existing = set(pd.read_sql_query("SELECT name FROM accounts", conn)["name"].tolist()) if _has_rows(conn, "accounts") else set()
    for a in ACCOUNTS:
        if a not in existing:
            conn.execute(
                "INSERT INTO accounts(name, interest_rate, day_basis) VALUES(?,?,?)",
                (a, float(DEFAULT_RATES.get(a, 0.0)), int(DEFAULT_DAY_BASIS)),
            )
    conn.commit()


def _has_rows(conn: sqlite3.Connection, table: str) -> bool:
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0] > 0
    except Exception:
        return False


def tx_hash(date: str, tx_type: str, from_account: str, to_account: str, amount: float, description: str) -> str:
    s = f"{date}|{tx_type}|{from_account}|{to_account}|{amount:.2f}|{(description or '').strip().upper()}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def insert_transaction(
    conn: sqlite3.Connection,
    date: str,
    tx_type: str,
    from_account: Optional[str],
    to_account: Optional[str],
    amount: float,
    description: str = "",
    category: Optional[str] = None,
    merchant: Optional[str] = None,
    source: Optional[str] = None,
) -> bool:
    """
    Inserta con deduplicación por hash.
    Retorna True si insertó, False si fue duplicado.
    """
    h = tx_hash(date, tx_type, from_account or "", to_account or "", float(amount), description or "")
    try:
        conn.execute(
            """
            INSERT INTO transactions(
                date, type, from_account, to_account, amount, description,
                merchant, category, source, hash, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                date, tx_type, from_account, to_account, float(amount), description,
                merchant, category, source, h, datetime.now().isoformat(timespec="seconds")
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def load_transactions(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT id, date, type, from_account, to_account, amount, description, merchant, category, source, created_at
        FROM transactions
        ORDER BY date ASC, id ASC
        """,
        conn,
    )
    if len(df):
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

