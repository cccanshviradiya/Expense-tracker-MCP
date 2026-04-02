"""
Expense Tracker MCP Server — Production-Ready
Tools: add_expense, edit_expense, delete_expense, get_expense,
       add_credit, list_expenses, list_credits, summarize,
       net_balance, search_expenses, monthly_trend,
       set_budget, check_budget, export_csv,
       set_recurring, list_recurring
Resource: expense://categories
"""

from fastmcp import FastMCP
import os
import sqlite3
import json
import csv
import io
from datetime import date as _date, datetime


# ── paths ────────────────────────────────────────────────────────────────────
DB_PATH         = os.path.join(os.path.dirname(__file__), "expenses.db")
CATEGORIES_PATH = os.path.join(os.path.dirname(__file__), "categories.json")

mcp = FastMCP("ExpenseTracker")


# ── helpers ───────────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    """Return a connection with row_factory set."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def _today() -> str:
    return _date.today().isoformat()


def _validate_date(d: str) -> str:
    """Raise ValueError if not a valid ISO date. Returns the string."""
    datetime.strptime(d, "%Y-%m-%d")
    return d


def _load_categories() -> dict:
    with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _rows_to_list(cur: sqlite3.Cursor) -> list[dict]:
    return [dict(r) for r in cur.fetchall()]


# ── DB init ───────────────────────────────────────────────────────────────────

def init_db():
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS expenses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT    NOT NULL,
                amount      REAL    NOT NULL CHECK(amount > 0),
                category    TEXT    NOT NULL,
                subcategory TEXT    DEFAULT '',
                note        TEXT    DEFAULT '',
                created_at  TEXT    NOT NULL DEFAULT (datetime('now')),
                updated_at  TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS credits (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT    NOT NULL,
                amount      REAL    NOT NULL CHECK(amount > 0),
                source      TEXT    NOT NULL,
                note        TEXT    DEFAULT '',
                created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS budgets (
                category    TEXT PRIMARY KEY,
                monthly_limit REAL NOT NULL CHECK(monthly_limit > 0),
                created_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS recurring (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                amount      REAL    NOT NULL CHECK(amount > 0),
                category    TEXT    NOT NULL,
                subcategory TEXT    DEFAULT '',
                note        TEXT    DEFAULT '',
                frequency   TEXT    NOT NULL,   -- 'daily' | 'weekly' | 'monthly' | 'yearly'
                next_date   TEXT    NOT NULL,
                active      INTEGER NOT NULL DEFAULT 1,
                created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
            );
        """)


init_db()


# ═══════════════════════════════════════════════════════════════════════════════
# EXPENSE TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def add_expense(
    amount: float,
    category: str,
    date: str = "",
    subcategory: str = "",
    note: str = "",
) -> dict:
    """
    Add a new expense entry.

    Args:
        amount:      Positive amount spent (required).
        category:    Expense category, e.g. 'food', 'transport' (required).
        date:        ISO date (YYYY-MM-DD). Defaults to today.
        subcategory: Optional sub-category within the category.
        note:        Optional free-text note.

    Returns:
        {"status": "ok", "id": <new_id>}
    """
    if amount <= 0:
        return {"status": "error", "message": "amount must be positive"}
    date = _validate_date(date) if date else _today()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO expenses(date, amount, category, subcategory, note) VALUES (?,?,?,?,?)",
            (date, amount, category.lower().strip(), subcategory.lower().strip(), note.strip()),
        )
        return {"status": "ok", "id": cur.lastrowid}


@mcp.tool()
def edit_expense(
    id: int,
    amount: float = 0,
    category: str = "",
    date: str = "",
    subcategory: str = "",
    note: str = "",
) -> dict:
    """
    Edit an existing expense by its ID. Only the fields you pass will be updated.

    Args:
        id:          The expense ID to update (required).
        amount:      New positive amount (0 = keep existing).
        category:    New category (empty = keep existing).
        date:        New date YYYY-MM-DD (empty = keep existing).
        subcategory: New subcategory (empty = keep existing).
        note:        New note (empty = keep existing).

    Returns:
        {"status": "ok"} or {"status": "error", "message": ...}
    """
    if amount < 0:
        return {"status": "error", "message": "amount cannot be negative"}

    with _conn() as con:
        row = con.execute("SELECT * FROM expenses WHERE id = ?", (id,)).fetchone()
        if not row:
            return {"status": "error", "message": f"No expense found with id={id}"}

        new_amount      = amount      if amount      else row["amount"]
        new_category    = category.lower().strip()    if category    else row["category"]
        new_date        = _validate_date(date) if date else row["date"]
        new_subcategory = subcategory.lower().strip() if subcategory else row["subcategory"]
        new_note        = note.strip()                if note        else row["note"]

        con.execute(
            """UPDATE expenses
               SET amount=?, category=?, date=?, subcategory=?, note=?,
                   updated_at=datetime('now')
               WHERE id=?""",
            (new_amount, new_category, new_date, new_subcategory, new_note, id),
        )
        return {"status": "ok", "id": id}


@mcp.tool()
def delete_expense(id: int) -> dict:
    """
    Permanently delete an expense by its ID.

    Args:
        id: The expense ID to delete.

    Returns:
        {"status": "ok"} or {"status": "error", "message": ...}
    """
    with _conn() as con:
        cur = con.execute("DELETE FROM expenses WHERE id = ?", (id,))
        if cur.rowcount == 0:
            return {"status": "error", "message": f"No expense found with id={id}"}
        return {"status": "ok", "deleted_id": id}


@mcp.tool()
def get_expense(id: int) -> dict:
    """
    Fetch a single expense entry by its ID.

    Args:
        id: The expense ID.

    Returns:
        The expense dict or {"status": "error", "message": ...}
    """
    with _conn() as con:
        row = con.execute("SELECT * FROM expenses WHERE id = ?", (id,)).fetchone()
        if not row:
            return {"status": "error", "message": f"No expense found with id={id}"}
        return dict(row)


@mcp.tool()
def list_expenses(
    start_date: str,
    end_date: str,
    category: str = "",
    limit: int = 200,
) -> list:
    """
    List expense entries within an inclusive date range.

    Args:
        start_date: ISO date (YYYY-MM-DD).
        end_date:   ISO date (YYYY-MM-DD).
        category:   Optional filter by category.
        limit:      Max rows to return (default 200).

    Returns:
        List of expense dicts ordered by date ascending.
    """
    _validate_date(start_date)
    _validate_date(end_date)
    query  = "SELECT * FROM expenses WHERE date BETWEEN ? AND ?"
    params: list = [start_date, end_date]
    if category:
        query  += " AND category = ?"
        params.append(category.lower().strip())
    query += " ORDER BY date ASC, id ASC LIMIT ?"
    params.append(limit)
    with _conn() as con:
        return _rows_to_list(con.execute(query, params))


@mcp.tool()
def search_expenses(
    keyword: str,
    start_date: str = "",
    end_date: str   = "",
) -> list:
    """
    Full-text search across note, category, and subcategory fields.

    Args:
        keyword:    Search term (case-insensitive).
        start_date: Optional ISO date lower bound.
        end_date:   Optional ISO date upper bound.

    Returns:
        Matching expense dicts ordered by date descending.
    """
    query  = "SELECT * FROM expenses WHERE (note LIKE ? OR category LIKE ? OR subcategory LIKE ?)"
    term   = f"%{keyword}%"
    params: list = [term, term, term]
    if start_date:
        _validate_date(start_date)
        query  += " AND date >= ?"
        params.append(start_date)
    if end_date:
        _validate_date(end_date)
        query  += " AND date <= ?"
        params.append(end_date)
    query += " ORDER BY date DESC LIMIT 200"
    with _conn() as con:
        return _rows_to_list(con.execute(query, params))


# ═══════════════════════════════════════════════════════════════════════════════
# CREDIT / INCOME TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def add_credit(
    amount: float,
    source: str,
    date: str = "",
    note: str = "",
) -> dict:
    """
    Record a credit / income entry.

    Args:
        amount: Positive amount received (required).
        source: Income source, e.g. 'salary', 'freelance', 'refund' (required).
        date:   ISO date (YYYY-MM-DD). Defaults to today.
        note:   Optional free-text note.

    Returns:
        {"status": "ok", "id": <new_id>}
    """
    if amount <= 0:
        return {"status": "error", "message": "amount must be positive"}
    date = _validate_date(date) if date else _today()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO credits(date, amount, source, note) VALUES (?,?,?,?)",
            (date, amount, source.lower().strip(), note.strip()),
        )
        return {"status": "ok", "id": cur.lastrowid}


@mcp.tool()
def list_credits(start_date: str, end_date: str) -> list:
    """
    List credit / income entries within an inclusive date range.

    Args:
        start_date: ISO date (YYYY-MM-DD).
        end_date:   ISO date (YYYY-MM-DD).

    Returns:
        List of credit dicts ordered by date ascending.
    """
    _validate_date(start_date)
    _validate_date(end_date)
    with _conn() as con:
        return _rows_to_list(con.execute(
            "SELECT * FROM credits WHERE date BETWEEN ? AND ? ORDER BY date ASC, id ASC",
            (start_date, end_date),
        ))


@mcp.tool()
def delete_credit(id: int) -> dict:
    """
    Delete a credit / income entry by its ID.

    Args:
        id: The credit ID to delete.
    """
    with _conn() as con:
        cur = con.execute("DELETE FROM credits WHERE id = ?", (id,))
        if cur.rowcount == 0:
            return {"status": "error", "message": f"No credit found with id={id}"}
        return {"status": "ok", "deleted_id": id}


# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY & ANALYTICS TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def summarize(
    start_date: str,
    end_date: str,
    category: str = "",
) -> list:
    """
    Summarize total expenses per category within a date range.

    Args:
        start_date: ISO date (YYYY-MM-DD).
        end_date:   ISO date (YYYY-MM-DD).
        category:   Optional filter to a single category.

    Returns:
        List of {"category": ..., "total_amount": ..., "count": ...}
    """
    _validate_date(start_date)
    _validate_date(end_date)
    query  = """
        SELECT category,
               SUM(amount)  AS total_amount,
               COUNT(*)     AS count
        FROM expenses
        WHERE date BETWEEN ? AND ?
    """
    params: list = [start_date, end_date]
    if category:
        query  += " AND category = ?"
        params.append(category.lower().strip())
    query += " GROUP BY category ORDER BY total_amount DESC"
    with _conn() as con:
        return _rows_to_list(con.execute(query, params))


@mcp.tool()
def net_balance(start_date: str, end_date: str) -> dict:
    """
    Compute net balance (total credits minus total expenses) for a date range.

    Args:
        start_date: ISO date (YYYY-MM-DD).
        end_date:   ISO date (YYYY-MM-DD).

    Returns:
        {"total_credits": ..., "total_expenses": ..., "net": ..., "period": {...}}
    """
    _validate_date(start_date)
    _validate_date(end_date)
    with _conn() as con:
        exp_row = con.execute(
            "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE date BETWEEN ? AND ?",
            (start_date, end_date),
        ).fetchone()[0]
        crd_row = con.execute(
            "SELECT COALESCE(SUM(amount),0) FROM credits WHERE date BETWEEN ? AND ?",
            (start_date, end_date),
        ).fetchone()[0]
    return {
        "total_credits":  round(crd_row, 2),
        "total_expenses": round(exp_row, 2),
        "net":            round(crd_row - exp_row, 2),
        "period":         {"start": start_date, "end": end_date},
    }


@mcp.tool()
def monthly_trend(year: int) -> list:
    """
    Return month-by-month expense and credit totals for a given year.

    Args:
        year: Four-digit year, e.g. 2025.

    Returns:
        List of {"month": "YYYY-MM", "total_expenses": ..., "total_credits": ..., "net": ...}
    """
    with _conn() as con:
        exp_rows = con.execute(
            """SELECT strftime('%Y-%m', date) AS month, COALESCE(SUM(amount),0) AS total
               FROM expenses WHERE strftime('%Y', date) = ?
               GROUP BY month ORDER BY month""",
            (str(year),),
        ).fetchall()
        crd_rows = con.execute(
            """SELECT strftime('%Y-%m', date) AS month, COALESCE(SUM(amount),0) AS total
               FROM credits WHERE strftime('%Y', date) = ?
               GROUP BY month ORDER BY month""",
            (str(year),),
        ).fetchall()

    exp_map = {r["month"]: r["total"] for r in exp_rows}
    crd_map = {r["month"]: r["total"] for r in crd_rows}
    months  = sorted(set(exp_map) | set(crd_map))
    return [
        {
            "month":          m,
            "total_expenses": round(exp_map.get(m, 0), 2),
            "total_credits":  round(crd_map.get(m, 0), 2),
            "net":            round(crd_map.get(m, 0) - exp_map.get(m, 0), 2),
        }
        for m in months
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# BUDGET TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def set_budget(category: str, monthly_limit: float) -> dict:
    """
    Set (or update) a monthly budget limit for a category.

    Args:
        category:      Category name, e.g. 'food'.
        monthly_limit: Maximum spend per month (must be > 0).

    Returns:
        {"status": "ok", "category": ..., "monthly_limit": ...}
    """
    if monthly_limit <= 0:
        return {"status": "error", "message": "monthly_limit must be positive"}
    with _conn() as con:
        con.execute(
            "INSERT INTO budgets(category, monthly_limit) VALUES(?,?) "
            "ON CONFLICT(category) DO UPDATE SET monthly_limit=excluded.monthly_limit",
            (category.lower().strip(), monthly_limit),
        )
    return {"status": "ok", "category": category, "monthly_limit": monthly_limit}


@mcp.tool()
def check_budget(month: str = "") -> list:
    """
    Check how much of each budget has been used in a given month.

    Args:
        month: ISO month prefix YYYY-MM (defaults to current month).

    Returns:
        List of {category, monthly_limit, spent, remaining, pct_used, over_budget}
    """
    if not month:
        month = _date.today().strftime("%Y-%m")
    start = f"{month}-01"
    # Last day via strftime trick
    end   = f"{month}-31"
    with _conn() as con:
        budgets = _rows_to_list(con.execute("SELECT * FROM budgets ORDER BY category"))
        result  = []
        for b in budgets:
            spent_row = con.execute(
                "SELECT COALESCE(SUM(amount),0) FROM expenses "
                "WHERE category=? AND date BETWEEN ? AND ?",
                (b["category"], start, end),
            ).fetchone()[0]
            spent     = round(spent_row, 2)
            limit     = b["monthly_limit"]
            remaining = round(limit - spent, 2)
            result.append({
                "category":     b["category"],
                "monthly_limit": limit,
                "spent":        spent,
                "remaining":    remaining,
                "pct_used":     round(spent / limit * 100, 1) if limit else 0,
                "over_budget":  spent > limit,
            })
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# RECURRING EXPENSE TOOLS
# ═══════════════════════════════════════════════════════════════════════════════

VALID_FREQUENCIES = {"daily", "weekly", "monthly", "yearly"}

@mcp.tool()
def set_recurring(
    amount: float,
    category: str,
    frequency: str,
    next_date: str = "",
    subcategory: str = "",
    note: str = "",
) -> dict:
    """
    Create a recurring expense template (e.g. monthly rent, weekly groceries).

    Args:
        amount:      Positive amount.
        category:    Category name.
        frequency:   One of 'daily', 'weekly', 'monthly', 'yearly'.
        next_date:   Next expected date (YYYY-MM-DD). Defaults to today.
        subcategory: Optional subcategory.
        note:        Optional note describing the recurring expense.

    Returns:
        {"status": "ok", "id": <new_id>}
    """
    if amount <= 0:
        return {"status": "error", "message": "amount must be positive"}
    freq = frequency.lower().strip()
    if freq not in VALID_FREQUENCIES:
        return {"status": "error", "message": f"frequency must be one of {sorted(VALID_FREQUENCIES)}"}
    next_date = _validate_date(next_date) if next_date else _today()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO recurring(amount, category, subcategory, note, frequency, next_date) "
            "VALUES (?,?,?,?,?,?)",
            (amount, category.lower().strip(), subcategory.lower().strip(), note.strip(), freq, next_date),
        )
        return {"status": "ok", "id": cur.lastrowid}


@mcp.tool()
def list_recurring(active_only: bool = True) -> list:
    """
    List all recurring expense templates.

    Args:
        active_only: If True (default), return only active templates.

    Returns:
        List of recurring expense dicts.
    """
    query  = "SELECT * FROM recurring"
    params: list = []
    if active_only:
        query  += " WHERE active = 1"
    query += " ORDER BY next_date ASC"
    with _conn() as con:
        return _rows_to_list(con.execute(query, params))


@mcp.tool()
def deactivate_recurring(id: int) -> dict:
    """
    Deactivate (soft-delete) a recurring expense template.

    Args:
        id: The recurring template ID.
    """
    with _conn() as con:
        cur = con.execute("UPDATE recurring SET active=0 WHERE id=?", (id,))
        if cur.rowcount == 0:
            return {"status": "error", "message": f"No recurring entry with id={id}"}
        return {"status": "ok", "deactivated_id": id}


# ═══════════════════════════════════════════════════════════════════════════════
# EXPORT TOOL
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.tool()
def export_csv(
    start_date: str,
    end_date: str,
    include_credits: bool = True,
) -> str:
    """
    Export expenses (and optionally credits) to a CSV string.

    Args:
        start_date:      ISO date (YYYY-MM-DD).
        end_date:        ISO date (YYYY-MM-DD).
        include_credits: Whether to append a credits section (default True).

    Returns:
        A CSV-formatted string ready to save as a .csv file.
    """
    _validate_date(start_date)
    _validate_date(end_date)
    output = io.StringIO()
    writer = csv.writer(output)

    with _conn() as con:
        # Expenses
        writer.writerow(["=== EXPENSES ==="])
        writer.writerow(["id", "date", "amount", "category", "subcategory", "note"])
        rows = con.execute(
            "SELECT id,date,amount,category,subcategory,note FROM expenses "
            "WHERE date BETWEEN ? AND ? ORDER BY date, id",
            (start_date, end_date),
        ).fetchall()
        for r in rows:
            writer.writerow(list(r))

        if include_credits:
            writer.writerow([])
            writer.writerow(["=== CREDITS ==="])
            writer.writerow(["id", "date", "amount", "source", "note"])
            crows = con.execute(
                "SELECT id,date,amount,source,note FROM credits "
                "WHERE date BETWEEN ? AND ? ORDER BY date, id",
                (start_date, end_date),
            ).fetchall()
            for r in crows:
                writer.writerow(list(r))

    return output.getvalue()


# ═══════════════════════════════════════════════════════════════════════════════
# RESOURCE
# ═══════════════════════════════════════════════════════════════════════════════

@mcp.resource("expense://categories", mime_type="application/json")
def categories() -> str:
    """Return the full categories taxonomy as JSON."""
    with open(CATEGORIES_PATH, "r", encoding="utf-8") as f:
        return f.read()

def run_server():
    mcp.run(transport="http", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    run_server()


# # ── entrypoint ────────────────────────────────────────────────────────────────
# if __name__ == "__main__":
#     mcp.run(transport="http", host="0.0.0.0", port=8000)
