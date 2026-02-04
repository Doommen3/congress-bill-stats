"""
SQLite database module for Congress Bill Stats.
Provides persistent storage for legislators, bills, and laws data.
"""
import os
import sqlite3
import json
import time
from typing import Dict, Any, List, Optional
from contextlib import contextmanager

# Database path - can be overridden via environment variable
DB_PATH = os.environ.get("DATABASE_PATH", os.path.join(os.path.dirname(__file__), "congress_stats.db"))


@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_database():
    """Initialize the database schema."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Legislators table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS legislators (
                bioguide_id TEXT PRIMARY KEY,
                name TEXT,
                party TEXT,
                state TEXT,
                chamber TEXT,
                updated_at INTEGER
            )
        """)

        # Bills table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bills (
                bill_id TEXT PRIMARY KEY,
                congress INTEGER NOT NULL,
                bill_type TEXT NOT NULL,
                bill_number INTEGER NOT NULL,
                sponsor_bioguide_id TEXT,
                title TEXT,
                latest_action_text TEXT,
                latest_action_date TEXT,
                update_date TEXT,
                cosponsors_last_update_date TEXT,
                cosponsors_updated_at INTEGER,
                updated_at INTEGER,
                FOREIGN KEY (sponsor_bioguide_id) REFERENCES legislators(bioguide_id)
            )
        """)

        # Bill cosponsors table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bill_cosponsors (
                bill_id TEXT NOT NULL,
                congress INTEGER NOT NULL,
                bioguide_id TEXT NOT NULL,
                is_original INTEGER DEFAULT 0,
                withdrawn INTEGER DEFAULT 0,
                updated_at INTEGER,
                PRIMARY KEY (bill_id, bioguide_id),
                FOREIGN KEY (bill_id) REFERENCES bills(bill_id),
                FOREIGN KEY (bioguide_id) REFERENCES legislators(bioguide_id)
            )
        """)

        # Laws table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS laws (
                law_id TEXT PRIMARY KEY,
                congress INTEGER NOT NULL,
                law_type TEXT NOT NULL,
                law_number TEXT,
                bill_id TEXT,
                sponsor_bioguide_id TEXT,
                updated_at INTEGER,
                FOREIGN KEY (bill_id) REFERENCES bills(bill_id),
                FOREIGN KEY (sponsor_bioguide_id) REFERENCES legislators(bioguide_id)
            )
        """)

        # Cache metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cache_metadata (
                congress INTEGER PRIMARY KEY,
                last_full_refresh INTEGER,
                total_bills INTEGER,
                total_laws INTEGER,
                stats_json TEXT
            )
        """)

        # Create indexes for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bills_congress ON bills(congress)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bills_sponsor ON bills(sponsor_bioguide_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bill_cosponsors_congress ON bill_cosponsors(congress)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_bill_cosponsors_bioguide ON bill_cosponsors(bioguide_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_laws_congress ON laws(congress)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_laws_sponsor ON laws(sponsor_bioguide_id)")

        conn.commit()
        print(f"[db] Database initialized at {DB_PATH}", flush=True)

        # Ensure new columns exist in existing databases
        _ensure_column(cursor, "bills", "update_date", "TEXT")
        _ensure_column(cursor, "bills", "cosponsors_last_update_date", "TEXT")
        _ensure_column(cursor, "bills", "cosponsors_updated_at", "INTEGER")
        conn.commit()


def _ensure_column(cursor: sqlite3.Cursor, table: str, column: str, coltype: str) -> None:
    """Add a column to a table if it does not already exist."""
    cursor.execute(f"PRAGMA table_info({table})")
    columns = {row[1] for row in cursor.fetchall()}
    if column in columns:
        return
    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")


def save_legislator(bioguide_id: str, name: str, party: str, state: str, chamber: str):
    """Save or update a legislator."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO legislators (bioguide_id, name, party, state, chamber, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (bioguide_id, name, party, state, chamber, int(time.time())))
        conn.commit()


def save_legislators_batch(legislators: List[Dict[str, Any]]):
    """Save multiple legislators in a single transaction."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        now = int(time.time())
        cursor.executemany("""
            INSERT OR REPLACE INTO legislators (bioguide_id, name, party, state, chamber, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            (l["bioguideId"], l.get("sponsorName") or l.get("name"), l.get("party"),
             l.get("state"), l.get("chamber"), now)
            for l in legislators if l.get("bioguideId")
        ])
        conn.commit()
        print(f"[db] Saved {len(legislators)} legislators", flush=True)


def save_bill(
    congress: int,
    bill_type: str,
    bill_number: int,
    sponsor_bioguide_id: str,
    title: str = None,
    latest_action_text: str = None,
    latest_action_date: str = None,
    update_date: str = None,
):
    """Save or update a bill."""
    bill_id = f"{congress}-{bill_type.lower()}-{bill_number}"
    with get_db_connection() as conn:
        cursor = conn.cursor()
        now = int(time.time())
        cursor.execute("""
            INSERT INTO bills
            (bill_id, congress, bill_type, bill_number, sponsor_bioguide_id,
             title, latest_action_text, latest_action_date, update_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bill_id) DO UPDATE SET
                congress=excluded.congress,
                bill_type=excluded.bill_type,
                bill_number=excluded.bill_number,
                sponsor_bioguide_id=excluded.sponsor_bioguide_id,
                title=excluded.title,
                latest_action_text=excluded.latest_action_text,
                latest_action_date=excluded.latest_action_date,
                update_date=excluded.update_date,
                updated_at=excluded.updated_at
        """, (bill_id, congress, bill_type.lower(), bill_number, sponsor_bioguide_id,
              title, latest_action_text, latest_action_date, update_date, now))
        conn.commit()


def save_bills_batch(congress: int, bills: List[Dict[str, Any]]):
    """Save multiple bills in a single transaction."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        now = int(time.time())
        data = []
        for b in bills:
            bill_type = (b.get("type") or "").lower()
            bill_number = b.get("number")
            if not bill_type or not bill_number:
                continue
            bill_id = f"{congress}-{bill_type}-{bill_number}"
            sponsor = b.get("_sponsor_info") or {}
            latest = b.get("latestAction") or {}
            update_date = (
                b.get("_update_date")
                or b.get("updateDateIncludingText")
                or b.get("updateDate")
                or latest.get("actionDate")
            )
            data.append((
                bill_id, congress, bill_type, bill_number,
                sponsor.get("bioguideId"),
                b.get("title"),
                latest.get("text"),
                latest.get("actionDate"),
                update_date,
                now
            ))

        cursor.executemany("""
            INSERT INTO bills
            (bill_id, congress, bill_type, bill_number, sponsor_bioguide_id,
             title, latest_action_text, latest_action_date, update_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bill_id) DO UPDATE SET
                congress=excluded.congress,
                bill_type=excluded.bill_type,
                bill_number=excluded.bill_number,
                sponsor_bioguide_id=excluded.sponsor_bioguide_id,
                title=excluded.title,
                latest_action_text=excluded.latest_action_text,
                latest_action_date=excluded.latest_action_date,
                update_date=excluded.update_date,
                updated_at=excluded.updated_at
        """, data)
        conn.commit()
        print(f"[db] Saved {len(data)} bills for Congress {congress}", flush=True)


def save_bill_cosponsors_batch(congress: int, cosponsors: List[Dict[str, Any]]):
    """Save multiple bill cosponsor records in a single transaction."""
    if not cosponsors:
        return
    with get_db_connection() as conn:
        cursor = conn.cursor()
        now = int(time.time())
        data = []
        for c in cosponsors:
            bill_id = c.get("bill_id")
            bioguide_id = c.get("bioguide_id")
            if not bill_id or not bioguide_id:
                continue
            data.append((
                bill_id,
                congress,
                bioguide_id,
                1 if c.get("is_original") else 0,
                1 if c.get("withdrawn") else 0,
                now,
            ))

        cursor.executemany("""
            INSERT OR REPLACE INTO bill_cosponsors
            (bill_id, congress, bioguide_id, is_original, withdrawn, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()
        print(f"[db] Saved {len(data)} bill cosponsors for Congress {congress}", flush=True)


def save_law(congress: int, law_type: str, law_number: str, bill_id: str,
             sponsor_bioguide_id: str = None):
    """Save or update a law."""
    law_id = f"{congress}-{law_type}-{law_number}"
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO laws
            (law_id, congress, law_type, law_number, bill_id, sponsor_bioguide_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (law_id, congress, law_type, law_number, bill_id, sponsor_bioguide_id, int(time.time())))
        conn.commit()


def save_laws_batch(congress: int, laws: List[Dict[str, Any]]):
    """Save multiple laws in a single transaction."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        now = int(time.time())
        data = []
        for law in laws:
            law_type = law.get("_law_type", "public")
            law_number = law.get("number")
            if not law_number:
                continue

            # Get bill reference
            bill = law.get("bill") or law
            bill_type = (bill.get("type") or "").lower()
            bill_num = bill.get("number")
            bill_id = f"{congress}-{bill_type}-{bill_num}" if bill_type and bill_num else None

            law_id = f"{congress}-{law_type}-{law_number}"
            data.append((
                law_id, congress, law_type, law_number, bill_id,
                law.get("_sponsor_bioguide_id"), now
            ))

        cursor.executemany("""
            INSERT OR REPLACE INTO laws
            (law_id, congress, law_type, law_number, bill_id, sponsor_bioguide_id, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()
        print(f"[db] Saved {len(data)} laws for Congress {congress}", flush=True)


def save_stats_cache(congress: int, stats: Dict[str, Any]):
    """Save computed stats to cache."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        summary = stats.get("summary", {})
        cursor.execute("""
            INSERT OR REPLACE INTO cache_metadata
            (congress, last_full_refresh, total_bills, total_laws, stats_json)
            VALUES (?, ?, ?, ?, ?)
        """, (
            congress,
            int(time.time()),
            summary.get("total_bills", 0),
            summary.get("total_laws", 0),
            json.dumps(stats)
        ))
        conn.commit()
        print(f"[db] Saved stats cache for Congress {congress}", flush=True)


def load_stats_cache(congress: int) -> Optional[Dict[str, Any]]:
    """Load cached stats from database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT stats_json, last_full_refresh FROM cache_metadata WHERE congress = ?
        """, (congress,))
        row = cursor.fetchone()
        if row and row["stats_json"]:
            stats = json.loads(row["stats_json"])
            print(f"[db] Loaded stats cache for Congress {congress} (cached at {row['last_full_refresh']})", flush=True)
            return stats
        return None


def get_cache_metadata(congress: int) -> Optional[Dict[str, Any]]:
    """Get cache metadata without the full stats JSON."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT congress, last_full_refresh, total_bills, total_laws
            FROM cache_metadata WHERE congress = ?
        """, (congress,))
        row = cursor.fetchone()
        if row:
            return {
                "congress": row["congress"],
                "last_full_refresh": row["last_full_refresh"],
                "total_bills": row["total_bills"],
                "total_laws": row["total_laws"],
            }
        return None


def get_bill_cosponsor_refresh_map(congress: int) -> Dict[str, Dict[str, Any]]:
    """Get per-bill cosponsor refresh metadata."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT bill_id, cosponsors_last_update_date, cosponsors_updated_at
            FROM bills
            WHERE congress = ?
        """, (congress,))
        out: Dict[str, Dict[str, Any]] = {}
        for row in cursor.fetchall():
            out[row["bill_id"]] = {
                "cosponsors_last_update_date": row["cosponsors_last_update_date"],
                "cosponsors_updated_at": row["cosponsors_updated_at"],
            }
        return out


def mark_bill_cosponsors_refreshed(congress: int, bill_updates: Dict[str, Optional[str]]) -> None:
    """Update cosponsor refresh metadata for bills."""
    if not bill_updates:
        return
    now = int(time.time())
    with get_db_connection() as conn:
        cursor = conn.cursor()
        with_update = []
        without_update = []
        for bill_id, update_date in bill_updates.items():
            if update_date:
                with_update.append((update_date, now, bill_id, congress))
            else:
                without_update.append((now, bill_id, congress))

        if with_update:
            cursor.executemany("""
                UPDATE bills
                SET cosponsors_last_update_date = ?, cosponsors_updated_at = ?
                WHERE bill_id = ? AND congress = ?
            """, with_update)
        if without_update:
            cursor.executemany("""
                UPDATE bills
                SET cosponsors_updated_at = ?
                WHERE bill_id = ? AND congress = ?
            """, without_update)
        conn.commit()


def delete_bill_cosponsors_for_bills(congress: int, bill_ids: List[str]) -> None:
    """Delete cosponsor records for specific bills."""
    if not bill_ids:
        return
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany("""
            DELETE FROM bill_cosponsors
            WHERE congress = ? AND bill_id = ?
        """, [(congress, bill_id) for bill_id in bill_ids])
        conn.commit()


def clear_bill_cosponsors_for_congress(congress: int) -> None:
    """Delete all cosponsor records for a congress."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM bill_cosponsors WHERE congress = ?", (congress,))
        conn.commit()


def get_stats_from_db(congress: int) -> Optional[Dict[str, Any]]:
    """
    Compute stats directly from the database tables.
    Useful when we have the raw data but no cached stats.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Get legislator stats with bill and law counts
        cursor.execute("""
            SELECT
                l.bioguide_id,
                l.name as sponsor_name,
                l.party,
                l.state,
                l.chamber,
                COUNT(DISTINCT b.bill_id) as sponsored_total,
                COUNT(DISTINCT CASE WHEN bc.withdrawn = 0 THEN bc.bill_id END) as cosponsor_total,
                COUNT(DISTINCT CASE WHEN bc.withdrawn = 0 AND bc.is_original = 1 THEN bc.bill_id END) as original_cosponsor_total,
                COUNT(DISTINCT CASE WHEN law.law_type = 'public' THEN law.law_id END) as public_law_count,
                COUNT(DISTINCT CASE WHEN law.law_type = 'private' THEN law.law_id END) as private_law_count,
                COUNT(DISTINCT law.law_id) as enacted_total
            FROM legislators l
            LEFT JOIN bills b ON l.bioguide_id = b.sponsor_bioguide_id AND b.congress = ?
            LEFT JOIN bill_cosponsors bc ON l.bioguide_id = bc.bioguide_id AND bc.congress = ?
            LEFT JOIN laws law ON b.bill_id = law.bill_id AND law.congress = ?
            GROUP BY l.bioguide_id
            HAVING sponsored_total > 0 OR cosponsor_total > 0 OR original_cosponsor_total > 0
            ORDER BY sponsored_total DESC, sponsor_name ASC
        """, (congress, congress, congress))

        rows = []
        for row in cursor.fetchall():
            rows.append({
                "bioguideId": row["bioguide_id"],
                "sponsorName": row["sponsor_name"],
                "party": row["party"],
                "state": row["state"],
                "chamber": row["chamber"],
                "sponsored_total": row["sponsored_total"],
                "primary_sponsor_total": row["sponsored_total"],
                "cosponsor_total": row["cosponsor_total"],
                "original_cosponsor_total": row["original_cosponsor_total"],
                "public_law_count": row["public_law_count"],
                "private_law_count": row["private_law_count"],
                "enacted_total": row["enacted_total"],
            })

        if not rows:
            return None

        # Get totals
        cursor.execute("SELECT COUNT(*) FROM bills WHERE congress = ?", (congress,))
        total_bills = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM laws WHERE congress = ?", (congress,))
        total_laws = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM laws WHERE congress = ? AND law_type = 'public'", (congress,))
        public_laws = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM laws WHERE congress = ? AND law_type = 'private'", (congress,))
        private_laws = cursor.fetchone()[0]

        return {
            "congress": congress,
            "generated_at": int(time.time()),
            "rows": rows,
            "summary": {
                "total_legislators": len(rows),
                "total_bills": total_bills,
                "total_laws": total_laws,
                "public_laws": public_laws,
                "private_laws": private_laws,
            },
            "note": "Stats computed from database.",
        }


def clear_congress_data(congress: int):
    """Clear all data for a specific congress."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM laws WHERE congress = ?", (congress,))
        cursor.execute("DELETE FROM bill_cosponsors WHERE congress = ?", (congress,))
        cursor.execute("DELETE FROM bills WHERE congress = ?", (congress,))
        cursor.execute("DELETE FROM cache_metadata WHERE congress = ?", (congress,))
        conn.commit()
        print(f"[db] Cleared data for Congress {congress}", flush=True)


# Initialize database on module import
init_database()
