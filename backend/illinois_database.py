"""
SQLite database module for Illinois General Assembly Bill Stats.
Provides persistent storage for IL legislators, bills, and laws data.
"""
import os
import sqlite3
import json
import time
from typing import Dict, Any, List, Optional
from contextlib import contextmanager

# Database path - uses same database as Congress stats
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


def _ensure_table_columns(cursor: sqlite3.Cursor, table: str, columns: Dict[str, str]) -> None:
    """Ensure a table has the requested columns, adding any that are missing."""
    cursor.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cursor.fetchall()}
    for name, col_type in columns.items():
        if name in existing:
            continue
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {name} {col_type}")


def init_il_database():
    """Initialize the Illinois-specific database schema."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # IL Legislators table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS il_legislators (
                member_id TEXT PRIMARY KEY,
                ga_session INTEGER NOT NULL,
                chamber TEXT NOT NULL,
                district INTEGER NOT NULL,
                name TEXT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                party TEXT,
                title TEXT,
                updated_at INTEGER
            )
        """)

        # IL Bills table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS il_bills (
                bill_id TEXT PRIMARY KEY,
                ga_session INTEGER NOT NULL,
                bill_type TEXT NOT NULL,
                bill_number INTEGER NOT NULL,
                sponsor_member_id TEXT,
                sponsor_name_raw TEXT,
                primary_sponsor_name TEXT,
                chief_co_sponsors TEXT,
                co_sponsors TEXT,
                title TEXT,
                synopsis TEXT,
                latest_action_text TEXT,
                latest_action_date TEXT,
                public_act_number TEXT,
                updated_at INTEGER,
                FOREIGN KEY (sponsor_member_id) REFERENCES il_legislators(member_id)
            )
        """)

        # IL Laws table (for enacted bills)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS il_laws (
                law_id TEXT PRIMARY KEY,
                ga_session INTEGER NOT NULL,
                public_act_number TEXT NOT NULL,
                bill_id TEXT,
                sponsor_member_id TEXT,
                effective_date TEXT,
                updated_at INTEGER,
                FOREIGN KEY (bill_id) REFERENCES il_bills(bill_id),
                FOREIGN KEY (sponsor_member_id) REFERENCES il_legislators(member_id)
            )
        """)

        # IL Cache metadata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS il_cache_metadata (
                ga_session INTEGER PRIMARY KEY,
                last_full_refresh INTEGER,
                total_bills INTEGER,
                total_laws INTEGER,
                total_members INTEGER,
                unmatched_sponsors INTEGER,
                stats_json TEXT
            )
        """)

        # Create indexes for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_il_bills_session ON il_bills(ga_session)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_il_bills_sponsor ON il_bills(sponsor_member_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_il_laws_session ON il_laws(ga_session)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_il_laws_sponsor ON il_laws(sponsor_member_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_il_legislators_session ON il_legislators(ga_session)")

        # Ensure new sponsor role columns exist in existing databases
        _ensure_table_columns(cursor, "il_bills", {
            "primary_sponsor_name": "TEXT",
            "chief_co_sponsors": "TEXT",
            "co_sponsors": "TEXT",
            "filing_date": "TEXT",
            "enactment_date": "TEXT",
        })

        conn.commit()
        print(f"[il_db] Illinois database tables initialized", flush=True)


def save_il_legislator(member_id: str, ga_session: int, chamber: str, district: int,
                       name: str, first_name: str = None, last_name: str = None,
                       party: str = None, title: str = None):
    """Save or update an Illinois legislator."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO il_legislators
            (member_id, ga_session, chamber, district, name, first_name, last_name, party, title, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (member_id, ga_session, chamber, district, name, first_name, last_name,
              party, title, int(time.time())))
        conn.commit()


def save_il_legislators_batch(ga_session: int, legislators: List[Dict[str, Any]]):
    """Save multiple Illinois legislators in a single transaction."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        now = int(time.time())
        data = []
        for leg in legislators:
            member_id = leg.get("member_id")
            if not member_id:
                continue
            data.append((
                member_id,
                ga_session,
                leg.get("chamber", ""),
                leg.get("district", 0),
                leg.get("name", ""),
                leg.get("first_name"),
                leg.get("last_name"),
                leg.get("party"),
                leg.get("title"),
                now
            ))

        cursor.executemany("""
            INSERT OR REPLACE INTO il_legislators
            (member_id, ga_session, chamber, district, name, first_name, last_name, party, title, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()
        print(f"[il_db] Saved {len(data)} IL legislators for session {ga_session}", flush=True)


def save_il_bill(ga_session: int, bill_type: str, bill_number: int,
                 sponsor_member_id: str = None, sponsor_name_raw: str = None,
                 primary_sponsor_name: str = None, chief_co_sponsors: List[str] = None,
                 co_sponsors: List[str] = None,
                 title: str = None, synopsis: str = None,
                 latest_action_text: str = None, latest_action_date: str = None,
                 public_act_number: str = None):
    """Save or update an Illinois bill."""
    bill_id = f"{ga_session}-{bill_type.lower()}-{bill_number}"
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO il_bills
            (bill_id, ga_session, bill_type, bill_number, sponsor_member_id, sponsor_name_raw,
             primary_sponsor_name, chief_co_sponsors, co_sponsors,
             title, synopsis, latest_action_text, latest_action_date, public_act_number, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (bill_id, ga_session, bill_type.lower(), bill_number, sponsor_member_id,
              sponsor_name_raw, primary_sponsor_name, json.dumps(chief_co_sponsors or []),
              json.dumps(co_sponsors or []), title, synopsis, latest_action_text, latest_action_date,
              public_act_number, int(time.time())))
        conn.commit()


def save_il_bills_batch(ga_session: int, bills: List[Dict[str, Any]]):
    """Save multiple Illinois bills in a single transaction."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        now = int(time.time())
        data = []
        for b in bills:
            bill_type = (b.get("bill_type") or "").lower()
            bill_number = b.get("bill_number")
            if not bill_type or bill_number is None:
                continue
            bill_id = f"{ga_session}-{bill_type}-{bill_number}"
            data.append((
                bill_id,
                ga_session,
                bill_type,
                bill_number,
                b.get("sponsor_member_id"),
                b.get("sponsor_name_raw"),
                b.get("primary_sponsor_name"),
                json.dumps(b.get("chief_co_sponsors") or []),
                json.dumps(b.get("co_sponsors") or []),
                b.get("title"),
                b.get("synopsis"),
                b.get("latest_action_text"),
                b.get("latest_action_date"),
                b.get("filing_date"),
                b.get("enactment_date"),
                b.get("public_act_number"),
                now
            ))

        cursor.executemany("""
            INSERT OR REPLACE INTO il_bills
            (bill_id, ga_session, bill_type, bill_number, sponsor_member_id, sponsor_name_raw,
             primary_sponsor_name, chief_co_sponsors, co_sponsors,
             title, synopsis, latest_action_text, latest_action_date, filing_date, enactment_date,
             public_act_number, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()
        print(f"[il_db] Saved {len(data)} IL bills for session {ga_session}", flush=True)


def save_il_law(ga_session: int, public_act_number: str, bill_id: str = None,
                sponsor_member_id: str = None, effective_date: str = None):
    """Save or update an Illinois law."""
    law_id = f"PA-{public_act_number}"
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO il_laws
            (law_id, ga_session, public_act_number, bill_id, sponsor_member_id, effective_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (law_id, ga_session, public_act_number, bill_id, sponsor_member_id,
              effective_date, int(time.time())))
        conn.commit()


def save_il_laws_batch(ga_session: int, laws: List[Dict[str, Any]]):
    """Save multiple Illinois laws in a single transaction."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        now = int(time.time())
        data = []
        for law in laws:
            public_act_number = law.get("public_act_number")
            if not public_act_number:
                continue
            law_id = f"PA-{public_act_number}"
            data.append((
                law_id,
                ga_session,
                public_act_number,
                law.get("bill_id"),
                law.get("sponsor_member_id"),
                law.get("effective_date"),
                now
            ))

        cursor.executemany("""
            INSERT OR REPLACE INTO il_laws
            (law_id, ga_session, public_act_number, bill_id, sponsor_member_id, effective_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, data)
        conn.commit()
        print(f"[il_db] Saved {len(data)} IL laws for session {ga_session}", flush=True)


def save_il_stats_cache(ga_session: int, stats: Dict[str, Any]):
    """Save computed Illinois stats to cache."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        summary = stats.get("summary", {})
        cursor.execute("""
            INSERT OR REPLACE INTO il_cache_metadata
            (ga_session, last_full_refresh, total_bills, total_laws, total_members, unmatched_sponsors, stats_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            ga_session,
            int(time.time()),
            summary.get("total_bills", 0),
            summary.get("total_laws", 0),
            summary.get("total_legislators", 0),
            stats.get("unmatched_sponsors", 0),
            json.dumps(stats)
        ))
        conn.commit()
        print(f"[il_db] Saved stats cache for IL session {ga_session}", flush=True)


def load_il_stats_cache(ga_session: int) -> Optional[Dict[str, Any]]:
    """Load cached Illinois stats from database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT stats_json, last_full_refresh FROM il_cache_metadata WHERE ga_session = ?
        """, (ga_session,))
        row = cursor.fetchone()
        if row and row["stats_json"]:
            stats = json.loads(row["stats_json"])
            print(f"[il_db] Loaded stats cache for IL session {ga_session} (cached at {row['last_full_refresh']})", flush=True)
            return stats
        return None


def get_il_cache_metadata(ga_session: int) -> Optional[Dict[str, Any]]:
    """Get cache metadata without the full stats JSON."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ga_session, last_full_refresh, total_bills, total_laws, total_members, unmatched_sponsors
            FROM il_cache_metadata WHERE ga_session = ?
        """, (ga_session,))
        row = cursor.fetchone()
        if row:
            return {
                "ga_session": row["ga_session"],
                "last_full_refresh": row["last_full_refresh"],
                "total_bills": row["total_bills"],
                "total_laws": row["total_laws"],
                "total_members": row["total_members"],
                "unmatched_sponsors": row["unmatched_sponsors"],
            }
        return None


def get_il_stats_from_db(ga_session: int) -> Optional[Dict[str, Any]]:
    """
    Compute Illinois stats directly from the database tables.
    Useful when we have the raw data but no cached stats.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Get legislator stats with bill and law counts
        cursor.execute("""
            SELECT
                l.member_id,
                l.name as sponsor_name,
                l.party,
                l.chamber,
                l.district,
                COUNT(DISTINCT b.bill_id) as sponsored_total,
                COUNT(DISTINCT law.law_id) as enacted_total
            FROM il_legislators l
            LEFT JOIN il_bills b ON l.member_id = b.sponsor_member_id AND b.ga_session = ?
            LEFT JOIN il_laws law ON b.bill_id = law.bill_id AND law.ga_session = ?
            WHERE l.ga_session = ?
            GROUP BY l.member_id
            HAVING sponsored_total > 0
            ORDER BY sponsored_total DESC, sponsor_name ASC
        """, (ga_session, ga_session, ga_session))

        rows = []
        for row in cursor.fetchall():
            rows.append({
                "memberId": row["member_id"],
                "sponsorName": row["sponsor_name"],
                "party": row["party"],
                "chamber": row["chamber"],
                "district": row["district"],
                "sponsored_total": row["sponsored_total"],
                "enacted_total": row["enacted_total"],
            })

        if not rows:
            return None

        # Get totals
        cursor.execute("SELECT COUNT(*) FROM il_bills WHERE ga_session = ?", (ga_session,))
        total_bills = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM il_laws WHERE ga_session = ?", (ga_session,))
        total_laws = cursor.fetchone()[0]

        return {
            "ga_session": ga_session,
            "generated_at": int(time.time()),
            "rows": rows,
            "summary": {
                "total_legislators": len(rows),
                "total_bills": total_bills,
                "total_laws": total_laws,
            },
            "note": "Stats computed from database.",
        }


def clear_il_session_data(ga_session: int):
    """Clear all data for a specific Illinois GA session."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM il_laws WHERE ga_session = ?", (ga_session,))
        cursor.execute("DELETE FROM il_bills WHERE ga_session = ?", (ga_session,))
        cursor.execute("DELETE FROM il_legislators WHERE ga_session = ?", (ga_session,))
        cursor.execute("DELETE FROM il_cache_metadata WHERE ga_session = ?", (ga_session,))
        conn.commit()
        print(f"[il_db] Cleared data for IL session {ga_session}", flush=True)


def get_il_legislator_by_id(member_id: str) -> Optional[Dict[str, Any]]:
    """Get a single legislator by member_id."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM il_legislators WHERE member_id = ?
        """, (member_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None


def get_il_legislators_by_session(ga_session: int) -> List[Dict[str, Any]]:
    """Get all legislators for a session."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM il_legislators WHERE ga_session = ? ORDER BY chamber, district
        """, (ga_session,))
        return [dict(row) for row in cursor.fetchall()]


def get_existing_bill_filenames(ga_session: int) -> set:
    """
    Get set of bill XML filenames that are already in the database.
    Returns filenames in format like '10400HB0001.xml'.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT bill_type, bill_number FROM il_bills WHERE ga_session = ?
        """, (ga_session,))

        filenames = set()
        for row in cursor.fetchall():
            bill_type = row["bill_type"].upper()
            bill_number = row["bill_number"]
            # Reconstruct filename: 10400HB0001.xml
            filename = f"{ga_session}00{bill_type}{bill_number:04d}.xml"
            filenames.add(filename)

        return filenames


def get_all_bills_for_session(ga_session: int) -> List[Dict[str, Any]]:
    """Get all bills for a session from database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM il_bills WHERE ga_session = ?
        """, (ga_session,))
        return [dict(row) for row in cursor.fetchall()]


def get_pending_bills_for_update(ga_session: int) -> List[Dict[str, Any]]:
    """
    Get bills that may need status updates (no public_act_number yet).
    Returns bills with their latest_action_date for comparison with server data.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT bill_id, bill_type, bill_number, latest_action_date
            FROM il_bills
            WHERE ga_session = ? AND (public_act_number IS NULL OR public_act_number = '')
        """, (ga_session,))
        return [dict(row) for row in cursor.fetchall()]


def update_il_bill(bill_id: str, data: Dict[str, Any]) -> None:
    """
    Update an existing bill record with new data.
    Used when re-fetching a bill that may have become a public act.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Build dynamic update query based on provided fields
        update_fields = []
        values = []

        allowed_fields = [
            "public_act_number", "latest_action_date", "latest_action_text",
            "sponsor_member_id", "primary_sponsor_name", "chief_co_sponsors",
            "co_sponsors", "title", "synopsis", "filing_date", "enactment_date"
        ]

        for field in allowed_fields:
            if field in data:
                update_fields.append(f"{field} = ?")
                value = data[field]
                # JSON-encode lists
                if isinstance(value, list):
                    value = json.dumps(value)
                values.append(value)

        if not update_fields:
            return

        # Add updated_at timestamp
        update_fields.append("updated_at = ?")
        values.append(int(time.time()))

        # Add bill_id for WHERE clause
        values.append(bill_id)

        query = f"UPDATE il_bills SET {', '.join(update_fields)} WHERE bill_id = ?"
        cursor.execute(query, values)
        conn.commit()


def get_il_timeline_data(ga_session: int) -> Dict[str, Any]:
    """
    Get bill activity data for timeline visualization.
    Returns bills aggregated by month for filing and enactment dates.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Get filing dates
        cursor.execute("""
            SELECT filing_date, COUNT(*) as count
            FROM il_bills
            WHERE ga_session = ? AND filing_date IS NOT NULL AND filing_date != ''
            GROUP BY substr(filing_date, 1, 7)
            ORDER BY filing_date
        """, (ga_session,))

        # Parse and aggregate by month (filing_date is M/D/YYYY format)
        filed_by_month: Dict[str, int] = {}
        for row in cursor.fetchall():
            date_str = row["filing_date"]
            try:
                # Parse M/D/YYYY and convert to YYYY-MM
                parts = date_str.split("/")
                if len(parts) == 3:
                    month_key = f"{parts[2]}-{parts[0].zfill(2)}"
                    filed_by_month[month_key] = filed_by_month.get(month_key, 0) + row["count"]
            except (ValueError, IndexError):
                continue

        # Get enactment dates
        cursor.execute("""
            SELECT enactment_date, COUNT(*) as count
            FROM il_bills
            WHERE ga_session = ? AND enactment_date IS NOT NULL AND enactment_date != ''
                AND public_act_number IS NOT NULL AND public_act_number != ''
            GROUP BY substr(enactment_date, 1, 7)
            ORDER BY enactment_date
        """, (ga_session,))

        enacted_by_month: Dict[str, int] = {}
        for row in cursor.fetchall():
            date_str = row["enactment_date"]
            try:
                parts = date_str.split("/")
                if len(parts) == 3:
                    month_key = f"{parts[2]}-{parts[0].zfill(2)}"
                    enacted_by_month[month_key] = enacted_by_month.get(month_key, 0) + row["count"]
            except (ValueError, IndexError):
                continue

        # Get all months in order
        all_months = sorted(set(list(filed_by_month.keys()) + list(enacted_by_month.keys())))

        return {
            "ga_session": ga_session,
            "months": all_months,
            "filed": [filed_by_month.get(m, 0) for m in all_months],
            "enacted": [enacted_by_month.get(m, 0) for m in all_months],
        }


def get_il_network_data(ga_session: int, min_connections: int = 3) -> Dict[str, Any]:
    """
    Get co-sponsor network data for D3.js visualization.
    Returns nodes (legislators) and links (co-sponsorship relationships).
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Get all bills with sponsors and co-sponsors
        cursor.execute("""
            SELECT
                b.bill_id,
                b.sponsor_member_id,
                b.chief_co_sponsors,
                b.co_sponsors,
                l.name as sponsor_name,
                l.party as sponsor_party
            FROM il_bills b
            LEFT JOIN il_legislators l ON b.sponsor_member_id = l.member_id
            WHERE b.ga_session = ? AND b.sponsor_member_id IS NOT NULL
        """, (ga_session,))

        # Build connection counts between legislators
        connections: Dict[tuple, int] = {}  # (member1, member2) -> count
        legislator_info: Dict[str, Dict[str, Any]] = {}

        # Get all legislators for lookup
        cursor.execute("""
            SELECT member_id, name, party, chamber, district
            FROM il_legislators WHERE ga_session = ?
        """, (ga_session,))

        for row in cursor.fetchall():
            legislator_info[row["member_id"]] = {
                "id": row["member_id"],
                "name": row["name"],
                "party": row["party"],
                "chamber": row["chamber"],
                "district": row["district"],
            }

        # Process bills for co-sponsorship links
        for row in cursor.fetchall():
            sponsor_id = row["sponsor_member_id"]
            if not sponsor_id:
                continue

            # Parse co-sponsors JSON
            chief_co = []
            co = []
            try:
                chief_co = json.loads(row["chief_co_sponsors"] or "[]")
            except json.JSONDecodeError:
                pass
            try:
                co = json.loads(row["co_sponsors"] or "[]")
            except json.JSONDecodeError:
                pass

            all_cosponsors = chief_co + co

            # For each co-sponsor, create a link to the primary sponsor
            for cosponsor_name in all_cosponsors:
                # Find cosponsor by name match (simplified - uses exact match on normalized name)
                cosponsor_id = None
                normalized = cosponsor_name.lower().strip()
                for lid, linfo in legislator_info.items():
                    if linfo["name"].lower().strip() == normalized:
                        cosponsor_id = lid
                        break

                if not cosponsor_id or cosponsor_id == sponsor_id:
                    continue

                # Create sorted pair for undirected link
                pair = tuple(sorted([sponsor_id, cosponsor_id]))
                connections[pair] = connections.get(pair, 0) + 1

        # Filter to only connections >= min_connections
        filtered_connections = {k: v for k, v in connections.items() if v >= min_connections}

        # Build nodes and links
        active_ids = set()
        for (id1, id2), count in filtered_connections.items():
            active_ids.add(id1)
            active_ids.add(id2)

        nodes = []
        for lid in active_ids:
            if lid in legislator_info:
                info = legislator_info[lid]
                nodes.append({
                    "id": lid,
                    "name": info["name"],
                    "party": info["party"],
                    "chamber": info["chamber"],
                    "district": info["district"],
                })

        links = []
        for (id1, id2), count in filtered_connections.items():
            links.append({
                "source": id1,
                "target": id2,
                "value": count,
            })

        return {
            "ga_session": ga_session,
            "nodes": nodes,
            "links": links,
            "min_connections": min_connections,
        }


# Initialize IL database tables on module import
init_il_database()
