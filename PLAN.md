# Illinois General Assembly Statistics Implementation Plan

## Overview
Add Illinois General Assembly (ILGA) bill statistics to the congress-bill-stats application, using official ILGA FTP XML data as the primary source for the 104th General Assembly (2025-2026).

## Data Sources
- **Bill Status XML**: `ilga.gov/ftp/legislation/104/BillStatus/XML/` (e.g., `10400SB0001.xml`)
- **Members XML**: `ilga.gov/ftp/Members/` (e.g., `104HouseMembers.xml`, `104SenateMembers.xml`)

## Implementation Steps

### Step 1: Create Illinois Database Module
**File**: `backend/illinois_database.py`
- New tables: `il_legislators`, `il_bills`, `il_laws`, `il_cache_metadata`
- Batch insert functions for legislators, bills, laws
- Cache load/save functions
- Follow existing `database.py` patterns

### Step 2: Create Illinois Stats Module
**File**: `backend/illinois_stats.py`
- `ILDataFetcher` class: Fetch XML from ILGA FTP
- `ILNameMatcher` class: Match sponsor names to member records
- XML parsing for members and bills
- Public Act detection via regex: `r'Public\s+Act\s*\.+\s*(\d{3}-\d{4})'`
- `build_il_stats()` main function following `build_stats()` pattern

### Step 3: Add API Endpoints
**File**: `backend/main.py`
- `GET /api/il-stats?session=104` - Get Illinois legislator statistics
- `GET /api/il-refresh-status?session=104` - Check background refresh status
- Same caching/background refresh pattern as Congress endpoints

### Step 4: Update Frontend for Toggle
**Files**: `frontend/static/index.html`, `frontend/static/app.js`, `frontend/static/styles.css`
- Add legislature toggle buttons (US Congress / Illinois GA)
- Session selector for Illinois (104th = 2025-2026)
- Same table structure, different data source
- Update table headers for Illinois (District instead of State)

### Step 5: Comprehensive Testing
**File**: `backend/tests/test_illinois.py`
- Name normalization tests
- Name matching tests (exact, fuzzy, edge cases)
- XML parsing tests with fixtures
- Public Act detection tests
- Database operation tests
- API endpoint tests
- Integration tests with mocked HTTP

## Name Matching Algorithm
Since ILGA XML identifies sponsors by name only (not unique ID):
1. Remove title prefix (Rep., Sen.)
2. Remove suffix (Jr., Sr., II, III)
3. Normalize case and whitespace
4. Build lookup with multiple name variants per member
5. Match using exact → normalized → fuzzy fallback
6. Log unmatched sponsors for debugging

## Database Schema

```sql
-- IL Legislators
CREATE TABLE il_legislators (
    member_id TEXT PRIMARY KEY,  -- "104-house-1"
    ga_session INTEGER NOT NULL,
    chamber TEXT NOT NULL,
    district INTEGER NOT NULL,
    name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    party TEXT,
    updated_at INTEGER
);

-- IL Bills
CREATE TABLE il_bills (
    bill_id TEXT PRIMARY KEY,  -- "104-hb-1"
    ga_session INTEGER NOT NULL,
    bill_type TEXT NOT NULL,
    bill_number INTEGER NOT NULL,
    sponsor_member_id TEXT,
    sponsor_name_raw TEXT,
    title TEXT,
    synopsis TEXT,
    public_act_number TEXT,
    updated_at INTEGER
);

-- IL Laws
CREATE TABLE il_laws (
    law_id TEXT PRIMARY KEY,  -- "104-0001"
    ga_session INTEGER NOT NULL,
    public_act_number TEXT NOT NULL,
    bill_id TEXT,
    sponsor_member_id TEXT,
    updated_at INTEGER
);
```

## LegiScan Cross-Checking
**Recommendation**: Not necessary for MVP
- ILGA FTP is authoritative primary source
- Name matching algorithm should achieve >95% match rate
- Can add LegiScan validation layer later if needed

## Local Testing Instructions

### 1. Install Dependencies
```bash
cd /Users/devin/congress-bill-stats/backend
pip install -r requirements.txt
```

### 2. Run Tests
```bash
# Run all tests
pytest -v

# Run only Illinois tests
pytest tests/test_illinois.py -v

# Run only Congress tests
pytest tests/test_main.py -v
```

### 3. Start the Server
```bash
# Set your Congress API key (required for Congress data, not for Illinois)
export CONGRESS_API_KEY="your-api-key-here"

# Start the server
python main.py
# or
uvicorn main:app --reload --port 8000
```

### 4. Test the Application
Open http://localhost:8000 in your browser.

**Test Congress functionality:**
- Default view shows US Congress toggle active
- Enter a Congress number (e.g., 119) and click "Load / Refresh"
- Verify legislators, bills sponsored, and laws enacted are displayed

**Test Illinois functionality:**
- Click "Illinois GA" toggle button
- Select a session (104th = 2025-2026)
- Click "Load / Refresh" (first load takes several minutes as XML files are fetched)
- Verify Illinois legislators with bills sponsored and enacted are displayed

**Test toggle behavior:**
- Switch between Congress and Illinois - data should reload
- Table headers should change (State vs District)
- Export CSV should export correct data format

### 5. API Endpoints
```bash
# Congress endpoints
curl http://localhost:8000/api/stats?congress=119
curl http://localhost:8000/api/refresh-status?congress=119

# Illinois endpoints
curl http://localhost:8000/api/il-stats?session=104
curl http://localhost:8000/api/il-refresh-status?session=104
curl http://localhost:8000/api/il-sessions

# Health check
curl http://localhost:8000/health
```

## Configuration
```bash
IL_FTP_ROOT=https://ilga.gov/ftp
IL_CACHE_DIR=./cache/illinois
IL_MAX_WORKERS=4
DEFAULT_IL_SESSION=104
```
