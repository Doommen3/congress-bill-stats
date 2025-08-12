# Congress Bill Stats

A small FastAPI + vanilla JS web app that lists **how many bills each legislator sponsored** in a given Congress and **how many became law**, sorted high → low. It uses the official **Congress.gov API**.

> First load for a Congress pulls all bills (pagination) and can take 1–3 minutes; results are cached on disk.

## Quickstart (Local)

1) Python env & deps

```bash
cd backend
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

2) Set your API key

- Get a key from Data.gov (works for Congress.gov API).
- Copy `.env.example` to `.env` and fill in `CONGRESS_API_KEY`

3) Run

```bash
uvicorn main:app --reload
```

4) Open the UI at http://localhost:8000

## Deploy (Render example)

- Push this folder to GitHub.
- Create a new **Web Service** on Render → Build Command:
  `pip install -r backend/requirements.txt`
- Start Command:
  `uvicorn backend.main:app --host 0.0.0.0 --port $PORT`
- Set environment variables:
  - `CONGRESS_API_KEY=...`
  - `DEFAULT_CONGRESS=119` (optional)
  - `CONGRESS_API_ROOT=https://api.data.gov/congress/v3` (optional)
- Add a persistent disk if you want the cache to survive restarts, or remove caching in `main.py`.

## Notes

- "Became Law" is computed from **action codes** in `latestAction.actionCode`, using Congress.gov’s values for public/private law:
  36000–40000 (public law) and 41000–45000 (private law).
- Counting "passed both chambers" is possible by scanning each bill’s full action history (slower).

## License

MIT
