#/bin/bash
DATA_DIR="${REPLICANT_DATA_DIR:-$HOME/.local/share/replicant}"

echo "=== replicantd configured path ==="
docker compose exec replicantd sh -c '
  echo "REPLICANT_TELEMETRY_DB=$REPLICANT_TELEMETRY_DB"
  find /var/lib/replicant -maxdepth 2 -type f -name "*telemetry*.sqlite*" -printf "%p  %s bytes\n" 2>/dev/null || true
'

echo
echo "=== telemetry DB contents on host ==="
python - "$DATA_DIR" <<'PY'
import os
import sqlite3
import sys

root = sys.argv[1]
candidates = [
    os.path.join(root, "telemetry", "replicant-telemetry.sqlite"),
    os.path.join(root, "replicant-telemetry.sqlite"),
]

for path in candidates:
    print(f"\n{path}")
    if not os.path.exists(path):
        print("  DOES NOT EXIST")
        continue

    print(f"  size: {os.path.getsize(path)} bytes")

    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        for table in ("telemetry_meta", "api_request_attempt", "api_request_rollup"):
            try:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table}: {count}")
            except Exception as e:
                print(f"  {table}: ERROR: {e}")

        try:
            print("  meta:")
            for row in con.execute("SELECT key, value FROM telemetry_meta ORDER BY key"):
                print("   ", row)
        except Exception:
            pass

        try:
            row = con.execute("""
                SELECT
                    MIN(observed_at_ms),
                    MAX(observed_at_ms),
                    COUNT(*)
                FROM api_request_attempt
            """).fetchone()
            print("  raw range:", row)
        except Exception:
            pass
    finally:
        con.close()
PY

echo
echo "=== telemetry-related daemon log ==="
docker compose exec replicantd sh -c '
  grep -Ei "telemetry|startup configuration" \
    /var/lib/replicant/logs/replicantd.log | tail -80
'