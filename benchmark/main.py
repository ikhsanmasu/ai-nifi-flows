"""
Benchmark sederhana Postgres vs ClickHouse
==========================================

Dependency:
    pip install psycopg2-binary clickhouse-connect python-dotenv

Konfigurasi lewat environment variable (opsional):
    PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD
    CH_HOST, CH_PORT, CH_DB, CH_USER, CH_PASSWORD
    BENCH_REPEATS

Default:
    PG: host=localhost, port=5432, db=postgres, user=postgres, password=postgres
    CH: host=localhost, port=8123 (HTTP), db=default,
        user=admin, password=adminclickhousemaxmar123
"""

import os
import time
import statistics as stats
from typing import List, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from clickhouse_connect import get_client
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIG
# =============================================================================

# Postgres config
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

# ClickHouse config (sesuai docker-compose)
CH_HOST = os.getenv("CH_HOST", "localhost")      # dari host: localhost, dari container lain: 'clickhouse'
CH_PORT = int(os.getenv("CH_PORT", "8123"))      # HTTP interface (clickhouse-connect pakai HTTP)
CH_DB = os.getenv("CH_DB", "default")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "adminclickhousemaxmar123")

# Jumlah pengulangan benchmark
REPEATS = int(os.getenv("BENCH_REPEATS", "5"))


# =============================================================================
# CONTOH QUERY
# =============================================================================
# Silakan kamu modifikasi sesuai kebutuhan (tanggal, id, LIMIT, dll).

# 1) Query analitik: group by tanggal + grade_id (ringan, LIMIT 1000)
QUERY_PG_ANALYTIC = """
SELECT
    date_trunc('day', tanggal) AS tgl,
    grade_id,
    count(*) AS cnt
FROM cultivation_anco_detail               -- Postgres table
WHERE tanggal >= TIMESTAMP '2024-01-01'
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 1000;
"""

# ClickHouse RAW (tanpa FINAL, semua versi/history ikut ke-count)
QUERY_CH_ANALYTIC_RAW = """
SELECT
    toDate(tanggal) AS tgl,
    grade_id,
    count(*) AS cnt
FROM cultivation.cultivation_anco_detail   -- ClickHouse table
WHERE tanggal >= toDateTime64('2024-01-01 00:00:00', 6)
GROUP BY tgl, grade_id
ORDER BY tgl, grade_id
LIMIT 1000
"""

# ClickHouse FINAL (snapshot “latest per id” berdasarkan ReplacingMergeTree(lsn))
QUERY_CH_ANALYTIC_FINAL = """
SELECT
    toDate(tanggal) AS tgl,
    grade_id,
    count(*) AS cnt
FROM cultivation.cultivation_anco_detail FINAL
WHERE tanggal >= toDateTime64('2024-01-01 00:00:00', 6)
GROUP BY tgl, grade_id
ORDER BY tgl, grade_id
LIMIT 1000
"""

# 2) Query point lookup per id (lihat history satu id)
DEFAULT_TEST_ID = 12345  # ganti dengan id yang pasti ada kalau mau hasil rows != 0

# Postgres: lookup by id (tanpa ORDER BY lsn)
QUERY_PG_BY_ID = f"""
SELECT *
FROM cultivation_anco_detail
WHERE id = {DEFAULT_TEST_ID}
LIMIT 10;
"""

# ClickHouse: samakan semantik, juga tanpa ORDER BY lsn
QUERY_CH_BY_ID = f"""
SELECT *
FROM cultivation.cultivation_anco_detail
WHERE id = {DEFAULT_TEST_ID}
LIMIT 10
"""

# 3) FULL COUNT / HEAVY SCAN (ini tipe kerjaan ClickHouse)
#    Jika tabel sudah jutaan row, di sini ClickHouse biasanya mulai kelihatan unggul.
QUERY_PG_COUNT_ALL = """
SELECT count(*) AS cnt
FROM cultivation_anco_detail;
"""

QUERY_CH_COUNT_ALL = """
SELECT count() AS cnt
FROM cultivation.cultivation_anco_detail
"""

# 4) HEAVY ANALYTIC (tanpa LIMIT, range tanggal lebar, agregasi lebih banyak)
#    Sesuaikan tanggal supaya banyak data yang kena.
QUERY_PG_HEAVY_ANALYTIC = """
SELECT
    date_trunc('day', tanggal) AS tgl,
    grade_id,
    status,
    count(*) AS cnt,
    avg(EXTRACT(EPOCH FROM created_at)) AS avg_created_epoch,
    min(created_at) AS min_created_at,
    max(created_at) AS max_created_at
FROM cultivation_anco_detail
WHERE tanggal >= TIMESTAMP '2023-01-01'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
"""

QUERY_CH_HEAVY_ANALYTIC = """
SELECT
    toDate(tanggal) AS tgl,
    grade_id,
    status,
    count(*) AS cnt,
    avg(toUnixTimestamp64Micro(created_at)) AS avg_created_epoch,
    min(created_at) AS min_created_at,
    max(created_at) AS max_created_at
FROM cultivation.cultivation_anco_detail
WHERE tanggal >= toDateTime64('2023-01-01 00:00:00', 6)
GROUP BY tgl, grade_id, status
ORDER BY tgl, grade_id, status
"""


# =============================================================================
# UTIL
# =============================================================================

def format_stats(times_ms: List[float]) -> str:
    if not times_ms:
        return "no data"
    return (
        f"min={min(times_ms):.2f} ms, "
        f"max={max(times_ms):.2f} ms, "
        f"mean={stats.mean(times_ms):.2f} ms, "
        f"median={stats.median(times_ms):.2f} ms"
    )


# =============================================================================
# POSTGRES BENCHMARK
# =============================================================================

def bench_postgres(query: str, repeats: int = REPEATS) -> Tuple[List[float], int]:
    """
    Jalankan query di Postgres beberapa kali dan kembalikan list waktu (ms) + jumlah row
    dari eksekusi terakhir.
    """
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    times_ms: List[float] = []
    last_rows = 0

    try:
        # Warmup ringan (opsional) - tidak diukur
        cursor.execute("SELECT 1;")
        cursor.fetchall()

        for i in range(repeats):
            t0 = time.perf_counter()
            cursor.execute(query)
            rows = cursor.fetchall()
            t1 = time.perf_counter()

            elapsed_ms = (t1 - t0) * 1000
            times_ms.append(elapsed_ms)
            last_rows = len(rows)
            print(f"[PG] Run {i + 1}/{repeats}: {elapsed_ms:.2f} ms, rows={len(rows)}")

    finally:
        cursor.close()
        conn.close()

    return times_ms, last_rows


# =============================================================================
# CLICKHOUSE BENCHMARK
# =============================================================================

def bench_clickhouse(query: str, repeats: int = REPEATS) -> Tuple[List[float], int]:
    """
    Jalankan query di ClickHouse beberapa kali dan kembalikan list waktu (ms) + jumlah row
    dari eksekusi terakhir.
    """
    client = get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
        interface="http",  # eksplisit pakai HTTP di port 8123
    )

    times_ms: List[float] = []
    last_rows = 0

    # Bersihkan ; terakhir supaya tidak dianggap multi-statement
    query_clean = query.strip().rstrip(";")

    # Warmup ringan (opsional) - tidak diukur
    _ = client.query("SELECT 1")

    for i in range(repeats):
        t0 = time.perf_counter()
        result = client.query(query_clean)
        rows = result.result_rows
        t1 = time.perf_counter()

        elapsed_ms = (t1 - t0) * 1000
        times_ms.append(elapsed_ms)
        last_rows = len(rows)
        print(f"[CH] Run {i + 1}/{repeats}: {elapsed_ms:.2f} ms, rows={len(rows)}")

    return times_ms, last_rows


# =============================================================================
# HIGH LEVEL COMPARISON
# =============================================================================

def compare_query(label: str, query_pg: str, query_ch: str, repeats: int = REPEATS) -> None:
    print("=" * 80)
    print(f"BENCHMARK: {label}")
    print("=" * 80)

    print("\n== Postgres ==")
    pg_times, pg_rows = bench_postgres(query_pg, repeats=repeats)

    print("\n== ClickHouse ==")
    ch_times, ch_rows = bench_clickhouse(query_ch, repeats=repeats)

    print("\n-- Summary --")
    print(f"Postgres:   rows={pg_rows}, {format_stats(pg_times)}")
    print(f"ClickHouse: rows={ch_rows}, {format_stats(ch_times)}")
    print()


def main():
    print("Postgres config:")
    print(f"  host={PG_HOST} port={PG_PORT} db={PG_DB} user={PG_USER}")
    print("ClickHouse config:")
    print(f"  host={CH_HOST} port={CH_PORT} db={CH_DB} user={CH_USER}")
    print(f"\nRepeats per query: {REPEATS}\n")

    # 1) Analytic ringan: PG snapshot vs ClickHouse RAW
    compare_query(
        label="Analytic by tanggal + grade_id (ClickHouse RAW, LIMIT 1000)",
        query_pg=QUERY_PG_ANALYTIC,
        query_ch=QUERY_CH_ANALYTIC_RAW,
        repeats=REPEATS,
    )

    # 2) Analytic ringan: PG snapshot vs ClickHouse FINAL
    compare_query(
        label="Analytic by tanggal + grade_id (ClickHouse FINAL, LIMIT 1000)",
        query_pg=QUERY_PG_ANALYTIC,
        query_ch=QUERY_CH_ANALYTIC_FINAL,
        repeats=REPEATS,
    )

    # 3) Point lookup per id
    compare_query(
        label=f"Point lookup by id={DEFAULT_TEST_ID}",
        query_pg=QUERY_PG_BY_ID,
        query_ch=QUERY_CH_BY_ID,
        repeats=REPEATS,
    )

    # 4) Full COUNT(*) (scan besar)
    compare_query(
        label="Full table COUNT(*)",
        query_pg=QUERY_PG_COUNT_ALL,
        query_ch=QUERY_CH_COUNT_ALL,
        repeats=REPEATS,
    )

    # 5) Heavy analytic (tanpa LIMIT, rentang tanggal lebar, agregasi lebih berat)
    compare_query(
        label="Heavy analytic (no LIMIT, wide date range, extra aggregates)",
        query_pg=QUERY_PG_HEAVY_ANALYTIC,
        query_ch=QUERY_CH_HEAVY_ANALYTIC,
        repeats=REPEATS,
    )


if __name__ == "__main__":
    main()
