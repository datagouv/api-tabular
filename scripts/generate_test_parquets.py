#!/usr/bin/env python3
"""Regenerate db/parquet/*.parquet fixtures from db/initdb/0-init.sql COPY blocks."""

from __future__ import annotations

import csv
import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def parse_cols(cols_raw: str) -> list[str]:
    cols: list[str] = []
    i = 0
    s = cols_raw.strip()
    while i < len(s):
        if s[i] in " \t\n,":
            i += 1
            continue
        if s[i] == '"':
            i += 1
            buf: list[str] = []
            while i < len(s):
                if s[i] == '"' and i + 1 < len(s) and s[i + 1] == '"':
                    buf.append('"')
                    i += 2
                elif s[i] == '"':
                    i += 1
                    break
                else:
                    buf.append(s[i])
                    i += 1
            cols.append("".join(buf))
        else:
            m = re.match(r"[^\s,]+", s[i:])
            assert m is not None
            cols.append(m.group(0))
            i += len(m.group(0))
    return cols


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main() -> None:
    sql = (ROOT / "db/initdb/0-init.sql").read_text()
    tables_index = {
        row["parsing_table"]: row["resource_id"]
        for row in csv.DictReader(open(ROOT / "db/tables_index.csv"))
    }
    out_dir = ROOT / "db" / "parquet"
    out_dir.mkdir(exist_ok=True)
    for path in out_dir.glob("*.parquet"):
        path.unlink()

    con = duckdb.connect()
    for m in re.finditer(r'COPY "csvapi"\.(\w+) \((.*)\) FROM stdin;\n', sql):
        table = m.group(1)
        cols_raw = m.group(2)
        start = m.end()
        end = sql.find("\\.\n", start)
        data = sql[start:end]
        resource_id = tables_index.get(table)
        if not resource_id:
            continue
        cols = parse_cols(cols_raw)
        rows = []
        for line in data.split("\n"):
            if not line.strip():
                continue
            values = line.split("\t")
            if len(values) != len(cols):
                raise ValueError(f"{table}: column/value mismatch")
            rows.append({c: (None if v == "\\N" else v) for c, v in zip(cols, values)})

        path = out_dir / f"{resource_id}.parquet"
        con.execute("DROP TABLE IF EXISTS tmp_export")
        col_defs = ", ".join(f"{qident(c)} VARCHAR" for c in cols)
        con.execute(f"CREATE TABLE tmp_export ({col_defs})")
        placeholders = ", ".join(["?"] * len(cols))
        quoted_cols = ", ".join(qident(c) for c in cols)
        for row in rows:
            con.execute(
                f"INSERT INTO tmp_export ({quoted_cols}) VALUES ({placeholders})",
                [row[c] for c in cols],
            )

        select_parts = []
        for c in cols:
            q = qident(c)
            if c == "__id":
                select_parts.append(f"CAST({q} AS INTEGER) AS {q}")
            elif c == "score":
                select_parts.append(f"TRY_CAST({q} AS DOUBLE) AS {q}")
            elif c == "decompte":
                select_parts.append(f"TRY_CAST({q} AS INTEGER) AS {q}")
            elif c == "is_true":
                select_parts.append(
                    f"CASE WHEN lower(cast({q} AS VARCHAR)) IN ('t','true','1') THEN true "
                    f"WHEN lower(cast({q} AS VARCHAR)) IN ('f','false','0') THEN false "
                    f"ELSE NULL END AS {q}"
                )
            else:
                select_parts.append(q)
        con.execute(
            f"CREATE OR REPLACE TABLE tmp_export AS SELECT {', '.join(select_parts)} FROM tmp_export"
        )
        con.execute(f"COPY tmp_export TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        count_row = con.execute("SELECT COUNT(*) FROM tmp_export").fetchone()
        n = 0 if count_row is None else count_row[0]
        print(f"wrote {path.name} ({n} rows)")


if __name__ == "__main__":
    main()
