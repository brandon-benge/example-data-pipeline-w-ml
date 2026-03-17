from __future__ import annotations

import csv
import io
import os
import subprocess
from typing import Any

from ml.features import PROJECT_ROOT


def run_trino_query(sql: str) -> list[dict[str, str]]:
    result = _run_trino(sql)
    return list(csv.DictReader(io.StringIO(result), delimiter="\t"))


def run_trino_statement(sql: str) -> None:
    _run_trino(sql)


def _run_trino(sql: str) -> str:
    if _should_use_python_client():
        try:
            return _run_trino_python(sql)
        except Exception:
            pass

    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "trino",
        "trino",
        "--server",
        "http://localhost:8080",
        "--output-format",
        "TSV_HEADER",
        "--execute",
        " ".join(line.strip() for line in sql.strip().splitlines()),
    ]
    result = subprocess.run(command, cwd=PROJECT_ROOT, check=True, capture_output=True, text=True)
    return result.stdout


def _should_use_python_client() -> bool:
    return bool(os.getenv("TRINO_HOST"))


def _run_trino_python(sql: str) -> str:
    from trino.dbapi import connect  # type: ignore

    connection = connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "admin"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema=os.getenv("TRINO_SCHEMA", "silver"),
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "http"),
    )
    try:
        cursor = connection.cursor()
        cursor.execute(" ".join(line.strip() for line in sql.strip().splitlines()))
        if cursor.description is None:
            return ""
        rows = cursor.fetchall()
        headers = [column[0] for column in cursor.description]
        output = io.StringIO()
        writer = csv.writer(output, delimiter="\t", lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)
        return output.getvalue()
    finally:
        connection.close()


def sql_literal(value: Any) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)
