from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from superset.app import create_app
from superset.extensions import db


BI_ROOT = Path("/app/bi")
DATASETS_DIR = BI_ROOT / "datasets"
QUERIES_DIR = BI_ROOT / "queries"
CHARTS_DIR = BI_ROOT / "charts"
DASHBOARDS_DIR = BI_ROOT / "dashboards"
DATABASE_NAME = "trino_iceberg"
DEFAULT_SCHEMA = "gold"
app = create_app()

from superset.models.core import Database
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice

try:
    from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
except Exception as exc:  # pragma: no cover - container-only bootstrap path
    raise RuntimeError("Superset SQLA models are unavailable in this image") from exc

try:  # pragma: no cover - import path varies by Superset release
    from superset.models.sql_lab import SavedQuery
except Exception:  # pragma: no cover - fallback for other package layouts
    SavedQuery = None  # type: ignore[assignment]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _admin_user():
    from flask_appbuilder.security.sqla.models import User

    username = os.environ["SUPERSET_ADMIN_USERNAME"]
    return db.session.query(User).filter_by(username=username).one()


def _metric_payload(metric_name: str, metric_expression: str) -> dict[str, Any]:
    return {
        "expressionType": "SQL",
        "sqlExpression": metric_expression,
        "label": metric_name,
        "optionName": metric_name,
    }


def _query_context(dataset_id: int, dataset_type: str, viz_type: str, metrics: list[dict[str, Any]], groupby: list[str], x_axis: str | None, row_limit: int) -> str:
    form_data = {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz_type,
        "metrics": metrics,
        "groupby": groupby,
        "columns": groupby,
        "adhoc_filters": [],
        "row_limit": row_limit,
    }
    if x_axis:
        form_data["granularity_sqla"] = x_axis
        form_data["x_axis"] = x_axis
        form_data["time_grain_sqla"] = "P1D"
    return json.dumps(
        {
            "datasource": {"id": dataset_id, "type": dataset_type},
            "queries": [form_data],
            "form_data": form_data,
            "result_type": "results",
            "result_format": "json",
        }
    )


def _chart_params(dataset_id: int, viz_type: str, metrics: list[dict[str, Any]], groupby: list[str], x_axis: str | None, row_limit: int) -> str:
    payload: dict[str, Any] = {
        "datasource": f"{dataset_id}__table",
        "viz_type": viz_type,
        "metrics": metrics,
        "groupby": groupby,
        "all_columns": groupby,
        "adhoc_filters": [],
        "row_limit": row_limit,
        "show_legend": True,
    }
    if viz_type == "table":
        payload["columns"] = groupby
    if x_axis:
        payload["granularity_sqla"] = x_axis
        payload["x_axis"] = x_axis
        payload["time_grain_sqla"] = "P1D"
    return json.dumps(payload)


def _dashboard_layout(title: str, charts: list[Slice]) -> str:
    layout: dict[str, Any] = {
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "children": ["HEADER_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
    }
    row_ids: list[str] = []
    for row_index in range(0, len(charts), 2):
        row_id = f"ROW-{row_index // 2 + 1}"
        row_ids.append(row_id)
        row_children: list[str] = []
        for col_index, chart in enumerate(charts[row_index : row_index + 2], start=1):
            chart_id = f"CHART-{row_index + col_index}"
            row_children.append(chart_id)
            layout[chart_id] = {
                "id": chart_id,
                "type": "CHART",
                "children": [],
                "meta": {
                    "chartId": chart.id,
                    "sliceName": chart.slice_name,
                    "width": 6,
                    "height": 50,
                },
            }
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "children": row_children,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
    layout["GRID_ID"]["children"].extend(row_ids)
    return json.dumps(layout)


def _upsert_dataset(database: Database, owner, payload: dict[str, Any]) -> SqlaTable:
    sql = None
    if payload.get("sql_file"):
        sql = (QUERIES_DIR / payload["sql_file"]).read_text(encoding="utf-8")
    table_name = payload.get("table_name") or payload["slug"]
    schema = payload.get("schema", DEFAULT_SCHEMA)

    dataset = (
        db.session.query(SqlaTable)
        .filter_by(database_id=database.id, table_name=table_name, schema=schema)
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(table_name=table_name, schema=schema, database=database)
        db.session.add(dataset)
        db.session.flush()

    dataset.sql = sql
    dataset.description = payload.get("description")
    dataset.is_sqllab_view = bool(sql)
    dataset.owners = [owner]
    db.session.flush()

    existing_columns = {column.column_name: column for column in dataset.columns}
    for column_def in payload.get("columns", []):
        column = existing_columns.get(column_def["name"])
        if column is None:
            column = TableColumn(column_name=column_def["name"], table=dataset)
            db.session.add(column)
        column.type = column_def.get("type", "STRING")
        column.is_dttm = bool(column_def.get("is_dttm", False))
        column.filterable = bool(column_def.get("filterable", True))
        column.groupby = bool(column_def.get("groupby", not column.is_dttm))
        column.expression = column_def.get("expression")

    existing_metrics = {metric.metric_name: metric for metric in dataset.metrics}
    for metric_def in payload.get("metrics", []):
        metric = existing_metrics.get(metric_def["name"])
        if metric is None:
            metric = SqlMetric(metric_name=metric_def["name"], table=dataset)
            db.session.add(metric)
        metric.expression = metric_def["expression"]
        metric.metric_type = "expression"
        metric.description = metric_def.get("description")
        metric.d3format = metric_def.get("d3format")

    db.session.flush()
    return dataset


def _upsert_saved_query(database: Database, owner, payload: dict[str, Any]) -> None:
    if SavedQuery is None:
        return
    sql = (QUERIES_DIR / payload["sql_file"]).read_text(encoding="utf-8")
    saved_query = db.session.query(SavedQuery).filter_by(db_id=database.id, label=payload["title"]).one_or_none()
    if saved_query is None:
        saved_query = SavedQuery(db_id=database.id, user_id=owner.id, label=payload["title"])
        db.session.add(saved_query)
    saved_query.schema = payload.get("schema", DEFAULT_SCHEMA)
    saved_query.description = payload.get("description")
    saved_query.sql = sql


def _upsert_chart(dataset_map: dict[str, SqlaTable], owner, payload: dict[str, Any]) -> Slice:
    dataset = dataset_map[payload["dataset_slug"]]
    dataset_metric_map = {metric.metric_name: metric.expression for metric in dataset.metrics}
    chart_metrics = [
        _metric_payload(metric_name, dataset_metric_map.get(metric_name, metric_name))
        for metric_name in payload.get("metrics", [])
    ]
    chart = db.session.query(Slice).filter_by(slice_name=payload["title"]).one_or_none()
    if chart is None:
        chart = Slice(slice_name=payload["title"])
        db.session.add(chart)
    chart.datasource_id = dataset.id
    chart.datasource_type = "table"
    chart.viz_type = payload["viz_type"]
    chart.description = payload.get("description")
    chart.params = _chart_params(
        dataset.id,
        payload["viz_type"],
        chart_metrics,
        payload.get("groupby", []),
        payload.get("x_axis"),
        int(payload.get("row_limit", 1000)),
    )
    if hasattr(chart, "query_context"):
        chart.query_context = _query_context(
            dataset.id,
            "table",
            payload["viz_type"],
            chart_metrics,
            payload.get("groupby", []),
            payload.get("x_axis"),
            int(payload.get("row_limit", 1000)),
        )
    chart.owners = [owner]
    db.session.flush()
    return chart


def _upsert_dashboard(owner, chart_map: dict[str, Slice], payload: dict[str, Any]) -> None:
    dashboard = db.session.query(Dashboard).filter_by(dashboard_title=payload["title"]).one_or_none()
    if dashboard is None:
        dashboard = Dashboard(dashboard_title=payload["title"])
        db.session.add(dashboard)
    if hasattr(dashboard, "slug"):
        dashboard.slug = payload["slug"]
    if hasattr(dashboard, "published"):
        dashboard.published = True
    dashboard.owners = [owner]
    charts = [chart_map[slug] for slug in payload.get("chart_slugs", [])]
    dashboard.slices = charts
    dashboard.position_json = _dashboard_layout(payload["title"], charts)
    dashboard.json_metadata = json.dumps({"native_filter_configuration": [], "timed_refresh_immune_slices": []})


def main() -> None:
    with app.app_context():
        database = db.session.query(Database).filter_by(database_name=DATABASE_NAME).one()
        owner = _admin_user()

        dataset_map: dict[str, SqlaTable] = {}
        for path in sorted(DATASETS_DIR.glob("*.json")):
            payload = _load_json(path)
            dataset_map[payload["slug"]] = _upsert_dataset(database, owner, payload)

        for path in sorted(QUERIES_DIR.glob("*.json")):
            _upsert_saved_query(database, owner, _load_json(path))

        chart_map: dict[str, Slice] = {}
        for path in sorted(CHARTS_DIR.glob("*.json")):
            payload = _load_json(path)
            chart_map[payload["slug"]] = _upsert_chart(dataset_map, owner, payload)

        for path in sorted(DASHBOARDS_DIR.glob("*.json")):
            _upsert_dashboard(owner, chart_map, _load_json(path))

        db.session.commit()
        print("Versioned BI assets synchronized.")


if __name__ == "__main__":
    main()
