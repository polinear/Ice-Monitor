import json
import uuid
from textwrap import dedent

from superset.app import create_app


DATASETS = [
    {
        "table_name": "ice_system_metrics",
        "schema": "public",
        "main_dttm_col": "ts",
        "description": "Technical runtime metrics written by the API into TimescaleDB.",
    },
    {
        "table_name": "ice_metrics",
        "schema": "public",
        "main_dttm_col": "ts",
        "description": "Model output metrics written by the API into TimescaleDB.",
    },
]

WHITE_DASHBOARD_CSS = dedent(
    """
    .dashboard-content,
    .dashboard,
    .grid-container,
    .dashboard-page,
    .dashboard-header,
    body {
        background:
            radial-gradient(1000px 420px at 92% -8%, rgba(58, 150, 255, 0.08), transparent 55%),
            radial-gradient(840px 360px at 0% 0%, rgba(56, 189, 248, 0.09), transparent 52%),
            linear-gradient(180deg, #fbfdff 0%, #f4f8fc 100%) !important;
        color: #102033 !important;
    }

    .dashboard-component-chart-holder,
    .chart-container,
    .slice-container,
    .dashboard-component-chart-holder > div,
    .dashboard-component-root,
    .dashboard-component,
    .dragdroppable-row {
        background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(246, 250, 254, 0.96)) !important;
        border: 1px solid rgba(104, 127, 153, 0.16) !important;
        border-radius: 18px !important;
        box-shadow: 0 16px 36px rgba(15, 23, 42, 0.06) !important;
    }

    .dashboard-component-header,
    .dashboard-component-title,
    .header-title,
    .dashboard-component-chart-holder .header,
    .chart-title {
        color: #102033 !important;
        letter-spacing: 0.01em;
    }

    .dashboard-component-chart-holder .header-subtitle,
    .dashboard-component-chart-holder .header-time,
    .text-muted,
    .ant-typography-secondary {
        color: #5c6f86 !important;
    }

    .big-number-total .header-title,
    .big-number-total .big-number {
        color: #0f172a !important;
    }
    """
).strip()

TECHNICAL_DASHBOARD = {
    "title": "Модель и хранилище",
    "slug": "ice-monitor-model-storage",
    "legacy_slugs": ["ice-monitor-bi"],
    "css": WHITE_DASHBOARD_CSS,
}

ICE_DASHBOARD = {
    "title": "Ледовая аналитика",
    "slug": "ice-monitor-ice-analytics",
    "legacy_slugs": ["ice-monitor-model-ops"],
    "css": WHITE_DASHBOARD_CSS,
}


def sql_metric(label, sql_expression):
    return {
        "label": label,
        "expressionType": "SQL",
        "sqlExpression": sql_expression,
    }


def simple_metric(label, column_name, aggregate):
    return {
        "label": label,
        "expressionType": "SIMPLE",
        "column": {"column_name": column_name},
        "aggregate": aggregate,
    }


def json_numeric_expr(key):
    return f"COALESCE(((pixels_json::jsonb ->> '{key}')::numeric), 0)"


def ice_core_expr():
    return (
        f"({json_numeric_expr('ice_field')} + "
        f"{json_numeric_expr('broken_ice')} + "
        f"{json_numeric_expr('slush_ice')})"
    )


def ice_scene_expr():
    return f"({ice_core_expr()} + {json_numeric_expr('open_water')})"

TECHNICAL_CHARTS = [
    {
        "slice_name": "Здоровье сервисов",
        "dataset_name": "ice_system_metrics",
        "viz_type": "big_number_total",
        "description": "Average percentage of healthy services across snapshots.",
        "params": {
            "viz_type": "big_number_total",
            "metric": sql_metric(
                "Доля здоровых сервисов, %",
                "AVG(100.0 * healthy_services / NULLIF(total_services, 0))",
            ),
            "time_range": "No filter",
            "row_limit": 1,
            "granularity_sqla": "ts",
            "time_grain_sqla": "PT1M",
            "extra_form_data": {},
        },
        "layout": {"row": "ROW-KPI", "width": 3, "height": 40},
    },
    {
        "slice_name": "Средний лаг обработки",
        "dataset_name": "ice_system_metrics",
        "viz_type": "big_number_total",
        "description": "Average stream lag in seconds across snapshots.",
        "params": {
            "viz_type": "big_number_total",
            "metric": sql_metric(
                "Средний лаг, сек",
                "AVG(COALESCE(result_lag_sec, frame_lag_sec))",
            ),
            "time_range": "No filter",
            "row_limit": 1,
            "granularity_sqla": "ts",
            "time_grain_sqla": "PT1M",
            "extra_form_data": {},
        },
        "layout": {"row": "ROW-KPI", "width": 3, "height": 40},
    },
    {
        "slice_name": "Результатов в БД",
        "dataset_name": "ice_system_metrics",
        "viz_type": "big_number_total",
        "description": "Total number of model results stored in TimescaleDB.",
        "params": {
            "viz_type": "big_number_total",
            "metric": sql_metric("Записей в БД", "MAX(db_rows)"),
            "time_range": "No filter",
            "row_limit": 1,
            "granularity_sqla": "ts",
            "time_grain_sqla": "PT1M",
            "extra_form_data": {},
        },
        "layout": {"row": "ROW-KPI", "width": 3, "height": 40},
    },
    {
        "slice_name": "Техзамеров в БД",
        "dataset_name": "ice_system_metrics",
        "viz_type": "big_number_total",
        "description": "Total number of system snapshots stored in TimescaleDB.",
        "params": {
            "viz_type": "big_number_total",
            "metric": sql_metric("Снимков в БД", "COUNT(*)"),
            "time_range": "No filter",
            "row_limit": 1,
            "granularity_sqla": "ts",
            "time_grain_sqla": "PT1M",
            "extra_form_data": {},
        },
        "layout": {"row": "ROW-KPI", "width": 3, "height": 40},
    },
    {
        "slice_name": "Накопление результатов в БД",
        "dataset_name": "ice_system_metrics",
        "viz_type": "echarts_timeseries_line",
        "description": "How stored model rows grow over time.",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "granularity_sqla": "ts",
            "time_grain_sqla": "PT1M",
            "time_range": "No filter",
            "x_axis": "ts",
            "metrics": [sql_metric("Записей в БД", "MAX(db_rows)")],
            "groupby": ["cam_id"],
            "row_limit": 1000,
            "show_legend": True,
            "line_interpolation": "linear",
            "marker_enabled": True,
            "truncate_metric": True,
            "extra_form_data": {},
        },
        "layout": {"row": "ROW-TRENDS", "width": 6, "height": 64},
    },
    {
        "slice_name": "Здоровье сервисов по времени",
        "dataset_name": "ice_system_metrics",
        "viz_type": "echarts_timeseries_line",
        "description": "Average healthy service ratio over time.",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "granularity_sqla": "ts",
            "time_grain_sqla": "PT1M",
            "time_range": "No filter",
            "x_axis": "ts",
            "metrics": [
                sql_metric(
                    "Доля здоровых сервисов, %",
                    "AVG(100.0 * healthy_services / NULLIF(total_services, 0))",
                )
            ],
            "groupby": ["cam_id"],
            "row_limit": 1000,
            "show_legend": True,
            "line_interpolation": "linear",
            "marker_enabled": True,
            "truncate_metric": True,
            "extra_form_data": {},
        },
        "layout": {"row": "ROW-TRENDS", "width": 6, "height": 64},
    },
    {
        "slice_name": "Лаг обработки по камерам",
        "dataset_name": "ice_system_metrics",
        "viz_type": "echarts_timeseries_line",
        "description": "Average result or frame lag by camera.",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "granularity_sqla": "ts",
            "time_grain_sqla": "PT1M",
            "time_range": "No filter",
            "x_axis": "ts",
            "metrics": [
                sql_metric(
                    "Средний лаг, сек",
                    "AVG(COALESCE(result_lag_sec, frame_lag_sec))",
                )
            ],
            "groupby": ["cam_id"],
            "row_limit": 1000,
            "show_legend": True,
            "line_interpolation": "linear",
            "marker_enabled": True,
            "truncate_metric": True,
            "extra_form_data": {},
        },
        "layout": {"row": "ROW-TRENDS-2", "width": 6, "height": 64},
    },
    {
        "slice_name": "Очередь Redis по камерам",
        "dataset_name": "ice_system_metrics",
        "viz_type": "echarts_timeseries_line",
        "description": "Current Redis backlog by camera.",
        "params": {
            "viz_type": "echarts_timeseries_line",
            "granularity_sqla": "ts",
            "time_grain_sqla": "PT1M",
            "time_range": "No filter",
            "x_axis": "ts",
            "metrics": [
                sql_metric("Сумма очередей", "MAX(frame_stream_len + result_stream_len)")
            ],
            "groupby": ["cam_id"],
            "row_limit": 1000,
            "show_legend": True,
            "line_interpolation": "linear",
            "marker_enabled": True,
            "truncate_metric": True,
            "extra_form_data": {},
        },
        "layout": {"row": "ROW-TRENDS-2", "width": 6, "height": 64},
    },
]

DASHBOARD_SPECS = [
    {**TECHNICAL_DASHBOARD, "charts": TECHNICAL_CHARTS},
    {
        **ICE_DASHBOARD,
        "charts": [
            {
                "slice_name": "Лёд в кадре, %",
                "dataset_name": "ice_metrics",
                "viz_type": "big_number_total",
                "description": "Average share of all visible ice pixels in the scene.",
                "params": {
                    "viz_type": "big_number_total",
                    "metric": sql_metric(
                        "Лёд в кадре, %",
                        f"AVG(100.0 * ({ice_core_expr()}) / NULLIF(({ice_scene_expr()}), 0))",
                    ),
                    "time_range": "No filter",
                    "row_limit": 1,
                    "granularity_sqla": "ts",
                    "time_grain_sqla": "PT1H",
                    "extra_form_data": {},
                },
                "layout": {"row": "ROW-KPI", "width": 3, "height": 40},
            },
            {
                "slice_name": "Сплошной лёд, %",
                "dataset_name": "ice_metrics",
                "viz_type": "big_number_total",
                "description": "Average share of solid ice inside the ice core.",
                "params": {
                    "viz_type": "big_number_total",
                    "metric": sql_metric(
                        "Сплошной лёд, %",
                        f"AVG(100.0 * {json_numeric_expr('ice_field')} / NULLIF(({ice_core_expr()}), 0))",
                    ),
                    "time_range": "No filter",
                    "row_limit": 1,
                    "granularity_sqla": "ts",
                    "time_grain_sqla": "PT1H",
                    "extra_form_data": {},
                },
                "layout": {"row": "ROW-KPI", "width": 3, "height": 40},
            },
            {
                "slice_name": "Рыхлый лёд, %",
                "dataset_name": "ice_metrics",
                "viz_type": "big_number_total",
                "description": "Average share of broken ice plus slush inside the ice core.",
                "params": {
                    "viz_type": "big_number_total",
                    "metric": sql_metric(
                        "Рыхлый лёд, %",
                        (
                            "AVG(100.0 * ("
                            f"{json_numeric_expr('broken_ice')} + {json_numeric_expr('slush_ice')}"
                            f") / NULLIF(({ice_core_expr()}), 0))"
                        ),
                    ),
                    "time_range": "No filter",
                    "row_limit": 1,
                    "granularity_sqla": "ts",
                    "time_grain_sqla": "PT1H",
                    "extra_form_data": {},
                },
                "layout": {"row": "ROW-KPI", "width": 3, "height": 40},
            },
            {
                "slice_name": "Открытая вода, %",
                "dataset_name": "ice_metrics",
                "viz_type": "big_number_total",
                "description": "Average visible open-water share in the scene.",
                "params": {
                    "viz_type": "big_number_total",
                    "metric": sql_metric(
                        "Открытая вода, %",
                        f"AVG(100.0 * {json_numeric_expr('open_water')} / NULLIF(({ice_scene_expr()}), 0))",
                    ),
                    "time_range": "No filter",
                    "row_limit": 1,
                    "granularity_sqla": "ts",
                    "time_grain_sqla": "PT1H",
                    "extra_form_data": {},
                },
                "layout": {"row": "ROW-KPI", "width": 3, "height": 40},
            },
            {
                "slice_name": "Состав ледового ядра по камерам",
                "dataset_name": "ice_metrics",
                "viz_type": "echarts_timeseries_line",
                "description": "Average composition of the ice core over time.",
                "params": {
                    "viz_type": "echarts_timeseries_line",
                    "granularity_sqla": "ts",
                    "time_grain_sqla": "PT1H",
                    "time_range": "No filter",
                    "x_axis": "ts",
                    "metrics": [
                        sql_metric(
                            "Сплошной лёд, %",
                            f"AVG(100.0 * {json_numeric_expr('ice_field')} / NULLIF(({ice_core_expr()}), 0))",
                        ),
                        sql_metric(
                            "Битый лёд, %",
                            f"AVG(100.0 * {json_numeric_expr('broken_ice')} / NULLIF(({ice_core_expr()}), 0))",
                        ),
                        sql_metric(
                            "Шуга, %",
                            f"AVG(100.0 * {json_numeric_expr('slush_ice')} / NULLIF(({ice_core_expr()}), 0))",
                        ),
                    ],
                    "groupby": ["cam_id"],
                    "row_limit": 1000,
                    "show_legend": True,
                    "line_interpolation": "linear",
                    "marker_enabled": True,
                    "truncate_metric": True,
                    "extra_form_data": {},
                },
                "layout": {"row": "ROW-TRENDS", "width": 12, "height": 64},
            },
            {
                "slice_name": "Лёд и вода по камерам",
                "dataset_name": "ice_metrics",
                "viz_type": "echarts_timeseries_line",
                "description": "Ice cover and open-water share by camera.",
                "params": {
                    "viz_type": "echarts_timeseries_line",
                    "granularity_sqla": "ts",
                    "time_grain_sqla": "PT1H",
                    "time_range": "No filter",
                    "x_axis": "ts",
                    "metrics": [
                        sql_metric(
                            "Лёд в кадре, %",
                            f"AVG(100.0 * ({ice_core_expr()}) / NULLIF(({ice_scene_expr()}), 0))",
                        ),
                        sql_metric(
                            "Открытая вода, %",
                            f"AVG(100.0 * {json_numeric_expr('open_water')} / NULLIF(({ice_scene_expr()}), 0))",
                        ),
                    ],
                    "groupby": ["cam_id"],
                    "row_limit": 1000,
                    "show_legend": True,
                    "line_interpolation": "linear",
                    "marker_enabled": True,
                    "truncate_metric": True,
                    "extra_form_data": {},
                },
                "layout": {"row": "ROW-TRENDS-2", "width": 12, "height": 64},
            },
        ],
    },
]


def compact_json(payload):
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def chart_component_id(slice_obj):
    return f"CHART-{slice_obj.id}"


def build_dashboard_layout(charts):
    row_order = []
    rows = {}
    for chart in charts:
        row_id = chart["layout"]["row"]
        if row_id not in rows:
            rows[row_id] = []
            row_order.append(row_id)
        rows[row_id].append(chart)

    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "id": "ROOT_ID",
            "type": "ROOT",
            "children": ["GRID_ID"],
        },
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": row_order,
        },
    }

    for row_id in row_order:
        row_charts = rows[row_id]
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": [chart_component_id(chart["slice"]) for chart in row_charts],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

        for chart in row_charts:
            component_id = chart_component_id(chart["slice"])
            layout[component_id] = {
                "id": component_id,
                "type": "CHART",
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "children": [],
                "meta": {
                    "chartId": chart["slice"].id,
                    "sliceName": chart["slice"].slice_name,
                    "height": chart["layout"]["height"],
                    "width": chart["layout"]["width"],
                    "uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, component_id)),
                },
            }

    return layout


def ensure_dataset(db_session, database, item, created, updated, datasets):
    from superset.connectors.sqla.models import SqlaTable

    dataset = (
        db_session.query(SqlaTable)
        .filter_by(
            table_name=item["table_name"],
            schema=item["schema"],
            database_id=database.id,
        )
        .one_or_none()
    )

    if dataset is None:
        dataset = SqlaTable(
            table_name=item["table_name"],
            schema=item["schema"],
            database=database,
        )
        db_session.add(dataset)
        db_session.commit()
        created.append(item["table_name"])

    dataset.main_dttm_col = item["main_dttm_col"]
    dataset.description = item["description"]
    dataset.fetch_metadata()
    updated.append(item["table_name"])
    datasets[item["table_name"]] = dataset
    return dataset


def upsert_dashboard(db_session, dashboard_cls, admin_user, spec, datasets, chart_created, chart_updated):
    from superset.models.slice import Slice
    from sqlalchemy import or_

    prepared_charts = []

    for item in spec["charts"]:
        dataset = datasets[item["dataset_name"]]
        params = {
            "datasource": f"{dataset.id}__table",
            **item["params"],
        }
        chart = (
            db_session.query(Slice)
            .filter_by(slice_name=item["slice_name"])
            .one_or_none()
        )
        if chart is None:
            chart = Slice(
                slice_name=item["slice_name"],
                viz_type=item["viz_type"],
                datasource_type="table",
                datasource_id=dataset.id,
            )
            chart_created.append(item["slice_name"])

        chart.viz_type = item["viz_type"]
        chart.datasource_type = "table"
        chart.datasource_id = dataset.id
        chart.params = compact_json(params)
        chart.description = item["description"]
        chart.owners = [admin_user]
        db_session.add(chart)
        db_session.commit()
        chart_updated.append(item["slice_name"])
        prepared_charts.append({**item, "slice": chart})

    slug_filters = [dashboard_cls.slug == spec["slug"]]
    slug_filters.extend(dashboard_cls.slug == slug for slug in spec.get("legacy_slugs", []))
    dashboard = db_session.query(dashboard_cls).filter(or_(*slug_filters)).one_or_none()
    if dashboard is None:
        dashboard = (
            db_session.query(dashboard_cls)
            .filter_by(dashboard_title="[ untitled dashboard ]")
            .one_or_none()
        ) or dashboard_cls()

    dashboard.dashboard_title = spec["title"]
    dashboard.slug = spec["slug"]
    dashboard.published = True
    dashboard.owners = [admin_user]
    dashboard.slices = [item["slice"] for item in prepared_charts]
    dashboard.position_json = compact_json(build_dashboard_layout(prepared_charts))
    dashboard.json_metadata = compact_json(
        {
            "color_namespace": None,
            "label_colors": {},
            "shared_label_colors": {},
            "native_filter_configuration": [],
        }
    )
    if spec.get("css") and hasattr(dashboard, "css"):
        dashboard.css = spec["css"]

    db_session.add(dashboard)
    db_session.commit()
    return dashboard


def main():
    app = create_app()
    with app.app_context():
        from superset.connectors.sqla.models import SqlaTable
        from superset.extensions import db
        from superset.models.dashboard import Dashboard
        from superset.models.core import Database
        from superset.models.slice import Slice
        from flask_appbuilder.security.sqla.models import User

        database = db.session.query(Database).filter_by(
            database_name="Ice Monitor Timescale"
        ).one()
        admin_user = db.session.query(User).filter_by(username="admin").one()

        created = []
        updated = []
        datasets = {}

        for stale_chart in list(db.session.query(Slice).all()):
            if stale_chart.slice_name == "tmp" or stale_chart.slice_name.startswith("__tmp"):
                db.session.delete(stale_chart)
        db.session.commit()

        for item in DATASETS:
            ensure_dataset(db.session, database, item, created, updated, datasets)

        chart_created = []
        chart_updated = []
        dashboards = []

        for spec in DASHBOARD_SPECS:
            dashboards.append(
                upsert_dashboard(
                    db.session,
                    Dashboard,
                    admin_user,
                    spec,
                    datasets,
                    chart_created,
                    chart_updated,
                )
            )

        print(
            "Superset content ready. "
            f"datasets_created={created}, datasets_updated={updated}, "
            f"charts_created={chart_created}, charts_updated={chart_updated}, "
            f"dashboards={[dashboard.dashboard_title for dashboard in dashboards]}",
            flush=True,
        )


if __name__ == "__main__":
    main()
