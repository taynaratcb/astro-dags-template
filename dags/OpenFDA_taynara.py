# dags/openfda_cosmetic_events_pipeline.py
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pendulum
import pandas as pd
import pandas_gbq
import requests
import time
from datetime import date
from typing import Any, Dict, List

# ========================= Config =========================
GCP_PROJECT    = "bigqueryenap"
BQ_DATASET     = "FDA"
BQ_TABLE_STAGE = "cosmetic_events_weekly"
BQ_TABLE_COUNT = "cosmetic_events_counts"
BQ_LOCATION    = "US"
GCP_CONN_ID    = "google_cloud_default"

START_DATE = date(2025, 1, 1)
END_DATE   = date(2025, 6, 30)

TIMEOUT_S   = 30
MAX_RETRIES = 3

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "didactic-openfda-etl/1.0 (contato: exemplo@dominio.com)"})


# ========================= Helpers =========================
def _search_expr_by_day(day: date) -> str:
    d = day.strftime("%Y%m%d")
    return f"initial_received_date:{d}"

def _openfda_get(url: str, params: Dict[str, str]) -> Dict[str, Any]:
    for attempt in range(1, MAX_RETRIES + 1):
        r = SESSION.get(url, params=params, timeout=TIMEOUT_S)
        if r.status_code == 404:
            return {"results": []}
        if 200 <= r.status_code < 300:
            return r.json()
        time.sleep(attempt)
        if attempt < MAX_RETRIES and r.status_code in (429, 500, 502, 503, 504):
            continue
        r.raise_for_status()

def _to_flat(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    flat: List[Dict[str, Any]] = []
    for ev in rows:
        flat.append({
            "report_id": ev.get("report_id"),
            "initial_received_date": ev.get("initial_received_date"),
            "event_type": ev.get("event_type"),
            "product_description": ev.get("product_description"),
            "adverse_event": ev.get("adverse_event"),
            "outcome": ev.get("outcome"),
        })
    df = pd.DataFrame(flat)
    if df.empty:
        return df
    df["report_id"] = df["report_id"].astype(str)
    df["initial_received_date"] = pd.to_datetime(df["initial_received_date"], format="%Y%m%d", errors="coerce").dt.date
    df = df.drop_duplicates(subset=["report_id"], keep="first")
    return df


# ========================= DAG =========================
@dag(
    dag_id="openfda_cosmetic_events_pipeline",
    description="Consulta openFDA (cosméticos) -> trata (flat) -> salva (BQ stage) -> agrega diário (BQ).",
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 9, 28, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["openfda", "cosmetics", "bigquery", "etl"],
)
def openfda_cosmetic_events_pipeline():

    @task(retries=0)
    def extract_transform_load() -> Dict[str, str]:
        base_url = "https://api.fda.gov/cosmetic/event.json"
        all_rows: List[Dict[str, Any]] = []

        day = START_DATE
        n_calls = 0
        while day <= END_DATE:
            limit = 100
            skip = 0
            total_dia = 0
            while True:
                params = {
                    "search": _search_expr_by_day(day),
                    "limit": str(limit),
                    "skip": str(skip),
                }
                payload = _openfda_get(base_url, params)
                rows = payload.get("results", []) or []
                all_rows.extend(rows)
                total_dia += len(rows)
                n_calls += 1
                if len(rows) < limit:
                    break
                skip += limit
                time.sleep(0.25)
            print(f"[fetch] {day}: {total_dia} registros.")
            day = date.fromordinal(day.toordinal() + 1)
        print(f"[fetch] Jan–Jun/2025: {n_calls} chamadas, {len(all_rows)} registros no total.")

        df = _to_flat(all_rows)
        print(f"[normalize] linhas pós-normalização: {len(df)}")
        if not df.empty:
            print("[normalize] preview:\n", df.head(10).to_string(index=False))

        schema = [
            {"name": "report_id",              "type": "STRING"},
            {"name": "initial_received_date",  "type": "DATE"},
            {"name": "event_type",             "type": "STRING"},
            {"name": "product_description",    "type": "STRING"},
            {"name": "adverse_event",          "type": "STRING"},
            {"name": "outcome",                "type": "STRING"},
        ]
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        creds = bq.get_credentials()
        if df.empty:
            empty = pd.DataFrame(columns=[c["name"] for c in schema])
            empty.to_gbq(
                destination_table=f"{BQ_DATASET}.{BQ_TABLE_STAGE}",
                project_id=GCP_PROJECT,
                if_exists="replace",
                credentials=creds,
                table_schema=schema,
                location=BQ_LOCATION,
                progress_bar=False,
            )
            print("[stage] Nenhum registro para gravar, stage substituída por tabela vazia.")
        else:
            df.to_gbq(
                destination_table=f"{BQ_DATASET}.{BQ_TABLE_STAGE}",
                project_id=GCP_PROJECT,
                if_exists="replace",
                credentials=creds,
                table_schema=schema,
                location=BQ_LOCATION,
                progress_bar=False,
            )
            print(f"[stage] Gravados {len(df)} em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}.")

        return {"start": START_DATE.strftime("%Y-%m-%d"),
                "end":   END_DATE.strftime("%Y-%m-%d")}

    @task(retries=0)
    def build_daily_counts(meta: Dict[str, str]) -> None:
        start, end = meta["start"], meta["end"]

        sql = f"""
        SELECT
          initial_received_date AS day,
          COUNT(*)              AS events,
          'cosmetic'            AS category
        FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_STAGE}`
        WHERE initial_received_date BETWEEN DATE('{start}') AND DATE('{end}')
        GROUP BY day
        ORDER BY day
        """
        bq = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
        creds = bq.get_credentials()
        df_counts = pandas_gbq.read_gbq(
            sql,
            project_id=GCP_PROJECT,
            credentials=creds,
            dialect="standard",
            progress_bar_type=None,
            location=BQ_LOCATION,
        )
        if df_counts.empty:
            df_counts = pd.DataFrame(columns=["day", "events", "category"])

        schema_counts = [
            {"name": "day",      "type": "DATE"},
            {"name": "events",   "type": "INTEGER"},
            {"name": "category", "type": "STRING"},
        ]
        df_counts.to_gbq(
            destination_table=f"{BQ_DATASET}.{BQ_TABLE_COUNT}",
            project_id=GCP_PROJECT,
            if_exists="replace",
            credentials=creds,
            table_schema=schema_counts,
            location=BQ_LOCATION,
            progress_bar=False,
        )
        print(f"[counts] {len(df_counts)} linhas gravadas em {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE_COUNT}.")

    build_daily_counts(extract_transform_load())

openfda_cosmetic_events_pipeline()
