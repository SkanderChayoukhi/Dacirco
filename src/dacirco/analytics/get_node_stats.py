import math
import sqlite3
import sys
from functools import partial
from pathlib import Path
from typing import Annotated
from urllib.parse import unquote

import httpx
import numpy as np
import pandas as pd
import typer

app = typer.Typer()


def p_get_gauge_fun(
    p_server: str, fun: str, gauge: str, node: str, end: str, duration: str
):
    if not node:
        return (np.nan, np.nan, np.nan)
    params = {
        "query": f'{fun}({gauge}{{instance="{node}:9100"}}[{duration}s])',
        "time": end,
    }
    res = httpx.get(f"http://{p_server}:9090/api/v1/query", params=params)
    res_json = res.json()
    if res.status_code != 200:
        print("Error while fetching data")
        print("Request url: ", unquote(str(res.url)))
        print("Response: ", res_json)
        sys.exit()
    if not res_json["data"]["result"]:
        return np.nan
    else:
        return round(float(res_json["data"]["result"][0]["value"][1]), 2)


def p_get_memory_fun(p_server: str, fun: str, node: str, end: str, duration: str):
    if not node:
        return (np.nan, np.nan, np.nan)
    mem_query = (
        f'node_memory_MemTotal_bytes{{instance="{node}:9100"}}'
        f'-node_memory_MemFree_bytes{{instance="{node}:9100"}}'
        f'-node_memory_Buffers_bytes{{instance="{node}:9100"}}'
        f'-node_memory_Cached_bytes{{instance="{node}:9100"}}'
    )

    params = {
        "query": f"{fun}(({mem_query})[{duration}s:10s])",
        "time": end,
    }
    res = httpx.get(f"http://{p_server}:9090/api/v1/query", params=params)
    res_json = res.json()
    if res.status_code != 200:
        print("Error while fetching data")
        print("Request url: ", unquote(str(res.url)))
        print("Response: ", res_json)
        sys.exit()
    if not res_json["data"]["result"]:
        return np.nan
    else:
        return round(float(res_json["data"]["result"][0]["value"][1]) / (10**9), 2)


def get_load1(row: pd.DataFrame, p_server: str):
    node = row["node"]  # type: ignore
    t_td = (
        row["t_upld_start"].tz_localize("Europe/Paris").tz_convert("UTC").isoformat("T")
        # + "Z"
    )  # type: ignore
    duration = str(math.ceil(row["t_diff_tc"].total_seconds()))  # type: ignore

    res = tuple(
        p_get_gauge_fun(
            p_server=p_server,
            fun=f,
            gauge="node_load1",
            node=node,  # type: ignore
            end=t_td,
            duration=duration,
        )
        for f in ["min_over_time", "avg_over_time", "max_over_time"]
    )

    return res


def get_memory(row: pd.DataFrame, p_server: str):
    node = row["node"]  # type: ignore
    t_td = (
        row["t_upld_start"].tz_localize("Europe/Paris").tz_convert("UTC")
    ).isoformat()  # type: ignore
    duration = str(math.ceil(row["t_diff_tc"].total_seconds()))  # type: ignore

    res = tuple(
        p_get_memory_fun(
            p_server=p_server,
            fun=f,
            node=node,  # type: ignore
            end=t_td,
            duration=duration,
        )
        for f in ["min_over_time", "avg_over_time", "max_over_time"]
    )

    return res


@app.command()
def process_db_file(
    prometheus_server: Annotated[
        str,
        typer.Option(
            "--prometheus-server",
            "-s",
            help="The hostname or IP address of the Prometheus server",
        ),
    ] = "controller",
    db_file: Annotated[
        Path,
        typer.Option(
            "--db-file",
            "-d",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ] = Path("dacirco-controller.db"),
    disable_memory_stats: Annotated[
        bool,
        typer.Option(
            "--disable-memory-stats",
            "-n",
            help="Disable the collection of memory stats",
        ),
    ] = False,
):
    conn = sqlite3.connect(db_file)
    query = """
        SELECT tc_request.id,
            tc_request.run_id,
            tc_request.input_video,
            tc_request.bitrate,
            tc_request.speed,
            tc_request.t_received,
            tc_task.t_assigned,
            tc_task.t_dwnld_start,
            tc_task.t_tc_start,
            tc_task.t_upld_start,
            tc_task.t_completed,
            worker.name,
            worker.node
        FROM tc_request
        JOIN tc_task ON tc_request.id == tc_task.request_id
        JOIN worker ON tc_task.worker_id == worker.id
        WHERE tc_task.t_completed IS NOT NULL AND tc_request.status == 'completed'
	"""
    req_w = pd.read_sql_query(query, conn)
    conn.close()

    time_cols = [
        "t_received",
        "t_assigned",
        "t_dwnld_start",
        "t_tc_start",
        "t_upld_start",
        "t_completed",
    ]
    req_w[time_cols] = req_w[time_cols].apply(pd.to_datetime)
    req_w["t_diff_wait"] = req_w["t_assigned"] - req_w["t_received"]
    req_w["t_diff_wait_assigned"] = req_w["t_dwnld_start"] - req_w["t_assigned"]
    req_w["t_diff_dwnld"] = req_w["t_tc_start"] - req_w["t_dwnld_start"]
    req_w["t_diff_tc"] = req_w["t_upld_start"] - req_w["t_tc_start"]
    req_w["t_diff_upld"] = req_w["t_completed"] - req_w["t_upld_start"]

    # Add three columns with the node_load1 min, avg, max
    req_w[["load1_min", "load1_avg", "load1_max"]] = req_w.apply(
        partial(get_load1, p_server=prometheus_server),  # type: ignore
        axis=1,
        result_type="expand",  # type: ignore
    )

    if not disable_memory_stats:
        # Add three columns with the used memory min, avg, max
        req_w[["used_mem_min", "used_mem_avg", "used_mem_max"]] = req_w.apply(
            partial(get_memory, p_server=prometheus_server),  # type: ignore
            axis=1,
            result_type="expand",  # type: ignore
        )

    req_w.to_csv(
        f"{db_file.stem}-ok-requests-node-stats.csv",
        index=False,
    )


if __name__ == "__main__":
    app()
