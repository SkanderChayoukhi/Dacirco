"""
This module contains the main entry point for the dacirco controller application.

Modules:
    logging: Provides logging capabilities.
    subprocess: Allows spawning new processes, connecting to their input/output/error pipes, and obtaining their return codes.
    sys: Provides access to some variables used or maintained by the interpreter and to functions that interact strongly with the interpreter.
    pathlib: Offers classes representing filesystem paths with semantics appropriate for different operating systems.
    uuid: Provides immutable UUID objects (universally unique identifiers) and the functions to generate them.
    tomllib: Provides functions for parsing TOML files.
    typer: A library for creating CLI applications.
    typing_extensions: Provides backports of new type system features.

Functions:
    log_config(verbose: int, log_file_name: str) -> None:
        Setup basic logging configuration.

    get_local_ip_address() -> str:
        Retrieve the local IP address of the machine.

    read_config_file(file_path: Path, dc) -> dict:
        Read and parse a TOML configuration file.

Commands:
    run(
        minio_server: Annotated[str, typer.Option],
        log_file: Annotated[str, typer.Option],
        verbose: Annotated[int, typer.Option],
        port: Annotated[int, typer.Option],
        runner_type: Annotated[RunnerType, typer.Option],
        minio_port: Annotated[int, typer.Option],
        minio_access_key: Annotated[str, typer.Option],
        minio_secret_key: Annotated[str, typer.Option],
        max_workers: Annotated[int, typer.Option],
        bind_ip_address: Annotated[str, typer.Option],
        ip_address_for_tc_worker: Annotated[str, typer.Option],
        call_interval: Annotated[int, typer.Option],
        max_idle_time: Annotated[int, typer.Option],
        algorithm: Annotated[AlgorithmName, typer.Option],
        vm_params_file: Annotated[Path, typer.Option],
        pod_params_file: Annotated[Path, typer.Option],
        concentrate_workers: Annotated[bool, typer.Option],
        spread_workers: Annotated[bool, typer.Option],
        tag: Annotated[str, typer.Option],
        db_file_path: Annotated[Path, typer.Option]
        Main command to run the dacirco controller application.
        )
"""

import logging

import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import tomllib  # type: ignore
import typer
from typing import Annotated

from dacirco.controller.data_store.data_store import DataStore
from dacirco.controller.data_store.models import Base
from dacirco.controller.database import db_engine_session
from dacirco.controller.rpc_server import run_server
from dacirco.dataclasses import (
    AlgorithmName,
    MinIoParameters,
    PodParameters,
    RunnerType,
    SchedulerParameters,
    VMParameters,
)

app = typer.Typer()
_logger = logging.getLogger("dacirco")


def log_config(verbose: int, log_file_name: str) -> None:
    """Setup basic logging

    Args:
      verbose (int): the number of v's on the command line
    """
    loglevel = logging.WARN
    if verbose == 1:
        loglevel = logging.INFO
    elif verbose == 2:
        loglevel = logging.DEBUG
    # logger = logging.getLogger("dacirco")
    _logger.setLevel(logging.DEBUG)
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    formatter = logging.Formatter(logformat, datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_file_name, mode="w")
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setFormatter(formatter)
    ch.setLevel(loglevel)
    _logger.addHandler(fh)
    _logger.addHandler(ch)
    _logger.info("Started logging")
    _logger.debug("Verbose level is %d", verbose)


def get_local_ip_address() -> str:
    """
    Retrieves the local IP address of the machine.

    This function runs shell commands to list network interfaces and their
    associated IPv4 addresses. It searches for IP addresses within specific
    subnets (10.29 and 10.35) and returns the first matching IP address found.
    If no matching IP address is found, the function logs a critical error and
    terminates the program.

    Returns:
        str: The local IP address found within the specified subnets.

    Raises:
        SystemExit: If no matching IP address is found or if an error occurs
                    while retrieving the IP addresses.
    """
    out1 = subprocess.run(
        ["ip", "-o", "link", "show"], stdout=subprocess.PIPE
    ).stdout.decode("utf-8")
    interfaces = [x.split(":")[1].strip() for x in out1.split("\n")[:-1]]
    for interface in interfaces:
        try:
            out2 = subprocess.run(
                ["ip", "-o", "-4", "addr", "show", interface], stdout=subprocess.PIPE
            ).stdout.decode("utf-8")
            ip_a = out2.split()[3].split("/")[0]
            _logger.debug("Found ip address: %s", ip_a)
            if "10.29" in ip_a:
                _logger.info("Found IP address in 10.29: %s", ip_a)
                return ip_a
            if "10.35" in ip_a:
                _logger.info("Found IP address in 10.35: %s", ip_a)
                return ip_a
        except:
            _logger.critical(
                "Unable to auto-detect the local IP address to use, aborting"
            )
            raise SystemExit
    _logger.critical("Unable to auto-detect the local IP, aborting")
    raise SystemExit


def read_config_file(file_path: Path, dc) -> dict:
    """
    Reads a configuration file and returns its contents as a dictionary.

    Args:
        file_path (Path): The path to the configuration file.
        dc (type): A callable that takes keyword arguments and returns an instance of a class.

    Returns:
        dict: A dictionary where the keys are the sections of the configuration file and the values are instances of the class created by `dc`.

    Raises:
        SystemExit: If the configuration file does not contain a "default" section.
    """
    with open(file_path, "rb") as f:
        res = tomllib.load(f)
    if "default" not in res:
        print("No default section in config file")
        raise SystemExit
    return {k: dc(**v) for k, v in res.items()}


@app.command()
def run(
    minio_server: Annotated[
        str,
        typer.Option(
            "--minio-server",
            help="The name (or IP address) of the MinIO server",
            rich_help_panel="MinIO options",
        ),
    ],
    log_file: Annotated[
        str,
        typer.Option(
            "--log-file",
            help="The log file to write to",
            rich_help_panel="Logging options",
        ),
    ] = "dacirco-controller.log",
    verbose: Annotated[
        int,
        typer.Option(
            "--verbose", "-v", count=True, help="Increase verbosity", show_default=False
        ),
    ] = 0,
    port: Annotated[
        int,
        typer.Option(
            "--port",
            "-p",
            help="The port number to bind to",
        ),
    ] = 50051,
    runner_type: Annotated[
        RunnerType,
        typer.Option(
            "--runner-type",
            "-r",
            help="The type of runner",
        ),
    ] = RunnerType.vm,
    minio_port: Annotated[
        int,
        typer.Option(
            "--minio-port",
            help="The port number of the MinIO server",
            rich_help_panel="MinIO options",
        ),
    ] = 9000,
    minio_access_key: Annotated[
        str,
        typer.Option(
            "--minio-access-key",
            help="The access key for the MinIO server",
            rich_help_panel="MinIO options",
        ),
    ] = "minio",
    minio_secret_key: Annotated[
        str,
        typer.Option(
            "--minio-secret-key",
            help="The access secret for the MinIO server",
            rich_help_panel="MinIO options",
        ),
    ] = "pass4minio",
    max_workers: Annotated[
        int,
        typer.Option(
            "--max-workers",
            help="The maximum number of workers",
            rich_help_panel="Scheduling Algorithm options",
        ),
    ] = 2,
    bind_ip_address: Annotated[
        str,
        typer.Option(
            "--bind-ip-address",
            help="The IP address to bind to (DO NOT USE unless you know what you are doing)",
            rich_help_panel="Network options",
        ),
    ] = "",
    ip_address_for_tc_worker: Annotated[
        str,
        typer.Option(
            "--ip-address-for-tc-worker",
            help="The IP address to send to the TC workers (DO NOT USE unless you know what you are doing)",
            rich_help_panel="Network options",
        ),
    ] = "",
    port_for_tc_worker: Annotated[
        int,
        typer.Option(
            "--port-for-tc-worker",
            help="The port number to send to the TC workers (DO NOT USE unless you know what you are doing)",
            rich_help_panel="Network options",
        ),
    ] = 50051,
    call_interval: Annotated[
        int,
        typer.Option(
            "--call-interval",
            help="How many seconds between calls of the scheduler periodic_function",
            rich_help_panel="Scheduling Algorithm options",
        ),
    ] = 1,
    max_idle_time: Annotated[
        int,
        typer.Option(
            "--max-idle-time",
            help="How many seconds before a worker is destroyed (grace period algorithm)",
            rich_help_panel="Scheduling Algorithm options",
        ),
    ] = 60,
    algorithm: Annotated[
        AlgorithmName,
        typer.Option(
            "--algorithm",
            help="The algorithm to use",
            rich_help_panel="Scheduling Algorithm options",
        ),
    ] = AlgorithmName.minimize_workers,
    vm_params_file: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ] = Path("vm-params.toml"),
    pod_params_file: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ] = Path("pod-params.toml"),
    concentrate_workers: Annotated[
        bool,
        typer.Option(
            "--concentrate-workers",
            help="Try to place all the workers on the same node",
            rich_help_panel="Kubernetes Pods options",
        ),
    ] = False,
    spread_workers: Annotated[
        bool,
        typer.Option(
            "--spread-workers",
            help="Try to place workers on different nodes",
            rich_help_panel="Kubernetes Pods options",
        ),
    ] = False,
    tag: Annotated[
        str,
        typer.Option(
            "--tag",
            help="A tag to identify the run",
        ),
    ] = "",
    db_file_path: Annotated[
        Path,
        typer.Option(
            "--db-file",
            help="The path to the database file",
        ),
    ] = Path("dacirco-controller.db"),
) -> None:
    """Run the main controller with the specified options.

    Parameters:
    - minio_server (str): The name (or IP address) of the MinIO server.
    - log_file (str, optional): The log file to write to. Defaults to "dacirco-controller.log".
    - verbose (int, optional): Increase verbosity. Defaults to 0.
    - port (int, optional): The port number to bind to. Defaults to 50051.
    - runner_type (RunnerType, optional): The type of runner. Defaults to RunnerType.vm.
    - minio_port (int, optional): The port number of the MinIO server. Defaults to 9000.
    - minio_access_key (str, optional): The access key for the MinIO server. Defaults to "minio".
    - minio_secret_key (str, optional): The access secret for the MinIO server. Defaults to "pass4minio".
    - max_workers (int, optional): The maximum number of workers. Defaults to 2.
    - bind_ip_address (str, optional): The IP address to bind to (DO NOT USE unless you know what you are doing). Defaults to "".
    - ip_address_for_tc_worker (str, optional): The IP address to send to the TC workers (DO NOT USE unless you know what you are doing). Defaults to "".
    - call_interval (int, optional): How many seconds between calls of the scheduler periodic_function. Defaults to 1.
    - max_idle_time (int, optional): How many seconds before a worker is destroyed (grace period algorithm). Defaults to 60.
    - algorithm (AlgorithmName, optional): The algorithm to use. Defaults to AlgorithmName.minimize_workers.
    - vm_params_file (Path, optional): Path to the VM parameters file. Defaults to Path("vm-params.toml").
    - pod_params_file (Path, optional): Path to the Pod parameters file. Defaults to Path("pod-params.toml").
    - concentrate_workers (bool, optional): Try to place all the workers on the same node. Defaults to False.
    - spread_workers (bool, optional): Try to place workers on different nodes. Defaults to False.
    - tag (str, optional): A tag to identify the run. Defaults to "".
    - db_file_path (Path, optional): The path to the database file. Defaults to Path("dacirco-controller.db").
    Raises:
    - SystemExit: If both concentrate_workers and spread_workers are set to True.
    """

    typer.echo("Running...")
    if concentrate_workers and spread_workers:
        print(
            "You cannot set both concentrate_workers and spread_workers, pick only one!"
        )
        raise SystemExit
    if not bind_ip_address:
        bind_ip_address = get_local_ip_address()
        ip_address_for_tc_worker = bind_ip_address
    if not ip_address_for_tc_worker:
        ip_address_for_tc_worker = bind_ip_address
    log_config(verbose, log_file_name=log_file)
    vm_params = read_config_file(vm_params_file, VMParameters)

    pod_params = read_config_file(pod_params_file, PodParameters)
    engine, SessionLocal = db_engine_session(f"sqlite:///{db_file_path}")
    Base.metadata.create_all(engine)
    run_id = uuid4()
    data_store = DataStore(session_maker=SessionLocal)

    data_store.add_run(
        id=run_id,
        tag=tag,
    )

    minio_params = MinIoParameters(
        server=minio_server,
        port=minio_port,
        access_key=minio_access_key,
        access_secret=minio_secret_key,
    )

    sched_params = SchedulerParameters(
        algorithm=algorithm,
        max_workers=max_workers,
        call_interval=call_interval,
        max_idle_time=max_idle_time,
        runner_type=runner_type,
        concentrate_workers=concentrate_workers,
        spread_workers=spread_workers,
    )
    data_store.add_scheduler_parameters(sched_params, run_id)
    data_store.add_vm_parameters(vm_params, run_id)
    data_store.add_pod_parameters(pod_params, run_id)
    run_server(
        vm_params=vm_params,
        minio_params=minio_params,
        port=port,
        bind_ip_address=bind_ip_address,
        ip_address_for_worker=ip_address_for_tc_worker,
        port_for_worker=port_for_tc_worker,
        event_log_file=log_file,
        sched_params=sched_params,
        pod_params=pod_params,
        data_store=data_store,
    )
