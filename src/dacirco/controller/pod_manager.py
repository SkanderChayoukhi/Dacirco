"""
Manages Kubernetes transcoding worker pods.
"""

import logging
from typing import Any
from uuid import UUID

from kubernetes import client, config  # type: ignore

from dacirco.dataclasses import MinIoParameters, PodParameters, SchedulerParameters

_logger = logging.getLogger("dacirco.pod_manager")


class PodManager:
    """
    Manages Kubernetes pods for workers in the dacirco namespace.

    Attributes:
        _pod_count (int): Counter for the number of pods.
        _NS_NAME (str): Namespace name for the pods.
        _k8s_client (CoreV1Api): Kubernetes client for interacting with the cluster.
        _ip_address_for_worker (str): IP address for the worker.
        _minio_parameters (MinIoParameters): Parameters for MinIO.
        _pod_parameters (dict[str, PodParameters]): Parameters for the pods.
        _sched_parameters (SchedulerParameters): Parameters for scheduling.
        _pods (dict[UUID, Any]): Dictionary to store pod information.

    Methods:
        __init__(pod_parameters, minio_parameters, sched_params, ip_address_for_worker):
            Initializes the PodManager with the given parameters.

        create_worker(param_key, worker_id, worker_name) -> str:
            Creates a new worker pod with the specified parameters.

        destroy_worker(worker_id) -> None:
            Destroys the worker pod with the given worker ID.
    """

    def __init__(
        self,
        pod_parameters: dict[str, PodParameters],
        minio_parameters: MinIoParameters,
        sched_params: SchedulerParameters,
        ip_address_for_worker: str,
        port_for_worker: int,
    ) -> None:
        """
        Initializes the PodManager with the given parameters.

        Args:
            pod_parameters (dict[str, PodParameters]): A dictionary containing pod parameters.
            minio_parameters (MinIoParameters): Parameters for MinIO configuration.
            sched_params (SchedulerParameters): Parameters for the scheduler configuration.
            ip_address_for_worker (str): The IP address to be sent to the worker nodes.
            port_for_worker (int): The port to be sent to the worker nodes.

        Returns:
            None
        """

        self._pod_count = 0
        self._NS_NAME = "dacirco"
        config.load_kube_config()  # type: ignore
        self._k8s_client = client.CoreV1Api()  # type: ignore
        namespaces = self._k8s_client.list_namespace()
        if not any(x.metadata.name == "dacirco" for x in namespaces.items):
            self._k8s_client.create_namespace(
                client.V1Namespace(metadata=client.V1ObjectMeta(name=self._NS_NAME))  # type: ignore
            )
        else:
            res = self._k8s_client.list_namespaced_pod(namespace=self._NS_NAME)
            for i in res.items:
                self._k8s_client.delete_namespaced_pod(i.metadata.name, self._NS_NAME)

        self._ip_address_for_worker = ip_address_for_worker
        self._port_for_worker = port_for_worker
        self._minio_parameters = minio_parameters
        self._pod_parameters = pod_parameters
        self._sched_parameters = sched_params
        self._pods: dict[UUID, Any] = {}

    def create_worker(self, param_key: str, worker_id: UUID, worker_name: str) -> str:
        """
        Creates a new worker pod with the specified parameters.

        Args:
            param_key (str): The key to access specific pod parameters.
            worker_id (UUID): The unique identifier for the worker.
            worker_name (str): The name of the worker.

        Returns:
            str: The name of the node where the pod is scheduled.

        Raises:
            ApiException: If there is an error creating or reading the pod.

        This method performs the following steps:

        1. Increments the internal pod count.
        2. Logs the creation of a new pod.
        3. Sets up the environment variables for the pod.
        4. Configures pod affinity based on scheduling parameters.
        5. Sets the node selector if specified.
        6. Defines resource limits and requests.
        7. Constructs the pod manifest.
        8. Creates the pod in the specified Kubernetes namespace.
        9. Stores the created pod in the internal pods dictionary.
        10. Reads and returns the node name where the pod is scheduled.
        """
        self._pod_count += 1
        pod_id = worker_id
        pod_name = worker_name
        _logger.info("Starting new pod. Name: %s, id: %s", pod_name, str(pod_id))
        pod_env = [
            {"name": "TCWORKER_GRPC_SERVER", "value": self._ip_address_for_worker},
            {"name": "TCWORKER_GRPC_PORT", "value": str(self._port_for_worker)},
            {"name": "TCWORKER_NAME", "value": pod_name},
            {"name": "TCWORKER_ID", "value": str(pod_id)},
            {
                "name": "TCWORKER_MINIO_SERVER",
                "value": self._minio_parameters.server,
            },
            {
                "name": "TCWORKER_MINIO_PORT",
                "value": str(self._minio_parameters.port),
            },
            {
                "name": "TCWORKER_MINIO_ACCESS_KEY",
                "value": self._minio_parameters.access_key,
            },
            {
                "name": "TCWORKER_MINIO_SECRET_KEY",
                "value": self._minio_parameters.access_secret,
            },
        ]

        pod_affinity = {}
        if self._sched_parameters.spread_workers:
            pod_affinity = {
                "podAntiAffinity": {
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "weight": 100,
                            "podAffinityTerm": {
                                "labelSelector": {
                                    "matchExpressions": [
                                        {
                                            "key": "app",
                                            "operator": "In",
                                            "values": ["tc-worker"],
                                        },
                                    ]
                                },
                                "topologyKey": "kubernetes.io/hostname",
                            },
                        }
                    ]
                }
            }
        if self._sched_parameters.concentrate_workers:
            pod_affinity = {
                "podAffinity": {
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "weight": 100,
                            "podAffinityTerm": {
                                "labelSelector": {
                                    "matchExpressions": [
                                        {
                                            "key": "app",
                                            "operator": "In",
                                            "values": ["tc-worker"],
                                        },
                                    ]
                                },
                                "topologyKey": "kubernetes.io/hostname",
                            },
                        }
                    ]
                }
            }
        node_selector = {}
        if self._pod_parameters[param_key].node_selector:
            node_selector = {
                "kubernetes.io/hostname": self._pod_parameters[param_key].node_selector
            }
        limits = {}
        limits["cpu"] = self._pod_parameters[param_key].cpu_limit
        limits["memory"] = self._pod_parameters[param_key].memory_limit
        requests = {}
        requests["cpu"] = self._pod_parameters[param_key].cpu_request
        requests["memory"] = self._pod_parameters[param_key].memory_request
        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "labels": {"app": "tc-worker"}},
            "spec": {
                "affinity": pod_affinity,
                "nodeSelector": node_selector,
                "containers": [
                    {
                        "image": self._pod_parameters[param_key].image,
                        "name": pod_name,
                        "env": pod_env,
                        "resources": {
                            "limits": limits,
                            "requests": requests,
                        },
                    }
                ],
                "restartPolicy": "Never",
            },
        }
        pod = self._k8s_client.create_namespaced_pod(
            body=pod_manifest, namespace=self._NS_NAME
        )
        self._pods[pod_id] = pod
        ret = self._k8s_client.read_namespaced_pod(
            name=pod_name, namespace=self._NS_NAME
        )
        return ret.spec.node_name  # type: ignore

    def destroy_worker(self, worker_id: UUID) -> None:
        """
        Destroys a worker pod identified by the given worker_id.

        Args:
            worker_id (UUID): The unique identifier of the worker pod to be destroyed.

        Raises:
            KeyError: If the worker_id does not exist in the current pods.

        Logs:
            Logs an error message if the worker_id is unknown.
            Logs an info message when a worker pod is being destroyed.
        """
        if worker_id not in self._pods.keys():
            msg = f"Destroy worker, asked to destroy unknown worker. Id: {worker_id}"
            _logger.error(msg)
        else:
            worker = self._pods[worker_id]
            pod_name = worker.metadata.name
            _logger.info("Destroying worker pod: %s (%s)", pod_name, worker_id)
            self._k8s_client.delete_namespaced_pod(pod_name, self._NS_NAME)

            self._pod_count -= 1
            del self._pods[worker_id]
