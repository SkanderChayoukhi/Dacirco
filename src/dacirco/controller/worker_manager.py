"""Protocol defining the interface implemented by the [dacirco.controller.vm_manager.VMManager][] and [dacirco.controller.pod_manager.PodManager][] classes."""

from typing import Protocol
from uuid import UUID


class WorkerManager(Protocol):
    """
    Protocol defining the interface for managing workers.

    The WorkerManager interface is used to create and destroy workers.
    """

    def create_worker(self, param_key: str, worker_id: UUID, worker_name: str) -> str:
        """
        Creates a worker with the specified parameters.

        Args:
            param_key (str): The key parameter for the worker.
            worker_id (UUID): The unique identifier for the worker.
            worker_name (str): The name of the worker.

        Returns:
            str: A string with the name of the node (hypervisor) running the worker. If the node name cannot be determined, an empty string is returned..
        """
        ...

    def destroy_worker(self, worker_id: UUID) -> None:
        """
        Terminates and removes a worker identified by the given UUID.

        Args:
            worker_id (UUID): The unique identifier of the worker to be destroyed.

        Returns:
            None
        """
        ...
