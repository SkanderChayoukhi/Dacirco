from typing import Protocol
from uuid import UUID

from dacirco.dataclasses import TCTask


class SendTaskToWorker(Protocol):
    """Protocol definition to decuple the [DaCircoRCPServer][dacirco.controller.rpc_server.DaCircoRCPServer]
    and the [DaCircoController][dacirco.controller.controller.DaCircoController] classes.  The latter calls the
    [send_tc_task_to_worker][dacirco.controller.rpc_server.DaCircoRCPServer.send_tc_task_to_worker`] method of the
    former to send a task to a worker.
    """

    def send_tc_task_to_worker(self, worker_id: UUID, tc_task_desc: TCTask) -> None: ...
    def remove_worker_queue(self, worker_id: UUID) -> None: ...
