"""
VMManager class for managing virtual machines using OpenStack.

Classes:
    OsVMParameters: Dataclass to store OpenStack VM parameters.
    VMManager: Class to manage VMs, including creation and destruction.
"""

import base64
import logging
from dataclasses import dataclass
import sched
from typing import Any
from uuid import UUID

import openstack

from dacirco.dataclasses import MinIoParameters, VMParameters, SchedulerParameters

_logger = logging.getLogger("dacirco.vm_manager")


@dataclass
class OsVMParameters:
    """
    Class that holds the parameters required to create a virtual machine (VM) in an OpenStack environment.

    Used only by the VMManager class.

    All the attributes are of type Any because the OpenStack SDK does not provide clear type hints for these attributes.

    Attributes:
        image (Any): The image to use for the VM.
        flavor (Any): The flavor (hardware configuration) to use for the VM.
        network (Any): The network configuration for the VM.
        keypair (Any): The keypair to use for SSH access to the VM.
        security_group (Any): The security group to apply to the VM.
    """

    image: Any
    flavor: Any
    network: Any
    keypair: Any
    security_group: Any


class VMManager:
    """
    VMManager class for managing virtual machines using OpenStack.

    Attributes:
        _vm_count (int): Counter for the number of VMs.
        _admin_os_client (Optional[openstack.connection.Connection]): OpenStack admin client connection.
        _os_client (openstack.connection.Connection): OpenStack client connection.
        _vms (dict[UUID, openstack.compute.v2.server.Server]): Dictionary to store VMs by their UUID.
        _ip_address_for_worker (str): IP address for the worker.
        _minio_parameters (MinIoParameters): Parameters for MinIO.
        _os_vm_params (dict[str, OsVMParameters]): Dictionary to store OpenStack VM parameters.

    Methods:
        __init__(vm_parameters: dict[str, VMParameters], minio_parameters: MinIoParameters, ip_address_for_worker: str) -> None:
            Initializes the VMManager with the given parameters and sets up OpenStack connections.

        _resource_not_found(name: str, value: str) -> None:
            Logs an error and raises SystemExit if a required OpenStack resource is not found.

        create_worker(param_key: str, worker_id: UUID, worker_name: str) -> str:
            Creates a new worker VM with the specified parameters and returns the node name.

        destroy_worker(worker_id: UUID) -> None:
            Destroys the worker VM with the given UUID.
    """

    def __init__(
        self,
        vm_parameters: dict[str, VMParameters],
        minio_parameters: MinIoParameters,
        sched_params: SchedulerParameters,
        ip_address_for_worker: str,
        port_for_worker: int,
    ) -> None:
        """
        Initialize the VMManager with the given parameters.

        Args:
            vm_parameters (dict[str, VMParameters]): A dictionary containing VM parameters.
            minio_parameters (MinIoParameters): Parameters for MinIO configuration.
            ip_address_for_worker (str): IP address to be sent to the worker.
            port_for_worker (int): Port to be sent to the worker.

        Raises:
            SystemExit: If there is an error setting up the OpenStack connection or retrieving resources.

        Attributes:
            _vm_count (int): Counter for the number of VMs.
            _admin_os_client (Optional[openstack.connection.Connection]): OpenStack admin client connection.
            _os_client (openstack.connection.Connection): OpenStack client connection.
            _vms (dict[UUID, openstack.compute.v2.server.Server]): Dictionary to store VM instances.
            _ip_address_for_worker (str): IP address for the worker.
            _minio_parameters (MinIoParameters): MinIO configuration parameters.
            _os_vm_params (dict[str, OsVMParameters]): Dictionary to store OpenStack VM parameters.
        """

        self._vm_count = 0
        self._admin_os_client = None
        try:
            self._os_client = openstack.connect(cloud="openstack")
        except Exception as exc:
            _logger.error(
                "Something went wrong when setting up the OpenStack connection: %s", exc
            )
            raise SystemExit
        try:
            self._admin_os_client = openstack.connect(cloud="openstack_admin")
            _logger.info("OpenStack admin access enabled")
        except Exception as exc:
            _logger.error(
                "Something went wrong when setting up the OpenStack admin connection: %s",
                exc,
            )

        self._vms: dict[UUID, openstack.compute.v2.server.Server] = {}  # type:ignore
        self._ip_address_for_worker = ip_address_for_worker
        self._port_for_worker = port_for_worker
        self._minio_parameters = minio_parameters
        self._sched_params = sched_params
        self._os_vm_params: dict[str, OsVMParameters] = {}
        for key, value in vm_parameters.items():
            # Get the OpenStack resources
            try:
                os_image = self._os_client.compute.find_image(value.image)
                if not os_image:
                    self._resource_not_found("Image", value.image)
                else:
                    self._os_image = os_image
                os_flavor = self._os_client.compute.find_flavor(value.flavor)
                if not os_flavor:
                    self._resource_not_found("Flavor", value.flavor)
                else:
                    self._os_flavor = os_flavor
                    value.cpus = os_flavor.vcpus  # type:ignore
                    value.memory = os_flavor.ram  # type:ignore
                os_network = self._os_client.network.find_network(value.network)
                if not os_network:
                    self._resource_not_found("Network", value.network)
                else:
                    self._os_network = os_network
                os_keypair = self._os_client.compute.find_keypair(value.key)
                if not os_keypair:
                    self._resource_not_found("Keypair", value.key)
                else:
                    self._os_keypair = os_keypair
                os_secgroup = self._os_client.network.find_security_group(
                    value.security_group
                )
                if not os_secgroup:
                    self._resource_not_found("Security group", value.security_group)
                else:
                    self._os_secgroup = os_secgroup
                self._os_vm_params[key] = OsVMParameters(
                    image=os_image,
                    flavor=os_flavor,
                    network=os_network,
                    keypair=os_keypair,
                    security_group=os_secgroup,
                )
            except Exception as exc:
                _logger.error(
                    "Something went wrong when getting the OpenStack resources: %s", exc
                )
                raise SystemExit
        # Check if the server groups exists
        self._soft_anti_affinity_group_name = "dacirco-anti-affinity"
        self._soft_affinity_group_name = "dacirco-affinity"
        self._soft_anti_affinity_group_id = None
        self._soft_affinity_group_id = None

        res = self._os_client.compute.find_server_group(
            self._soft_anti_affinity_group_name
        )
        if res:
            _logger.debug("Soft anti-affinity server group exists")
            self._soft_anti_affinity_group_id = res.id
        else:
            _logger.debug("Soft anti-affinity server group does not exist")
            res = self._os_client.compute.create_server_group(
                name=self._soft_anti_affinity_group_name,
                policies=["anti-affinity"],
            )
            if res:
                _logger.debug("Soft anti-affinity server group created")
                self._soft_anti_affinity_group_id = res.id

        res = self._os_client.compute.find_server_group(self._soft_affinity_group_name)
        if res:
            _logger.debug("Soft affinity server group exists")
            self._soft_affinity_group_id = res.id
        else:
            _logger.debug("Soft affinity server group does not exist")
            res = self._os_client.compute.create_server_group(
                name=self._soft_affinity_group_name,
                policies=["affinity"],
            )
            if res:
                _logger.debug("Soft affinity server group created")
                self._soft_affinity_group_id = res.id

    def _resource_not_found(self, name: str, value: str):
        """
        Logs an error message indicating that a specified OpenStack resource was not found
        and raises a SystemExit exception.

        Args:
            name (str): The name of the resource that was not found.
            value (str): The value associated with the resource that was not found.

        Raises:
            SystemExit: This exception is raised to terminate the program.
        """
        _logger.error(f"OpenStack: {name} (value: {value}) not found")
        raise SystemExit

    def create_worker(self, param_key: str, worker_id: UUID, worker_name: str) -> str:
        """
        Creates a new worker virtual machine (VM) with the specified parameters.

        Args:
            param_key (str): The key to retrieve VM parameters from the internal dictionary.
            worker_id (UUID): The unique identifier for the worker.
            worker_name (str): The name to assign to the worker VM.

        Returns:
            str: The name of the node (hypervisor) running the VM. If the node name cannot be determined, an empty string is returned.

        Raises:
            KeyError: If the param_key does not exist in the internal VM parameters dictionary.
            Exception: If there is an error during the VM creation process.

        Logs:
            Info: Logs the start of a new VM with its name, ID, and parameter key.
            Debug: Logs the VM parameter key and its associated parameters.
            Warning: Logs a warning if the node name cannot be determined or if there is no admin access.
        """
        self._vm_count += 1

        _logger.info(
            f"Starting new VM. Name: {worker_name}, id: {worker_id}, param key: {param_key}"
        )
        _logger.debug(
            f"VM param key: {param_key}\nVM params: {self._os_vm_params[param_key]}"
        )
        commands = f"#!/bin/bash\ntee /etc/dacirco/environment <<EOF\n"
        commands += f"TCWORKER_GRPC_SERVER={self._ip_address_for_worker}\n"
        commands += f"TCWORKER_GRPC_PORT={self._port_for_worker}\n"
        commands += f"TCWORKER_NAME={worker_name}\n"
        commands += f"TCWORKER_ID={worker_id}\n"
        commands += f"TCWORKER_MINIO_SERVER={self._minio_parameters.server}\n"
        commands += f"TCWORKER_MINIO_PORT={self._minio_parameters.port}\n"
        commands += f"TCWORKER_MINIO_ACCESS_KEY={self._minio_parameters.access_key}\n"
        commands += (
            f"TCWORKER_MINIO_SECRET_KEY={self._minio_parameters.access_secret}\n"
        )
        commands += f"TCWORKER_LOG_FILE=/home/ubuntu/dacirco_tc_worker.log\n"
        commands += f"EOF"

        userdata = userdata = base64.b64encode(commands.encode("utf-8")).decode("utf-8")
        scheduler_hints = {}
        if self._soft_anti_affinity_group_id and self._sched_params.spread_workers:
            scheduler_hints["group"] = self._soft_anti_affinity_group_id
        if self._soft_affinity_group_id and self._sched_params.concentrate_workers:
            scheduler_hints["group"] = self._soft_affinity_group_id

        server: openstack.compute.v2.server.Server = (  # type:ignore
            self._os_client.compute.create_server(
                name=worker_name,
                image_id=self._os_vm_params[param_key].image.id,
                flavor_id=self._os_vm_params[param_key].flavor.id,
                networks=[{"uuid": self._os_vm_params[param_key].network.id}],
                key_name=self._os_vm_params[param_key].keypair.name,  # type:ignore
                user_data=userdata,
                security_groups=[
                    {"name": self._os_vm_params[param_key].security_group.name},
                    {"name": "default"},
                ],
                scheduler_hints=scheduler_hints,
            )
        )
        node = ""
        if self._admin_os_client:
            # For some unknown reason, the hypervisor_hostname is often None
            # hence we try multiple times (usually it works after 4 or 5 times)
            server = self._admin_os_client.compute.get_server(server.id)
            node = server.hypervisor_hostname
            max_count = 30
            count = 1
            while node is None and count <= max_count:
                server = self._admin_os_client.compute.get_server(  # type:ignore
                    server.id
                )  # type:ignore
                node = server.hypervisor_hostname
                count += 1
            if node is None:
                _logger.warning("Failed to get node name, even if admin access enabled")
        else:
            _logger.warning("No OpenStack admin access, cannot get node running VM")
        self._vms[worker_id] = server
        return node

    def destroy_worker(self, worker_id: UUID) -> None:
        """
        Destroy a worker VM identified by the given worker_id.

        This method checks if the worker_id exists in the internal VM dictionary.
        If the worker_id is not found, it logs an error message. If the worker_id
        is found, it proceeds to delete the VM using the OpenStack client and
        removes the VM from the internal dictionary, decrementing the VM count.

        Args:
            worker_id (UUID): The unique identifier of the worker VM to be destroyed.

        Returns:
            None
        """
        if worker_id not in self._vms.keys():
            msg = f"Destroy worker, asked to destroy unknown worker. Id: {worker_id}"
            _logger.error(msg)
        else:
            vm = self._vms[worker_id]
            _logger.info("Destroying VM: %s (%s)", worker_id, vm.name)
            self._os_client.compute.delete_server(vm)  # type:ignore

            del self._vms[worker_id]
            self._vm_count -= 1
