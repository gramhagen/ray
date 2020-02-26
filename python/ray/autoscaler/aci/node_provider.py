import logging
from threading import RLock
from uuid import uuid4

from azure.common.client_factory import get_client_from_cli_profile
from msrestazure.azure_active_directory import MSIAuthentication
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute.models import ResourceIdentityType

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME

INSTANCE_NAME_MAX_LEN = 64
INSTANCE_NAME_UUID_LEN = 4

logger = logging.getLogger(__name__)


def synchronized(f):
    def wrapper(self, *args, **kwargs):
        self.lock.acquire()
        try:
            return f(self, *args, **kwargs)
        finally:
            self.lock.release()

    return wrapper


class AciNodeProvider(NodeProvider):
    """
    Azure Container Instance Node Provider
    Assumes credentials are set by running `az login`
    and default subscription is configured through `az account`

    Nodes may be in one of three states: {pending, running, terminated}. Nodes
    appear immediately once started by `create_node`, and transition
    immediately to terminated when `terminate_node` is called.
    """

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        kwargs = {}
        if "subscription_id" in provider_config:
            kwargs["subscription_id"] = provider_config["subscription_id"]
        try:
            self.compute_client = get_client_from_cli_profile(
                client_class=ComputeManagementClient, **kwargs)
            self.network_client = get_client_from_cli_profile(
                client_class=NetworkManagementClient, **kwargs)
        except Exception:
            logger.info(
                "CLI profile authentication failed. Trying MSI")

            credentials = MSIAuthentication()
            self.compute_client = ComputeManagementClient(
                credentials=credentials, **kwargs)
            self.network_client = NetworkManagementClient(
                credentials=credentials, **kwargs)

        self.lock = RLock()

        # cache node objects
        self.cached_nodes = {}

