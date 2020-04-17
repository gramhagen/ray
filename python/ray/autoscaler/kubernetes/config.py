import logging

import kubernetes
from kubernetes.config import ConfigException

log_prefix = "KubernetesNodeProvider: "
logger = logging.getLogger(__name__)


def config_k8s():
    try:
        kubernetes.config.load_incluster_config()
    except ConfigException:
        kubernetes.config.load_kube_config()


def using_existing_msg(resource_type, name):
    logger.info("%susing existing %s '%s'", log_prefix, resource_type, name)


def not_found_msg(resource_type, name):
    logger.info("%s%s '%s' not found, attempting to create it", log_prefix,
                resource_type, name)


def created_msg(resource_type, name):
    logger.info("%ssuccessfully created %s '%s'", log_prefix, resource_type,
                name)


def not_provided_msg(resource_type):
    logger.info("%sno %s config provided, must already exist", log_prefix,
                resource_type)


def bootstrap_kubernetes(config):
    if not config["provider"].get("use_internal_ips", False):
        return ValueError("Ray must use internal IP addresses for Kubernetes "
                          "Please set 'use_internal_ips' to true.")

    config_k8s()
    core_api = kubernetes.client.CoreV1Api()
    auth_api = kubernetes.client.RbacAuthorizationV1Api()

    config = configure_namespace(config, core_api)
    config = configure_autoscaler_service_account(config, core_api)
    config = configure_autoscaler_role(config, auth_api)
    config = configure_autoscaler_role_binding(config, auth_api)

    return config


def find_namespace_entry(items, field_name, namespace):
    if len(items) > 0:
        assert len(items) == 1, (
            "Found multiple {field} entries with namespace: {name}".format(
                field=field_name, name=namespace))
        using_existing_msg(field_name, namespace)
        return True
    else:
        not_found_msg(field_name, namespace)
        return False


def check_metadata(field, config):
    entry = config["provider"][field]
    namespace = config["provider"]["namespace"]
    if "namespace" not in entry["metadata"]:
        entry["metadata"]["namespace"] = namespace
    elif entry["metadata"]["namespace"] != namespace:
        raise ValueError(
            "Namespace of {field} config doesn't match provided namespace "
            "'{ns}'. Set it to {ns} or remove the field.".format(
                field=field, ns=namespace))

    name = entry["metadata"]["name"]
    return "metadata.name={}".format(name)


def configure_namespace(config, core_api):
    namespace_field = "namespace"
    namespace = config["provider"].get(namespace_field)
    assert namespace, "Provider config must include {} field".format(
        namespace_field)

    field_selector = "metadata.name={}".format(namespace)
    namespaces = core_api.list_namespace(field_selector=field_selector).items

    if not find_namespace_entry(namespaces, namespace_field, namespace):
        metadata = kubernetes.client.V1ObjectMeta(name=namespace)
        namespace_config = kubernetes.client.V1Namespace(metadata=metadata)
        core_api().create_namespace(namespace_config)
        created_msg(namespace_field, namespace)

    return config


def configure_autoscaler_service_account(config, core_api):
    namespace = config["provider"]["namespace"]

    account_field = "autoscaler_service_account"
    try:
        field_selector = check_metadata(account_field, config)
    except KeyError:
        not_provided_msg(account_field)
        return config

    account = config["provider"][account_field]
    accounts = core_api().list_namespaced_service_account(
        namespace, field_selector=field_selector).items

    if not find_namespace_entry(accounts, account_field, namespace):
        core_api.create_namespaced_service_account(namespace, account)
        created_msg(account_field, namespace)

    return config


def configure_autoscaler_role(config, auth_api):
    namespace = config["provider"]["namespace"]

    role_field = "autoscaler_role"
    try:
        field_selector = check_metadata(role_field, config)
    except KeyError:
        not_provided_msg(role_field)
        return config

    role = config["provider"][role_field]
    roles = auth_api().list_namespaced_role(
        namespace, field_selector=field_selector).items

    if not find_namespace_entry(roles, role_field, namespace):
        auth_api.create_namespaced_role(namespace, role)
        created_msg(role_field, namespace)

    return config


def configure_autoscaler_role_binding(config, auth_api):
    namespace = config["provider"]["namespace"]

    binding_field = "autoscaler_role_binding"
    try:
        field_selector = check_metadata(binding_field, config)
    except KeyError:
        not_provided_msg(binding_field)
        return None

    binding = config["provider"][binding_field]
    for subject in binding["subjects"]:
        if "namespace" not in subject:
            subject["namespace"] = namespace
        elif subject["namespace"] != namespace:
            raise ValueError(
                "Namespace of {field} subject {subject} config doesn't match "
                "provided namespace '{ns}'. Set to {ns} or remove the field."
                .format(field=binding_field, subject=subject, ns=namespace))

    bindings = auth_api.list_namespaced_role_binding(
        namespace, field_selector=field_selector).items
    if not find_namespace_entry(bindings, binding_field, namespace):
        auth_api.create_namespaced_role_binding(namespace, binding)
        created_msg(binding_field, namespace)

    return config
