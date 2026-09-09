"""Background ETL trigger broadcast for Pumpwood views."""
import copy
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from pumpwood_communication.microservices import PumpWoodMicroService

_executor = ThreadPoolExecutor(
    max_workers=4, thread_name_prefix='pumpwood-etl-broadcast')
"""Shared pool for non-blocking ETL trigger calls."""


def _run_etl_trigger(microservice: PumpWoodMicroService,
                     parameters: dict) -> None:
    """Login and call ``ETLTrigger.process_matching_triggers``.

    Args:
        microservice (PumpWoodMicroService):
            Worker-scoped microservice client.
        parameters (dict):
            Payload for ``process_matching_triggers``.
    """
    try:
        microservice.login()
        microservice.execute_action(
            model_class="ETLTrigger",
            action="process_matching_triggers",
            parameters=parameters)
        logger.info("ETL trigger in broadcast successfully")
    except Exception:
        logger.exception(
            "ETL trigger broadcast failed. model_class={model_class} "
            "trigger_type={trigger_type} object_id={object_id}".format(
                model_class=parameters.get("model_class"),
                trigger_type=parameters.get("trigger_type"),
                object_id=parameters.get("object_id")))


def broadcast_etl_trigger_background(
        microservice: PumpWoodMicroService,
        parameters: dict) -> None:
    """Schedule ``ETLTrigger.process_matching_triggers`` without blocking.

    Args:
        microservice (PumpWoodMicroService):
            View microservice client; a worker copy is created for the
            background call.
        parameters (dict):
            Serializable trigger payload (deep-copied before submit).
    """
    worker_client = microservice.clone()
    payload = copy.deepcopy(parameters)
    _executor.submit(_run_etl_trigger, worker_client, payload)
