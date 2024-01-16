# watchdog.py

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

from prefect import State, runtime, flow, get_client, task
from prefect.server.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
    FlowRunFilterStartTime,
    FlowRunFilterExpectedStartTime,
)
from prefect.server.schemas.states import StateType

from electracommons.log_config import PrefectLogger

logger_prefect = PrefectLogger(__file__)


@task
async def find_long_running_flows(threshold_hours) -> list[UUID]:
    # threshold_hours = 0.0001 # Prueba para probar que no encuentre otro watchdog

    async with get_client() as client:
        flow_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(any_=[StateType.RUNNING]),
                ),
                start_time=FlowRunFilterStartTime(
                    before_=datetime.now(timezone.utc) -
                    timedelta(hours=threshold_hours),
                ),
            )
        )

    logger = logger_prefect.obtener_logger_prefect()

    for flow_run in flow_runs:
        if runtime.flow_run.id != flow_run.flow_id:
            flow_runs.remove(flow_run)
            logger.info(
                "El ID es el del Watchdog actual. No se cancelara. ID: %s", flow_run.flow_id)

    logger.info(
        f"Se encontraron {len(flow_runs)} flujos de larga duración (> {threshold_hours} horas)\n "
        + "\n ".join([f"{flow_run.name} ({flow_run.id})" for flow_run in flow_runs])
    )

    return [flow_run.id for flow_run in flow_runs]


@task
async def find_stale_flows(threshhold_hours) -> list[UUID]:

    async with get_client() as client:
        flow_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(any_=[StateType.SCHEDULED]),
                ),
                expected_start_time=FlowRunFilterExpectedStartTime(
                    before_=datetime.now(timezone.utc) -
                    timedelta(hours=threshhold_hours),
                ),
            )
        )

    logger = logger_prefect.obtener_logger_prefect()

    for flow_run in flow_runs:
        if runtime.flow_run.id != flow_run.flow_id:
            flow_runs.remove(flow_run)
            logger.info(
                "El ID es el del Watchdog actual. No se cancelara. ID: %s", flow_run.flow_id)

    logger.info(
        f"Se encontraron {len(flow_runs)} flujos con alta demora (> {threshhold_hours} horas)\n "
        + "\n ".join([f"{flow_run.name} ({flow_run.id})" for flow_run in flow_runs])
    )

    return [flow_run.id for flow_run in flow_runs]


@task
async def cancel_flow_runs(flow_run_id):
    logger = logger_prefect.obtener_logger_prefect()
    async with get_client() as client:
        logger.info("Cancelando flujo con ID: %s", flow_run_id)
        state = State(type=StateType.CANCELLED,
                      message="Cancelado por watchdog")
        await client.set_flow_run_state(flow_run_id, state, force=True)


@flow(name="Watchdog")
async def watchdog(stale_threshold_hours: float = 12, long_running_threshold_hours: float = 4):
    logger = logger_prefect.obtener_logger_prefect()
    # Obtengo la diferencia de tiempo entre que se programó y empezó
    flow_timezone = runtime.flow_run.scheduled_start_time.tzinfo
    time_difference = datetime.now(
        flow_timezone) - runtime.flow_run.scheduled_start_time

    # Si empezó más de 30 minutos despues de que se programó entonces no se debe ejecutar
    # debido a que otro run de watchdog se hará cargo
    if time_difference < timedelta(minutes=30):
        stale_flows = await find_stale_flows(stale_threshold_hours)
        await cancel_flow_runs.map(stale_flows)

        long_running_flows = await find_long_running_flows(long_running_threshold_hours)
        await cancel_flow_runs.map(long_running_flows)
    else:
        logger.warning("El flujo está atrasado. Se cancelará\n")
        await cancel_flow_runs(runtime.flow_run.id)

if __name__ == "__main__":
    asyncio.run(watchdog())
