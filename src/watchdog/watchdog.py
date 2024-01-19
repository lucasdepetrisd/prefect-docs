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
from prefect.client.schemas.actions import LogCreate
from prefect.client.orchestration import PrefectClient
from prefect.states import Cancelled

from electracommons.log_config import PrefectLogger

logger_prefect = PrefectLogger(__file__)

CURRENT_FLOW_RUN = runtime.flow_run

@task(timeout_seconds=30)
async def find_long_running_flows(threshold_hours: float) -> list[UUID]:
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

    filtered_flows = flow_runs.copy()
    for flow_run in flow_runs:
        if CURRENT_FLOW_RUN.id == str(flow_run.id):
            filtered_flows.remove(flow_run)
            logger.info(
                "El ID %s es el del Watchdog actual. No se cancelara.", str(flow_run.flow_id))

    logger.info(
        f"Se encontraron {len(filtered_flows)} flujos de larga duracion (> {threshold_hours} horas)\n "
        + "\n ".join([f"{flow_run.name} ({flow_run.id})" for flow_run in filtered_flows])
    )

    return [flow_run.id for flow_run in filtered_flows]


@task(timeout_seconds=30)
async def find_stale_flows(threshhold_hours: float) -> list[UUID]:
    # await asyncio.sleep(20)
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

    filtered_flows = flow_runs.copy()
    for flow_run in flow_runs:
        if CURRENT_FLOW_RUN.id == str(flow_run.id):
            filtered_flows.remove(flow_run)
            logger.info(
                "El ID %s es el del Watchdog actual. No se cancelara.", str(flow_run.flow_id))

    logger.info(
        f"Se encontraron {len(filtered_flows)} flujos con alta demora (> {threshhold_hours} horas)\n "
        + "\n ".join([f"{flow_run.name} ({flow_run.id})" for flow_run in filtered_flows])
    )

    return [flow_run.id for flow_run in filtered_flows]


@task(timeout_seconds=20)
async def cancel_flow_runs(flow_run_id: UUID):
    logger = logger_prefect.obtener_logger_prefect()

    state = State(type=StateType.CANCELLED,
                    message="Cancelado por watchdog debido a alta duracion")

    async with get_client() as client:

        logger.info("Cancelando flujo de ID: %s", flow_run_id)
        await send_log(client, flow_run_id, f"Se cancelara la ejecucion por Watchdog con ID: {CURRENT_FLOW_RUN.id}")

        result_state = await client.set_flow_run_state(flow_run_id, state, force=True)

        if str(result_state.status) == 'SetStateStatus.ACCEPT':
            logger.info("Flujo cancelado de ID: %s", flow_run_id)
            await send_log(client, flow_run_id, f"Ejecucion cancelada por Watchdog con ID: {CURRENT_FLOW_RUN.id}")


async def send_log(client: PrefectClient, flow_run_id: UUID, message: str):
    """
    Función para enviar un log a un flujo externo antes de cancelarlo.

    Args:
        client (PrefectClient): cliente obtenido a partir de 'async with get_cliente() as client'.
        flow_run_id (UUID): flujo en el que se loggeara el mensaje
        message (str): mensaje a loggear en el flujo.
    """
    log_cancelacion = LogCreate(
                name="Watchdog-Logger",
                level=30, # Warning
                message=message,
                timestamp=datetime.now(tz=timezone.utc),
                flow_run_id=flow_run_id
        )
    await client.create_logs(logs=[log_cancelacion])


@flow(name="Watchdog", timeout_seconds=60)
async def watchdog(stale_threshold_hours: float = 12, long_running_threshold_hours: float = 1):
    # Obtengo la diferencia de tiempo entre que se programó y empezó
    flow_timezone = CURRENT_FLOW_RUN.scheduled_start_time.tzinfo
    time_difference = datetime.now(flow_timezone) - CURRENT_FLOW_RUN.scheduled_start_time

    try:
        # Si empezó más de 30 minutos despues de que se programó entonces no se debe ejecutar
        # debido a que otro run de watchdog se hará cargo
        if time_difference < timedelta(minutes=30):
            # stale_flows = await asyncio.wait_for(find_stale_flows(stale_threshold_hours), timeout=10) # Alternativa para limitar segundos
            stale_flows = await find_stale_flows(stale_threshold_hours)
            await cancel_flow_runs.map(stale_flows)

            long_running_flows = await find_long_running_flows(long_running_threshold_hours)
            await cancel_flow_runs.map(long_running_flows)
        else:
            return Cancelled(message="El flujo estaba demorado por lo que se canceló.")
    except asyncio.TimeoutError:
        return Cancelled(message="Se superó el límite de tiempo y se detendrá la ejecución")

if __name__ == "__main__":
    asyncio.run(watchdog())
