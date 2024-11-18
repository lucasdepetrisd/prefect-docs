"""
    Script para obtener información de flujos y deployments de Prefect e interactuar con su client.

    - `get_flow_runs_info(start_date: datetime, end_date: datetime, states: list[str]) -> dict`: 
        Esta tarea obtiene información de los flujos que se ejecutaron en un rango de fechas y estados específicos.
    - `get_subflow_info(parent_flow_run_id: UUID, states: list[str]) -> dict`: 
        Esta tarea obtiene información de los subflujos de un flujo padre específico utilizando su ID.
    - `get_flow_info(flow_id: UUID) -> dict`: 
        Esta tarea obtiene información de un flujo específico utilizando su ID.
    - `get_deployment_info(deployment_id: UUID) -> dict`: 
        Esta tarea obtiene información de un deployment específico utilizando su ID.
    
"""

import asyncio
from typing import Union, Sequence
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from uuid import UUID

from prefect import runtime, task, get_client
from prefect.client.schemas.objects import StateType, StateDetails
from prefect.states import State
from prefect import exceptions

# Se podria filtrar en las funciones de get_xxx_info utilizando los filtros de prefect pero no lo vi necesario
# ya que se puede hacer con diccionarios y leyendo la documentación de prefect en {url de prefect}/docs
from prefect.server.schemas.filters import (
    FlowRunFilter,
    # FlowRunFilterState,
    # FlowRunFilterStateType,
    # FlowRunFilterStartTime,
)


@task
async def get_flow_runs_info(
        start_date: datetime,
        end_date: datetime,
        states: Union[Sequence[str], Sequence[StateType], None] = None
    ) -> list[dict]:
    """
    Obtiene información de ejecuciones de flujo dentro de un rango de fechas y estados específicos.
    La zona horaria corresponde a UTC.

    Parámetros:
    - start_date (datetime): Fecha de inicio del rango de fechas.
    - end_date (datetime): Fecha de fin del rango de fechas.
    - states (list[str], opcional): Lista de estados de ejecución a filtrar. Por defecto es None.
    Retorna:
    - list[dict]: Lista de diccionarios con información de las ejecuciones de flujo.
    """

    # Filter for flows that have a start_time
    flow_run_filter = FlowRunFilter(
        state={
            'type': {'any_': states} if states else None,
        },
        start_time={
            'after_': start_date,
            'before_': end_date
        }
    )

    # Filter for flows that have an expected_start_time but no start_time
    flow_run_filter_without_start_time = FlowRunFilter(
        state={
            'type': {'any_': states} if states else None,
        },
        expected_start_time={
            'after_': start_date,
            'before_': end_date
        },
        start_time={
            'is_null_': True
        }
    )

    async with get_client() as client:
        flow_runs = await client.read_flow_runs(
            flow_run_filter=flow_run_filter,
            sort="START_TIME_DESC"
        )

        flow_runs_without_start_time = await client.read_flow_runs(
            flow_run_filter=flow_run_filter_without_start_time,
            sort="START_TIME_DESC"
        )

    combined_flow_runs = flow_runs + flow_runs_without_start_time

    flow_runs_info = []
    for flow_run in combined_flow_runs:
        flow_run_dict = {
            "id": flow_run.id,
            "flow_run_name": flow_run.name,
            "state": {
                "message": flow_run.state.message,
                "type": flow_run.state.type
            },
            "start_time": flow_run.start_time if flow_run.start_time else flow_run.expected_start_time,
            "end_time": flow_run.end_time,
            "total_duration": flow_run.total_run_time,
            "parameters": flow_run.parameters,
            "flow_id": flow_run.flow_id,
            "deployment_id": flow_run.deployment_id
        }
        flow_runs_info.append(flow_run_dict)

    flow_runs_info.sort(key=lambda x: x['start_time'], reverse=True)

    return flow_runs_info


@task
async def get_flow_run_info(flow_run_id: UUID) -> dict:
    try:
        async with get_client() as client:
            flow_run_info = await client.read_flow_run(flow_run_id)
    except exceptions.ObjectNotFound:
        return {
            "id": flow_run_id,
            "name": "Flow Run not found"
        }

    return flow_run_info


@task
async def get_flow_info(flow_id: UUID) -> dict:
    try:
        async with get_client() as client:
            flow_info = await client.read_flow(flow_id)
        flow_dict = {
            "id": flow_info.id,
            "name": flow_info.name,
        }
    except exceptions.ObjectNotFound:
        return {
            "id": flow_id,
            "name": "Flow not found"
        }

    return flow_dict


@task
async def get_deployment_info(deployment_id: UUID) -> dict:
    try:
        async with get_client() as client:
            deployment_info = await client.read_deployment(deployment_id) # pylint: disable=no-member

        deployment_dict = {
            "id": deployment_info.id,
            "name": deployment_info.name,
            "entrypoint": deployment_info.entrypoint,
            "description": deployment_info.description
        }
    except exceptions.ObjectNotFound:
        deployment_dict = {
            "id": deployment_id,
            "name": "Deployment not found",
            "entrypoint": "Deployment not found",
            "description": "Deployment not found"
        }

    return deployment_dict


@task
async def get_subflow_runs_info(
        parent_flow_run_id: UUID,
        states: Union[Sequence[str], Sequence[StateType], None] = None
    ) -> list[dict]:
    try:
        async with get_client() as client:
            subflow_info = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    parent_flow_run_id={'any_': [parent_flow_run_id]} if parent_flow_run_id else None,
                    state={'type': {'any_': states}} if states else None,
                )
            )
    except exceptions.ObjectNotFound:
        return []

    subflow_info_list = []
    for subflow in subflow_info:
        subflow_dict = {
            "id": subflow.id,
            "parent_flow_run_id": parent_flow_run_id,
            "state": {
                "message": subflow.state.message,
                "type": subflow.state.type
            },
            "start_time": subflow.start_time,
            "end_time": subflow.end_time,
            "total_duration": subflow.total_run_time,
            "parameters": subflow.parameters
        }

        subflow_info_list.append(subflow_dict)

    return subflow_info_list


def get_prefect_url():
    default = 'http://127.0.0.2:5000/'
    try:
        parsed_url = urlparse(runtime.flow_run.ui_url)
    except AttributeError:
        return default

    base_url = urlunparse(
        (parsed_url.scheme, parsed_url.netloc, '/', '', '', ''))
    return base_url if base_url else default


@task
async def schedule_executions_for_deploy(deploy_id: UUID, list_executions: list[datetime]) -> dict:
    try:
        async with get_client() as client:
            scheduled_flow_runs = await client.get_scheduled_flow_runs_for_deployments([deploy_id])

            # Convert pydantic datetime to regular datetime
            scheduled_times = [
                run.expected_start_time.replace(tzinfo=None)
                for run in scheduled_flow_runs
            ]

            # Convert input datetimes to same format
            list_executions = [
                dt.replace(tzinfo=None)
                for dt in list_executions
            ]

            if all(dt in scheduled_times for dt in list_executions):
                return {
                    "id": deploy_id,
                    "message": "All executions are already scheduled"
                }

            for dt in list_executions:
                flow_run_info = await client.create_flow_run_from_deployment(
                    deployment_id=deploy_id,
                    state=State(
                        type=StateType.SCHEDULED,
                        state_details=StateDetails(
                            scheduled_time=dt
                        )
                    ),
                    tags=["Programado por script"]
                )
    except exceptions.ObjectNotFound:
        return {
            "id": deploy_id,
            "name": "Deployment not found"
        }

    return flow_run_info


if __name__ == "__main__":
    # import pytz
    # tz_arg = pytz.timezone('America/Argentina/Buenos_Aires')
    # failed_flows_list = get_flow_runs_info(
    #     datetime(2024, 9, 1, 22, tzinfo=tz_arg),
    #     datetime(2024, 9, 2, 0, tzinfo=tz_arg),
    #     ["FAILED"])

    # for flow_info in failed_flows_list:
    #     print(f"Flow Run Name: {flow_info['flow_run_name']}")
    #     print(f"Start Time: {flow_info['start_time']}")
    #     print("--------------------")

    asyncio.run(
        schedule_executions_for_deploy(
                UUID('393be22b-8990-4110-a681-2475e9ec850b'),
                [datetime(2024, 12, 12, 11), datetime(2024, 12, 1, 11)]
            )
        )


# logger_global = PrefectLogger(__file__)