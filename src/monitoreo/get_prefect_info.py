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

import requests
from datetime import timezone
from urllib.parse import urljoin
import asyncio
from typing import Union, Sequence
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from uuid import UUID

from prefect import runtime, task, get_client
from prefect.cli import config as cli_config
from prefect.client.schemas.objects import StateType, StateDetails
from prefect.states import State
from prefect import exceptions

# Se podria filtrar en las funciones de get_xxx_info utilizando los filtros de prefect pero no lo vi necesario
# ya que se puede hacer con diccionarios y leyendo la documentación de prefect en {url de prefect}/docs
from prefect.server.schemas.filters import (
    FlowRunFilter,
    TaskRunFilter
    # FlowRunFilterState,
    # FlowRunFilterStateType,
    # FlowRunFilterStartTime,
)


def get_prefect_server_settings():
    """Get Prefect API settings including limits."""
    base_url = get_prefect_url()  # Base URL from your function
    endpoint = "/api/admin/settings"  # Endpoint path

    # Use urljoin to safely concatenate base URL and endpoint
    url = urljoin(base_url.rstrip('/'), endpoint.lstrip('/'))

    try:
        response = requests.get(url, headers={'accept': 'application/json'}, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get API settings: {response.status_code}")
    except Exception as e:
        raise Exception(f"Error fetching API settings: {str(e)}")


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

    # Ensure start_date and end_date are timezone-aware
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    settings = get_prefect_server_settings()
    api_limit = settings.get('PREFECT_API_DEFAULT_LIMIT', 200)

    async def fetch_paginated_data(start_date, end_date, without_start_time=False):
        """Fetch data from API with pagination."""
        results = []
        current_start_date = start_date

        while current_start_date < end_date:
            # Update the filter dynamically based on whether we are handling without start time
            if without_start_time:
                flow_run_filter = FlowRunFilter(
                    state={
                        'type': {'any_': states} if states else None,
                    },
                    expected_start_time={
                        'after_': current_start_date,
                        'before_': end_date
                    },
                    start_time={
                        'is_null_': True
                    }
                )
            else:
                flow_run_filter = FlowRunFilter(
                    state={
                        'type': {'any_': states} if states else None,
                    },
                    start_time={
                        'after_': current_start_date,
                        'before_': end_date
                    }
                )

            async with get_client() as client:
                flow_runs = await client.read_flow_runs(
                    flow_run_filter=flow_run_filter,
                    sort="START_TIME_ASC",
                    limit=api_limit
                )

            results.extend(flow_runs)

            if len(flow_runs) < api_limit:
                break

            # Update start date for next page
            flow_runs.sort(key=lambda x: x.start_time or x.expected_start_time)
            last_retrieved_time = flow_runs[-1].start_time or flow_runs[-1].expected_start_time
            current_start_date = last_retrieved_time

        return results

    # Fetch data for flow runs with and without start time
    flow_runs = await fetch_paginated_data(start_date, end_date, without_start_time=False)
    flow_runs_without_start_time = await fetch_paginated_data(start_date, end_date, without_start_time=True)

    # Combine and process data
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
async def get_task_runs_info(
            start_date: datetime,
        end_date: datetime,
        states: Union[Sequence[str], Sequence[StateType], None] = None
    ) -> list[dict]:
    """
    Obtiene información de ejecuciones de tarea dentro de un rango de fechas y estados específicos.
    La zona horaria corresponde a UTC.

    Parámetros:
    - start_date (datetime): Fecha de inicio del rango de fechas.
    - end_date (datetime): Fecha de fin del rango de fechas.
    - states (list[str], opcional): Lista de estados de ejecución a filtrar. Por defecto es None.
    Retorna:
    - list[dict]: Lista de diccionarios con información de las ejecuciones de tarea.
    """

    # Ensure start_date and end_date are timezone-aware
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    settings = get_prefect_server_settings()
    api_limit = settings.get('PREFECT_API_DEFAULT_LIMIT', 200)

    async def fetch_paginated_data(start_date, end_date, without_start_time=False):
        """Fetch data from API with pagination."""
        results = []
        current_start_date = start_date

        while current_start_date < end_date:
            # Update the filter dynamically based on whether we are handling without start time
            if without_start_time:
                task_run_filter = TaskRunFilter(
                    state={
                        'type': {'any_': states} if states else None,
                    },
                    expected_start_time={
                        'after_': current_start_date,
                        'before_': end_date
                    },
                    start_time={
                        'is_null_': True
                    }
                )
            else:
                task_run_filter = TaskRunFilter(
                    state={
                        'type': {'any_': states} if states else None,
                    },
                    start_time={
                        'after_': current_start_date,
                        'before_': end_date
                    }
                )

            async with get_client() as client:
                task_runs = await client.read_task_runs(
                    task_run_filter=task_run_filter,
                    sort="EXPECTED_START_TIME_ASC",
                    limit=api_limit
                )

            results.extend(task_runs)

            if len(task_runs) < api_limit:
                break

            # Update start date for next page
            task_runs.sort(key=lambda x: x.start_time or x.expected_start_time)
            last_retrieved_time = task_runs[-1].start_time or task_runs[-1].expected_start_time
            current_start_date = last_retrieved_time

        return results

    # Fetch data for task runs with and without start time
    task_runs = await fetch_paginated_data(start_date, end_date, without_start_time=False)
    task_runs_without_start_time = await fetch_paginated_data(start_date, end_date, without_start_time=True)

    # Combine and process data
    combined_flow_runs = task_runs + task_runs_without_start_time
    task_runs_info = []

    for task_run in combined_flow_runs:
        task_run_dict = {
            "id": task_run.id,
            "flow_run_name": task_run.name,
            "state": {
                "message": task_run.state.message,
                "type": task_run.state.type
            },
            "start_time": task_run.start_time if task_run.start_time else task_run.expected_start_time,
            "end_time": task_run.end_time,
            "total_duration": task_run.total_run_time,
            "parameters": task_run.parameters,
            "flow_id": task_run.flow_id,
            "deployment_id": task_run.deployment_id
        }
        task_runs_info.append(task_run_dict)

    task_runs_info.sort(key=lambda x: x['start_time'], reverse=True)

    return task_runs_info


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
            "source": deployment_info.pull_steps[0].get('prefect.deployments.steps.git_clone', {}),
            "description": deployment_info.description,
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
    """
    Obtener la URL de Prefect UI.
    Debe ser ejecutada desde un flujo o tarea de Prefect. Sino levanta una excepción.
    """
    default = 'http://192.168.1.13:442/'
    try:
        parsed_url = urlparse(runtime.flow_run.ui_url)
        components = (
            str(parsed_url.scheme or ''),
            str(parsed_url.netloc or ''),
            '/',
            '',
            '',
            ''
        )
        base_url = urlunparse(components)
        return base_url if base_url and base_url != '/' else default
    except (AttributeError, TypeError):
        return default

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
    asyncio.run(get_flow_runs_info(datetime(2024, 10, 1), datetime(2024, 10, 15)))
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

    # asyncio.run(
    #     schedule_executions_for_deploy(
    #             UUID('393be22b-8990-4110-a681-2475e9ec850b'),
    #             [datetime(2024, 12, 12, 11), datetime(2024, 12, 1, 11)]
    #         )
    #     )


# logger_global = PrefectLogger(__file__)