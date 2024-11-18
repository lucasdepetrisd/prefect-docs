"""
Script para generar un reporte periódico de flujos fallidos en Prefect.

Este script utiliza la biblioteca Prefect para obtener información sobre los flujos fallidos en un rango de fechas especificado y 
enviar un informe por correo electrónico a los destinatarios especificados.

Para que el script funcione correctamente, se debe configurar la URL de la API de Prefect utilizando el siguiente comando:
    prefect config set PREFECT_API_URL='http://192.168.1.13:442/api'

El script consta de las siguientes funciones y tareas:
    - `get_failed_flow_runs(start_date: datetime, end_date: datetime) -> list[dict]`: Esta tarea obtiene los flujos fallidos en el rango de fechas especificado y realiza mapeos de información adicional.
    - `send_report_failed_flows(failed_flow_runs, destinatarios, fecha_ejecucion, exec_type: str)`: Esta tarea envía un informe por correo electrónico con los flujos fallidos y otra información relevante.
    - `generar_reporte_prefect(destinatarios: str, tipo_ejecucion: str = "semanal", fecha_ejecucion: datetime = None)`: Esta función de flujo principal genera el informe periódico de flujos fallidos en Prefect.

    El informe puede generarse de forma semanal, mensual o diaria, y se envía por correo electrónico a los destinatarios especificados.
    - `destinatarios` (str): Lista de correos electrónicos de los destinatarios del informe.
    - `tipo_ejecucion` (str, opcional): Tipo de ejecución del informe. Puede ser "semanal", "mensual" o "diario". Por defecto es "semanal".
    - `fecha_ejecucion` (datetime, opcional): Fecha de ejecución del informe. Si no se proporciona, se utiliza la fecha y hora actual.
    - `ValueError`: Si el tipo de ejecución no es reconocido.
"""

from typing import Union, Optional, Sequence
from datetime import datetime, timedelta
import asyncio

import pytz
import pandas as pd

from prefect import flow, task, unmapped
from prefect.variables import Variable
from prefect.client.schemas.objects import StateType

from consulterscommons.log_tools import PrefectLogger

from dev.MONITOREO_PREFECT.periodic_report.tipo_ejecucion import TipoEjecucion
from dev.MONITOREO_PREFECT.periodic_report.extract_metadata import extract_metadata
from dev.MONITOREO_PREFECT.periodic_report.send_report_failed_flows import send_report_failed_flows
from dev.MONITOREO_PREFECT.get_prefect_info import get_flow_runs_info, get_prefect_url, get_subflow_runs_info, get_flow_info, get_deployment_info

logger_global = PrefectLogger(__file__)


# Convertir a funcion generica para diferentes estados
@task
def get_failed_flow_runs(
        start_date: datetime,
        end_date: datetime,
        states_to_check: Union[Sequence[str], Sequence[StateType]]
    ) -> pd.DataFrame:
    logger = logger_global.obtener_logger_prefect()

    start_date = start_date.astimezone(pytz.utc)
    end_date = end_date.astimezone(pytz.utc)

    logger.info("Obteniendo flujos fallidos desde %s hasta %s.", start_date, end_date)

    failed_flow_runs = asyncio.run(get_flow_runs_info(start_date, end_date, states_to_check))

    logger.info("Se obtuvieron %s flujos fallidos.", len(failed_flow_runs))
    logger.debug(failed_flow_runs)

    if not failed_flow_runs:
        return pd.DataFrame()

    logger.debug("Obteniendo información adicional de los flujos fallidos.")

    # Obtengo información adicional de los flujos y despliegues
    flow_run_ids = [flow_run['id'] for flow_run in failed_flow_runs]
    flow_ids = [flow_run['flow_id'] for flow_run in failed_flow_runs]
    deployment_ids = [flow_run['deployment_id'] for flow_run in failed_flow_runs if flow_run['deployment_id']]

    # Quito duplicados
    flow_ids = list(set(flow_ids))
    deployment_ids = list(set(deployment_ids))

    failed_flow_runs_df = pd.DataFrame(failed_flow_runs)

    # ------------------------------------------------
    # Obtengo información de los subflujos
    subflow_info_future = get_subflow_runs_info.map(flow_run_ids, unmapped(states_to_check))
    subflow_info_lists = subflow_info_future.result()

    # Aplano la lista de listas de subflujos
    subflow_info_list = [
        subflow
        for subflows in subflow_info_lists
        for subflow in subflows
    ]

    logger.debug(subflow_info_list)

    # Se lee asi:
    # Por cada id de flujo fallido realizo una función
    # En la función busco en la lista de subflujos el id que coincida con el id del flujo fallido
    # De esa manera agrego a la fila del flujo fallido el parent_flow_run_id.
    # next() devuelve el primer valor que cumple la condición, si no encuentra nada devuelve None
    # Basicamente acá se esta haciendo un left join entre failed_flow_runs_df y subflow_info_list
    failed_flow_runs_df['parent_flow_run_id'] = failed_flow_runs_df['id'].map(
        lambda flow_id: next((subflow['parent_flow_run_id'] for subflow in subflow_info_list if subflow['id'] == flow_id), None)
    )

    logger.info("Se obtuvo información de subflujos.")

    # ------------------------------------------------
    # Obtengo información de los flujos
    flows_info_future = get_flow_info.map(flow_ids)
    flow_info_list = flows_info_future.result()

    flow_info_dict = {info['id']: info for info in flow_info_list}

    failed_flow_runs_df['flow_name'] = failed_flow_runs_df['flow_id'].map(lambda id: flow_info_dict[id]['name'])

    # ------------------------------------------------
    # Obtengo información de los despliegues
    deploys_info_future = get_deployment_info.map(deployment_ids)
    deploys_info_list = deploys_info_future.result()

    deployment_info_dict = {info['id']: info for info in deploys_info_list}

    deployment_info = failed_flow_runs_df['deployment_id'].map(deployment_info_dict.get)
    failed_flow_runs_df['deployment_name'] = deployment_info.map(lambda x: x['name'] if x else None)
    failed_flow_runs_df['deployment_entrypoint'] = deployment_info.map(lambda x: x['entrypoint'] if x else None)
    failed_flow_runs_df['deployment_description'] = deployment_info.map(lambda x: x['description'] if x else None)

    logger.info("Se obtuvo información de despliegues.")

    # ------------------------------------------------
    # Deprecado
    # Obtengo la metadata de los despliegues asociadas
    # ------------------------------------------------
    # metadata_info_future = metadata_utils.get_metadata.map(failed_flow_runs_df['deployment_id'])
    # metadata_info_list = metadata_info_future.result()

    # failed_flow_runs_df['deployment_metadata'] = metadata_info_list

    # logger.info("Se obtuvo información de metadatos.")

    # ------------------------------------------------
    # Extrae y parsea metadatos YAML de cada descripción

    # Aplica la función de parsing a cada descripción y almacena los metadatos en una columna
    failed_flow_runs_df['deployment_metadata'] = failed_flow_runs_df['deployment_description'].apply(lambda x: extract_metadata(x) if x and x != 'None' else {})

    logger.info("Se obtuvo información de metadatos desde YAML en la descripción.")

    # ------------------------------------------------
    # Actualizar Ids de responsables con información completa
    dict_devs = Variable.get("devs_responsables", default={})

    def update_responsable_metadata(metadata: dict) -> dict:
        if not isinstance(metadata, dict):
            return metadata

        # Get responsable ID and replace with full info if exists
        if (responsable_id := metadata.get('responsable')):
            if (responsable_info := dict_devs.get(responsable_id)):
                metadata['responsable'] = responsable_info

        return metadata

    failed_flow_runs_df['deployment_metadata'] = failed_flow_runs_df['deployment_metadata'].apply(lambda x: update_responsable_metadata(x) if x else {})

    logger.debug("Metadatos actualizados con información de responsables.")

    # ------------------------------------------------
    # Obtengo la URL base de la UI de Prefect
    ui_url = get_prefect_url()

    failed_flow_runs_df['ui_url'] = failed_flow_runs_df['id'].map(lambda x: f"{ui_url}flow-runs/flow-run/{x}")

    return failed_flow_runs_df


@task
def calculate_dates(tipo_ejecucion: TipoEjecucion, fecha_ejecucion: datetime) -> tuple[datetime, datetime]:
    if tipo_ejecucion == TipoEjecucion.DIARIA:
        # Calcular el inicio del día
        fecha_inicial = fecha_ejecucion.replace(hour=0, minute=0, second=0, microsecond=0)
        fecha_final = fecha_inicial.replace(hour=23, minute=59, second=59, microsecond=999000)
    elif tipo_ejecucion == TipoEjecucion.SEMANAL:
        # Calcular el lunes de la semana actual a las 00:00:00 y el domingo a las 23:59:59
        fecha_inicial = fecha_ejecucion - timedelta(days=fecha_ejecucion.weekday())
        fecha_inicial = fecha_inicial.replace(hour=0, minute=0, second=0, microsecond=0)
        fecha_final = fecha_inicial + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999000)
    elif tipo_ejecucion == TipoEjecucion.MENSUAL:
        # Calcular desde el día 1 a las 00:00:00 hasta el último día a las 23:59:59
        fecha_inicial = fecha_ejecucion.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        siguiente_mes = fecha_inicial + timedelta(days=32)
        ultimo_dia = siguiente_mes.replace(day=1) - timedelta(days=1)
        fecha_final = ultimo_dia.replace(hour=23, minute=59, second=59, microsecond=999000)
    else:
        raise ValueError(f"Tipo de ejecución '{tipo_ejecucion}' no reconocido.")

    return fecha_inicial, fecha_final


@flow
def prefect_periodic_report(
        destinatarios: Union[str, Sequence[str]],
        tipo_ejecucion: TipoEjecucion = TipoEjecucion.SEMANAL,
        fecha_ejecucion: Optional[datetime] = None,
        fecha_final: Optional[datetime] = None,
        timezone_str: str = "America/Argentina/Buenos_Aires",
        states_to_check: Sequence[StateType] = ("FAILED", "CRASHED")
    ) -> None:
    """Genera un reporte periódico de flujos fallidos en Prefect.

    Args:
        destinatarios (Union[str, Sequence[str]]): Lista de correos electrónicos de los destinatarios del reporte.
        tipo_ejecucion (TipoEjecucion, optional): Tipo de ejecución del reporte. Valores posibles en TipoEjecucion enum.
            Defaults to TipoEjecucion.SEMANAL.
        fecha_ejecucion (datetime, optional): Fecha de ejecución del reporte.
            Para reportes periódicos: fecha final del periodo.
            Para reportes personalizados: fecha inicial del periodo.
            Defaults to datetime.now().
        fecha_final (datetime, optional): Fecha final del rango. Requerido solo para reportes personalizados.
            Defaults to None.
        timezone_str (str, optional): Zona horaria para las fechas.
            Defaults to "America/Argentina/Buenos_Aires".
        states_to_check (Sequence[StateType], optional): Estados de flujo a considerar.
            Defaults to ("FAILED", "CRASHED").

    Raises:
        ValueError: Si el tipo de ejecución no es reconocido o las fechas son inválidas.

    Returns:
        None

    Notes:
        Los estados posibles son: "FAILED", "CRASHED", "TIMED_OUT", "CANCELLED", 
        "COMPLETED", "PENDING", "SCHEDULED"

    ---
    metadata:
        responsable: LD
        script_path: "dev/MONITOREO_PREFECT/periodic_report/prefect_periodic_report.py"
        area: Monitoreo
    """

    logger = logger_global.obtener_logger_prefect()

    # Inicializar y localizar
    timezone = pytz.timezone(timezone_str)
    if fecha_ejecucion is None:
        fecha_ejecucion = datetime.now()
    elif not isinstance(fecha_ejecucion, datetime):
        raise ValueError("fecha_ejecucion debe ser de tipo datetime.")

    fecha_ejecucion = fecha_ejecucion.astimezone(timezone)

    # Validación y asignación de fechas según el tipo de ejecución
    if tipo_ejecucion == TipoEjecucion.PERSONALIZADA:
        if fecha_final is None:
            raise ValueError("fecha_final debe ser especificada para reportes personalizados.")
        if not isinstance(fecha_final, datetime):
            raise ValueError("fecha_final debe ser de tipo datetime cuando es especificada.")
        fecha_final = fecha_final.astimezone(timezone)
        fecha_inicial = fecha_ejecucion
    elif tipo_ejecucion in [TipoEjecucion.DIARIA, TipoEjecucion.SEMANAL, TipoEjecucion.MENSUAL]:
        if fecha_final is not None:
            logger.warning("fecha_final solo puede ser especificada en reportes personalizados. No se tendrá en cuenta.")
        # Calcular fechas inicial y final para tipos de ejecución periódicos
        fecha_inicial, fecha_final = calculate_dates(tipo_ejecucion, fecha_ejecucion)
    else:
        raise ValueError(f"Tipo de ejecución '{tipo_ejecucion}' no reconocido.")

    # Verificar que la fecha final sea mayor o igual a la fecha inicial
    if fecha_final < fecha_inicial:
        raise ValueError("La fecha final debe ser mayor o igual a la fecha inicial.")

    # Obtener los flujos fallidos en el rango de fechas especificado
    try:
        logger.info("Se obtendra el reporte de flujos fallidos desde %s hasta %s.", fecha_inicial, fecha_final)
        failed_flow_runs = get_failed_flow_runs(fecha_inicial, fecha_final, states_to_check)
        # failed_flow_runs = pd.DataFrame()
    except Exception as e:
        logger.error("Error en obtencion de datos: %s", e)
        raise e

    # Verificar si no se encontraron flujos fallidos
    if failed_flow_runs.empty:
        logger.info("No se encontraron flujos fallidos en el rango de fechas especificado. Se enviara un email vacio.")
        email_status = send_report_failed_flows(failed_flow_runs, destinatarios, fecha_inicial, fecha_final, exec_type=tipo_ejecucion)
        return

    failed_flow_runs.sort_values(by='start_time', inplace=True)

    # Loggeo de flujos fallidos
    try:
        for _, flow_run in failed_flow_runs.iterrows():
            mensaje_log = (
                f"Flujo fallido: {flow_run['deployment_name'] + ':' if flow_run['deployment_name'] is not None else ''}{flow_run['flow_name']}:{flow_run['flow_run_name']} - "
                f"ID: {flow_run['id']} - "
                f"Fecha de inicio: {flow_run['start_time']} - "
                f"Detalles: {flow_run['ui_url']}"
            )
            logger.info(mensaje_log)
    except Exception as e:
        logger.error("Error en loggeo de datos: %s", e)
        raise e

    # Envío de email con el reporte de flujos fallidos
    try:
        email_status = send_report_failed_flows(
            failed_flow_runs, destinatarios, fecha_inicial, fecha_final=fecha_final, exec_type=tipo_ejecucion)
        logger.info(email_status)
        logger.info("Para mas informacion visita el mail enviado a %s.", destinatarios)
    except Exception as e:
        logger.error("Error enviando email: %s", e)
        raise e


if __name__ == "__main__":
    # pass
    prefect_periodic_report(
        "lucas.depetris@consulters.com.ar",
        tipo_ejecucion=TipoEjecucion.DIARIA,
        fecha_ejecucion=datetime(2024, 11, 6),
        timezone_str="America/Argentina/Buenos_Aires",
        states_to_check=["FAILED", "CRASHED"]
    )

    from prefect.runner.storage import GitRepository
    from prefect.blocks.system import Secret
    from prefect.client.schemas.schedules import CronSchedule

    # Deploy de ejecución diaria
    prefect_periodic_report.from_source(
        source=GitRepository(
            url="https://github.com/lucasdepetrisd/prefect-test.git",
            credentials={"access_token": Secret.load("pat-access-token")},
            branch="main"
        ),
        entrypoint="src/monitoreo/periodic_report/prefect_periodic_report.py:prefect_periodic_report"
    ).deploy(
        name="Prefect Reporte Diario",
        work_pool_name="pool-dev",
        schedules=[CronSchedule(cron="55 23 * * 1-5", timezone="America/Buenos_Aires")],
        parameters={
            "destinatarios": ["lucasdepetris14@gmail.com"],
            "tipo_ejecucion": "diaria",
        },
        tags=['Dev', 'Monitoreo'],
        ignore_warnings=True
    )

    # Deploy de ejecución semanal
    deploy_id = prefect_periodic_report.from_source(
        source=GitRepository(
            url="https://github.com/lucasdepetrisd/prefect-test.git",
            credentials={"access_token": Secret.load("pat-access-token")},
            branch="main"
        ),
        entrypoint="src/monitoreo/periodic_report/prefect_periodic_report.py:prefect_periodic_report"
    ).deploy(
        name="Prefect Reporte Semanal",
        work_pool_name="pool-dev",
        schedules=[CronSchedule(cron="55 23 * * 0", timezone="America/Buenos_Aires")],
        parameters={
            "destinatarios": ["lucasdepetris14@gmail.com"],
            "tipo_ejecucion": "semanal",
        },
        tags=['Dev', 'Monitoreo'],
        ignore_warnings=True
    )

    