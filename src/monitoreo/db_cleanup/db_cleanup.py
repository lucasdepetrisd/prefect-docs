import pytz
from datetime import datetime
import asyncio

from prefect import flow
from prefect import get_client

from lucasdp.log_tools import PrefectLogger

from dev.MONITOREO_PREFECT.get_prefect_info import get_flow_runs_info, get_task_runs_info

logger_global = PrefectLogger(__file__)

@flow
async def db_cleanup(
        fecha_inicio: datetime,
        fecha_fin: datetime,
        timezone_str: str = 'America/Argentina/Buenos_Aires'
    ) -> None:

    logger = logger_global.obtener_logger_prefect()

    # Inicializar y localizar
    timezone = pytz.timezone(timezone_str)
    fecha_inicio = timezone.localize(fecha_inicio)
    fecha_fin = timezone.localize(fecha_fin)

    # Verificar que la fecha final sea mayor o igual a la fecha inicial
    if fecha_fin < fecha_inicio:
        raise ValueError("La fecha final debe ser mayor o igual a la fecha inicial.")

    # Obtener los flujos en el rango de fechas especificado
    try:
        logger.info("Se obtendran los flujos desde %s hasta %s.", fecha_inicio, fecha_fin)
        flow_runs = await get_flow_runs_info(fecha_inicio, fecha_fin)
        logger.info("Se obtuvieron %s flujos.", len(flow_runs))
        # failed_flow_runs = pd.DataFrame()
    except Exception as e:
        logger.error("Error en obtencion de datos: %s", e)
        raise e

    if len(flow_runs) != 0:
        flow_runs.sort(key=lambda x: x['start_time'])

        # Eliminar los flujos obtenidos
        try:
            logger.info("Se eliminaran los flujos obtenidos.")
            async with get_client() as client:
                for flow_run in flow_runs:
                    await client.delete_flow_run(flow_run['id'])
                    logger.info("Flujo %s eliminado.", flow_run['id'])
        except Exception as e:
            logger.error("Error en eliminacion de flujos: %s", e)
            raise e

        logger.info("Flujos eliminados.")
    else:
        logger.info("No hay flujos para eliminar.")

    logger.info("Ejecución finalizada.")

if __name__ == '__main__':
    asyncio.run(db_cleanup(datetime(2024, 10, 15), datetime(2024, 10, 16)))
