from prefect import flow, task

from consulterscommons.log_tools.prefect_log_config import PrefectLogger

logger_global = PrefectLogger(__file__)

@task
def mi_tarea(mensaje_tarea: str = ""):
    logger = logger_global.obtener_logger_prefect()
    logger.info("Iniciando tarea...")
    logger.info("Hola %s desde la tarea", mensaje_tarea)

    # Cambio el archivo de salida
    # logger = logger_global.cambiar_rotfile_handler_params(r"c:\Users\Lucas\Documents\Consulters\Electra\prefect-test\src\logeo\logs\hola.log")
    logger.info("Tarea finalizada...")

@flow
def mi_flujo(mensaje_flujo: str = ""):
    logger = logger_global.obtener_logger_prefect()
    logger.info("Hola %s desde el flujo", mensaje_flujo)
    mi_tarea(mensaje_flujo)

if __name__ == '__main__':
    mi_flujo("mundo")
    # mi_flujo.from_source(
    #     source="C:/Users/Lucas/Documents/Consulters/Electra/Python/prefect-test",
    #     entrypoint="src/logeo/test4.py:mi_flujo",
    # ).deploy(
    #     name="logeo-test4",
    #     work_pool_name="pool-dev",
    #     ignore_warnings=True
    # )
