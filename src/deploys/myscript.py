from prefect import flow, task
from prefect.logging import get_run_logger
import os

# print("PYTHONPATH:" + os.getenv("PYTHONPATH"))

# from electracommons.log_config import CALLER_SCRIPT_PATH, FILE_PATH, PrefectLogger

# logger_prefect = PrefectLogger()

@task
def mi_tarea():
    # logger = logger_prefect.obtener_logger_prefect()
    logger = get_run_logger()
    logger.info("Iniciando tarea por prefect...")
    logger.info("Tarea finalizada por prefect...")

@flow
def mi_flujo(mensaje="Predeterminado"):
    # logger = logger_prefect.obtener_logger_prefect()
    logger = get_run_logger()
    logger.info("Hola pero de prefect")
    logger.info("Tengo un mensaje: %s", mensaje)
    logger.info("Script path: %s", os.path.abspath(__file__))
    # logger.info("SCRIPT_PATH: %s", CALLER_SCRIPT_PATH)
    mi_tarea()


if __name__ == '__main__':
    # mi_flujo.serve(name="my-first-deployment",
    #                               tags=["testing"],
    #                               parameters={"mensaje": "Hola!"},
    #                               interval=60)
    mi_flujo()
