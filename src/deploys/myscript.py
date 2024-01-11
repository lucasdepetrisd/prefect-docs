import os
import logging
from prefect import flow, task

# print("PYTHONPATH:" + os.getenv("PYTHONPATH"))

from electracommons.log_config import FILE_NAME, PrefectLogger

logger_prefect = PrefectLogger()

@task
def mi_tarea():
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Iniciando tarea por prefect...")
    logger.info("Tarea finalizada por prefect...")

@flow
def mi_flujo():
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Hola pero de prefect")
    mi_tarea()


if __name__ == '__main__':
    mi_flujo()
