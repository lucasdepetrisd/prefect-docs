import os
import logging
from prefect import flow, task

# print("PYTHONPATH:" + os.getenv("PYTHONPATH"))

from electracommons.log_config import PrefectLogger

# os.environ["PREFECT_LOGGING_EXTRA_LOGGERS"] = FILE_NAME

# mylogger = logging.getLogger(FILE_NAME)
# mylogger.setLevel(logging.DEBUG)
# mylogger.propagate = True

logger_prefect = PrefectLogger(__file__)

@task
def mi_tarea():
    # mylogger.info("Iniciando tarea...")
    # mylogger.info("Tarea finalizada...")
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Iniciando tarea por prefect...")
    # logger = logger_prefect.cambiar_rotfile_handler_params(r"C:\Users\Lucas\OneDrive\Consulters\Electra\ElectraTest\src\logeo\logs\test32.log")
    logger.info("Tarea finalizada por prefect...")

@flow
def mi_flujo():
    logger = logger_prefect.obtener_logger_prefect()
    # mylogger.info("Hola")
    logger.info("Hola pero de prefect")
    mi_tarea()


if __name__ == '__main__':
    mi_flujo()
