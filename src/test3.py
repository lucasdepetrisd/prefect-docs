import os
# import logging
from prefect import flow, task

# from commons.log_config import obtener_nombre_script, inicializar_logger_prefect, obtener_logger_prefect
from commons.log_config import obtener_nombre_script, PrefectLogger

FILE_NAME = obtener_nombre_script()

os.environ["PREFECT_LOGGING_EXTRA_LOGGERS"] = FILE_NAME

# mylogger = logging.getLogger(FILE_NAME)
# mylogger.setLevel(logging.DEBUG)
# mylogger.propagate = True

logger_prefect = PrefectLogger(r"C:\Users\Lucas\Documents\Consulters\Electra\PythonTest\src\logs\test31.log")

@task
def mi_tarea():
    # mylogger.info("Iniciando tarea...")
    # mylogger.info("Tarea finalizada...")
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Iniciando tarea por prefect...")
    logger = logger_prefect.cambiar_rotfile_handler_params(r"C:\Users\Lucas\Documents\Consulters\Electra\PythonTest\src\logs\test32.log")
    logger.info("Tarea finalizada por prefect...")

@flow
def mi_flujo():
    # mylogger.info("Hola")
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Hola pero de prefect")
    mi_tarea()


if __name__ == '__main__':
    mi_flujo()
