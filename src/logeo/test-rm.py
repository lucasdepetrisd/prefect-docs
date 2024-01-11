import os
import logging
from prefect import flow, task, get_run_logger

# os.environ["PREFECT_LOGGING_EXTRA_LOGGERS"] = "test"
# os.environ["PREFECT_LOGGING_LOGGERS_PREFECT_FLOW_RUNS_LEVEL"] = "DEBUG"
# os.environ["PREFECT_LOGGING_LEVEL"] = "WARNING"

# logging.basicConfig(
#     format='%(asctime)s | %(levelname)-7s | %(name)s - %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S',
#     filename=r'C:/Reportes_Power_BI/Python/dev/Prefect_Test/logs/log_prefect.log',
#     encoding='utf-8',
#     filemode='a'
# )
# mylogger = logging.getLogger("test")
mylogger = logging.getLogger(__name__)
mylogger.propagate = True
# mylogger.setLevel(logging.INFO)
mylogger.setLevel(logging.DEBUG)


# file_handler = logging.FileHandler('C:/Reportes_Power_BI/Python/dev/Prefect_Test/logs/log_prefect.log', 'a')
# file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-7s | %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
# mylogger.addHandler(file_handler)

@task
def mi_tarea():
    logger = get_run_logger()
    mylogger.warning("Hola desde la tarea")
    logger.info("Hola desde la tarea por prefect")

@flow
def mi_flujo():
    logger = get_run_logger()
    mylogger.info("Hola desde el flujo")
    logger.info("Hola desde el flujo por prefect")
    mi_tarea()

if __name__ == '__main__':
    mi_flujo()