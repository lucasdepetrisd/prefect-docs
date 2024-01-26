import logging
import os

SCRIPT_NAME = os.path.basename(os.path.splitext(__file__)[0])

# os.environ["PREFECT_LOGGING_EXTRA_LOGGERS"] = SCRIPT_NAME

# print(f"{os.path.splitext(__file__)[0]} {os.path.splitext(__file__)[0]}")

logging.basicConfig(
    format='%(asctime)s,%(msecs)03d %(name)-8s %(levelname)-8s : %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=fr'{os.path.splitext(__file__)[0]}.log',
    encoding='utf-8',
    filemode='a',
    level=logging.INFO
)
# mylogger = logging.getLogger(SCRIPT_NAME)
mylogger = logging.getLogger('custom')
mylogger.setLevel(logging.INFO)

from prefect import flow, task, get_run_logger

@task
def mi_tarea():
    mylogger.info("Iniciando tarea...") 
    mylogger.info("Tarea finalizada...")

@flow
def mi_flow():
    mylogger.info("Hola")
    logger = get_run_logger()
    logger.info("Hola pero de prefect")
    mi_tarea()

if __name__ == '__main__':
    # mi_flow.serve(name="mi-deploy")
    mi_flow()

# @task
# def mi_tarea(mensaje_tarea: str = ""):
#     logger = get_run_logger()
#     logger.info("Hola %s desde la tarea", mensaje_tarea)

# @flow
# def mi_flujo(mensaje_flujo: str = ""):
#     logger = get_run_logger()
#     logger.info("Hola %s desde el flujo", mensaje_flujo)
#     mi_tarea(mensaje_flujo)

# if __name__ == '__main__':
#     mi_flujo("mundo")