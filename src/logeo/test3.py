# import os
# import logging
import asyncio
from datetime import datetime
from prefect import flow, task

# print("PYTHONPATH:" + os.getenv("PYTHONPATH"))

# os.environ["PREFECT_LOGGING_EXTRA_LOGGERS"] = FILE_NAME

# mylogger = logging.getLogger(FILE_NAME)
# mylogger.setLevel(logging.DEBUG)
# mylogger.propagate = True

# logger_prefect = PrefectLogger(__file__)

from dev.MONITOREO_PREFECT.get_prefect_info import get_flow_runs_info


@task
def my_task(name: str):
    # mylogger.info("Iniciando tarea...")
    # mylogger.info("Tarea finalizada...")
    # logger = logger_prefect.obtener_logger_prefect()
    # logger.info("Iniciando tarea por prefect...")
    # logger = logger_prefect.cambiar_rotfile_handler_params(r"C:\Users\Lucas\OneDrive\Consulters\Electra\prefect-test\src\logeo\logs\test32.log")
    # logger.info("Tarea finalizada por prefect...")
    print(f"Hi {name}! you are starting task")


@flow
def my_flow():
    # logger = logger_prefect.obtener_logger_prefect()
    # mylogger.info("Hola")
    # logger.info("Hola pero de prefect")
    # my_task("Flow")
    list_dicts_flows = asyncio.run(get_flow_runs_info(datetime(2024, 9, 1, 22), datetime.today(), ["FAILED"]))
    # print(list_dicts_flows)
    # my_task.map(list_names)


if __name__ == '__main__':
    my_flow()
    # my_flow.from_source(
    #     source="C:/Users/ldepetris/Documents/prefect-test/",
    #     entrypoint="src/logeo/test3.py:my_flow",
    # ).deploy(
    #     name="my-deploy",
    #     work_pool_name="pool-dev",
    #     ignore_warnings=True,
    #     tags=["test"]
    # )
