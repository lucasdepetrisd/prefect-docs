from prefect import flow, task

from consulterscommons.log_tools import PrefectLogger
from consulterscommons.log_tools.prefect_log_config import obtener_path_script

logger_prefect = PrefectLogger(__file__)

@task
def mi_tarea():
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Iniciando tarea por prefect...")
    logger.info("Tarea finalizada por prefect...")

@flow(timeout_seconds=60)
def my_flow():
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Hola pero de prefect")
    logger.info("Script path: %s", obtener_path_script(__file__))
    mi_tarea()


if __name__ == '__main__':
    # my_flow.serve(name="my-second-deployment",
    #                tags=["testing"])
    my_flow.from_source(
        source="C:/Users/Lucas/Documents/Consulters/Electra/Python/prefect-test",
        entrypoint="src/deploys/deploy.py:my_flow",
    ).deploy(
        name="deploys-deploy",
        work_pool_name="pool-dev",
        ignore_warnings=True,
        tags=["testing"]
    )
    # my_flow()
