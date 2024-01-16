from prefect import task, flow
from datetime import datetime

from electracommons.log_config import PrefectLogger

logger_prefect = PrefectLogger(__file__)

@task(name="Printear Mensaje")
def print_message(message):
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Iniciando tarea...")
    print(message)

@flow(name="Flujo Básico", version="v1.0", flow_run_name=f"{datetime.strftime(datetime.now(), "%d/%m/%Y - %H:%M:%S")} | Flujo Run")
def basic_flow(message="Hello, Prefect!"):
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Iniciando flujo...")
    print_message(message)

if __name__ == "__main__":
    basic_flow()
    # basic_flow.serve(name="my-basic-deploy",
    #                  tags=["testing"],
    #                  parameters={"message": "Hola"})
        # mi_flujo.serve(name="my-first-deployment",
    #                               tags=["testing"],
    #                               parameters={"mensaje": "Hola!"},
    #                               interval=60)
