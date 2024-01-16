import asyncio
from prefect import flow
# from prefect.concurrency import asyncio

from electracommons.log_config import PrefectLogger

logger_prefect = PrefectLogger(__file__)

@flow(name="Subflujo 1")
async def subflow_1():
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Subflow 1 started!")
    await asyncio.sleep(5)

@flow(name="Subflujo 2")
async def subflow_2():
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Subflow 2 started!")
    await asyncio.sleep(180)

@flow(name="Subflujo 3")
async def subflow_3():
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Subflow 3 started!")
    await asyncio.sleep(3)

@flow(name="Subflujo 4")
async def subflow_4():
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Subflow 4 started!")
    await asyncio.sleep(2)

@flow
async def main_flow():
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Main Flow started!")
    parallel_subflows = [subflow_1(), subflow_2(), subflow_3(), subflow_4()]
    await asyncio.gather(*parallel_subflows)
    # await asyncio.

if __name__ == "__main__":
    asyncio.run(main_flow())
    # asyncio.run(main_flow.serve(
    #     name="my-basic-deploy",
    #     tags=["testing"],
    #     parameters={"message": "Hola"}
    # ))
