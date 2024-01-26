import asyncio
from prefect import flow, task, get_run_logger

# @flow
# async def subflow_1():
#     print("Subflow 1 started!")
#     await asyncio.sleep(1)

# @flow
# async def subflow_2():
#     print("Subflow 2 started!")
#     await asyncio.sleep(1)

# @flow
# async def subflow_3():
#     print("Subflow 3 started!")
#     await asyncio.sleep(1)

# @flow
# async def subflow_4():
#     print("Subflow 4 started!")
#     await asyncio.sleep(1)

# @flow
# async def main_flow():
#     parallel_subflows = [subflow_1(), subflow_2(), subflow_3(), subflow_4()]
#     await asyncio.gather(*parallel_subflows)

# if __name__ == "__main__":
#     main_flow_state = asyncio.run(main_flow())


@task
async def print_text(mensaje: str):
    logger = get_run_logger()
    print(mensaje)
    logger.info("Se printeo el mensaje: %s", mensaje)

@flow
async def subflow_1():
    await print_text("Subflow 1 started!")
    await asyncio.sleep(1)

@flow
async def subflow_2():
    await print_text("Subflow 2 started!")
    await asyncio.sleep(1)

@flow
async def main_flow():
    parallel_subflows = [subflow_1(), subflow_2()]
    await asyncio.gather(*parallel_subflows)

if __name__ == "__main__":
    main_flow_state = asyncio.run(main_flow())