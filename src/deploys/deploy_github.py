
from prefect import flow
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository
from prefect.client.schemas.schedules import CronSchedule

@flow
def test():
    print("Hello World!")


# async def main(access_token):
#     dict_dir = await git_clone(
#         repository="https://github.com/DesarrollosElectra/ProyectosPython.git",
#         branch="main",
#         access_token=access_token
#     )
#     return dict_dir


if __name__ == '__main__':
    # load_pbi_accesses()

    # _access_token = Secret.load("desarrollos-electra-access-token").get()
    # print(_access_token)

    # dict_dir = asyncio.run(main(_access_token))

    # print(dict_dir)

    test.from_source(
        source=GitRepository(
            url="https://github.com/lucasdepetrisd/prefect-test.git",
            credentials={"access_token": Secret.load("github-pull-step", _sync=True)},
            branch="main",
            # include_submodules=True
        ),
        entrypoint=r"src\deploys\deploy_github.py:test"
    ).deploy(
        name="Test",
        work_pool_name="pool-dev",
        schedules=[CronSchedule(cron="0 22 * * *", timezone="America/Buenos_Aires")],
        # parameters={
        #     "destinatarios": "lucas.depetris@consulters.com.ar",
        #     "tipo_ejecucion": "semanal"
        # },
        tags=['Dev', 'Monitoreo'],
        ignore_warnings=True
    )