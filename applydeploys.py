from src.deploys.project import basic
from prefect.deployments import Deployment

DEPLOYMENTS = [
Deployment.build_from_flow(
    flow=my_flow, 
    name='deploy_1'
    ),
Deployment.build_from_flow(
    flow=my_flow2, 
    name='deploy_2'
    )
...
]
if __name__ == '__main__':
    for deployment in DEPLOYMENTS:
        deployment.apply()

deployment = Deployment.build_from_flow(
    flow=log_flow,
    name="log-simple",
    parameters={"name": "Marvin"},
    infra_overrides={"env": {"PREFECT_LOGGING_LEVEL": "DEBUG"}},
    work_queue_name="test",
)

if __name__ == "__main__":
    deployment.apply()