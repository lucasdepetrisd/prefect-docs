from prefect.deployments import run_deployment

def main():
    run_deployment(name="my_flow/my_deploy")